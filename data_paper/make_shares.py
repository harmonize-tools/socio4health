"""Proportions per subnational unit and year from the harmonized records (run after s4h_records.py).

Reads <ROOT>/<SURVEY>/households/<year>/ and <ROOT>/<SURVEY>/persons/<year>/ written by
s4h_records.py and produces, in <ROOT>:

- shares.csv / shares/ (Parquet): for each unit and year, the weighted share of each category of every
  harmonized variable (categories, including Missing, add up to 1), POPULATION and HOUSEHOLDS (sums of
  the weights; empty when the survey only published relative weights) and P_AGE_mean. Educational attainment counts people aged EDUCATION_MIN_AGE or over.
- codebook.csv: the label of every code.
- socio4health_records.duckdb: tables households, persons (all surveys and years), shares and codebook.

ROOT is S4H_SCRIPTS_OUTPUT, or the first command-line argument.
"""
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from socio4health import s4h_export
from s4h_records import HARMONIZED, HOUSEHOLD_VARS, NUMERIC_VARS, PERSON_VARS, SURVEYS

EDUCATION_MIN_AGE = 25


def label(variable, code):
    text = HARMONIZED[variable]["VALUES"].get(str(code), f"code {code}")
    return re.sub(r"\W+", "_", str(text)).strip("_")


def unit_shares(households, persons, survey):
    """Weighted shares per unit and year for one survey (any number of years)."""
    group = SURVEYS[survey]["group_col"]
    keys = ["YEAR", group]
    h = households.dropna(subset=[group, "WEIGHT"]) if households is not None else pd.DataFrame(columns=keys + ["WEIGHT"])
    p = persons.dropna(subset=[group, "WEIGHT"]) if persons is not None else pd.DataFrame(columns=keys + ["WEIGHT"])
    parts = [p.groupby(keys)["WEIGHT"].sum().rename("POPULATION"), h.groupby(keys)["WEIGHT"].sum().rename("HOUSEHOLDS")]
    for df, variables in ((h, HOUSEHOLD_VARS), (p, PERSON_VARS)):
        for v in variables:
            if v not in df.columns or df[v].isna().all():
                continue
            d = df.dropna(subset=[v])
            if v in NUMERIC_VARS:
                parts.append((d[v] * d["WEIGHT"]).groupby([d[k] for k in keys]).sum()
                             .div(d.groupby(keys)["WEIGHT"].sum()).rename(f"{v}_mean"))
                continue
            if v == "P_EDUCATION":
                d = d[d["P_AGE"] >= EDUCATION_MIN_AGE]
            t = d.groupby(keys + [v])["WEIGHT"].sum().unstack(v)
            t = t.div(t.sum(axis=1), axis=0)
            t.columns = [f"{v}_{label(v, c)}" for c in t.columns]
            parts.append(t)
    out = pd.concat(parts, axis=1).reset_index().rename(columns={group: "ADMIN_DIVISION"})
    # Relative (normalized) weights do not add up to population totals: leave those totals empty
    relative = SURVEYS[survey].get("relative_weight_years", {})
    for column, level in (("POPULATION", "persons"), ("HOUSEHOLDS", "households")):
        out.loc[out["YEAR"].astype(int).isin(relative.get(level, [])), column] = pd.NA
    out.insert(0, "ADMIN_LEVEL", group)
    out.insert(0, "SURVEY", survey)
    out.insert(0, "COUNTRY", SURVEYS[survey]["country"])
    return out


def codebook():
    rows = [{"variable": v, "table": "households" if v in HOUSEHOLD_VARS else "persons", "code": None,
             "label": "number (years)", "description": HARMONIZED[v]["DESCRIPTION"].strip()} for v in NUMERIC_VARS]
    rows += [{"variable": v, "table": "households" if v in HOUSEHOLD_VARS else "persons", "code": int(k), "label": lab,
              "description": HARMONIZED[v]["DESCRIPTION"].strip()}
             for v in HARMONIZED if v not in NUMERIC_VARS
             for k, lab in sorted(HARMONIZED[v]["VALUES"].items(), key=lambda kv: int(kv[0]))]
    rows += [{"variable": c, "table": "households, persons", "code": None, "label": None, "description": d} for c, d in {
        "HOUSEHOLD_ID": "Household identifier (survey keys joined with '-'); unique per survey and year. "
                        "Empty for persons of ENHOGAR 2006-2013, whose public files have no identifiers",
        "PERSON_ID": "Person identifier (household ID plus the person's line number)",
        "WEIGHT": "Expansion factor: households (households table) or persons (persons table) the record represents in its year",
        "WEIGHT_TYPE": "'expansion': WEIGHT adds up to the population; 'relative': the survey only published normalized "
                       "weights (average 1), valid for shares but not for totals",
        "ADMIN_DIVISION": "First-level unit code (department, region, state; district UBIGEO in Peru)",
        "ADMIN_DIVISION_2": "Second-level unit code where available (Brazil: state capital; Dominican Republic: province)",
        "ADMIN_LEVEL": "Unit column used in `shares` for this survey",
    }.items()]
    out = pd.DataFrame(rows)
    out["code"] = out["code"].astype("Int16")
    return out


def main(root):
    root = Path(root)
    surveys = [s for s in SURVEYS if (root / s / "households").exists() or (root / s / "persons").exists()]
    print("surveys found:", surveys)

    shares, checks = [], []
    valid = {v: {int(k) for k in HARMONIZED[v]["VALUES"]} for v in HARMONIZED if v not in NUMERIC_VARS}
    for survey in surveys:
        years = sorted({int(p.name) for lvl in ("households", "persons") for p in (root / survey / lvl).glob("*") if p.name.isdigit()})
        for year in years:
            tables = {}
            for level in ("households", "persons"):
                folder = root / survey / level / str(year)
                tables[level] = pd.read_parquet(folder) if folder.exists() else None
            for level, df in tables.items():
                if df is None:
                    continue
                id_col = "HOUSEHOLD_ID" if level == "households" else "PERSON_ID"
                bad = {}
                for v in valid:
                    if v in df.columns:
                        invalid = df[v].notna() & ~df[v].isin(valid[v])
                        if invalid.any():
                            bad[v] = int(invalid.sum())
                            df.loc[invalid, v] = 0   # codes outside the codebook count as Missing
                checks.append({"survey": survey, "year": year, "table": level, "records": len(df),
                               "duplicate IDs": int(df[id_col].dropna().duplicated().sum()),
                               "without weight": round(float(df["WEIGHT"].isna().mean()), 4),
                               "codes not in codebook": ", ".join(f"{k} ({n})" for k, n in bad.items())})
            shares.append(unit_shares(tables["households"], tables["persons"], survey))
            print(f"{survey} {year}: shares for {len(shares[-1])} units")

    shares = pd.concat(shares, ignore_index=True)
    key_cols = ["COUNTRY", "SURVEY", "YEAR", "ADMIN_LEVEL", "ADMIN_DIVISION", "POPULATION", "HOUSEHOLDS"]
    shares = shares[key_cols + sorted(c for c in shares if c not in key_cols)]
    for c in ["COUNTRY", "SURVEY", "ADMIN_LEVEL", "ADMIN_DIVISION"]:
        shares[c] = shares[c].astype("string")
    checks = pd.DataFrame(checks)
    book = codebook()

    s4h_export({"shares": shares, "codebook": book, "record_checks": checks}, root, format="csv", overwrite=True)
    s4h_export({"shares": shares}, root, format="parquet", overwrite=True)

    # One database with every record: DuckDB reads the yearly Parquet files directly
    db = root / "socio4health_records.duckdb"
    if db.exists():
        db.unlink()
    with duckdb.connect(str(db)) as con:
        for level in ("households", "persons"):
            files = [str(f).replace("\\", "/") for s in surveys for f in sorted((root / s / level).glob("*/*.parquet"))]
            con.execute(f"CREATE TABLE {level} AS SELECT * FROM read_parquet({files!r}, union_by_name = true)")
        for name, df in {"shares": shares, "codebook": book, "record_checks": checks}.items():
            con.register("_df", df)
            con.execute(f"CREATE TABLE {name} AS SELECT * FROM _df")
            con.unregister("_df")
        sizes = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("households", "persons", "shares", "codebook")}

    (root / "shares_metadata.json").write_text(json.dumps({
        "created": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "education_min_age": EDUCATION_MIN_AGE, "surveys": surveys, "rows": sizes,
    }, indent=2), encoding="utf-8")
    print("database:", db, sizes)
    print(checks.groupby(["survey", "table"]).agg(records=("records", "sum"), duplicate_ids=("duplicate IDs", "sum")).to_string())


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else os.environ.get("S4H_SCRIPTS_OUTPUT", r"D:\EQUIPO\Documents HDD\Harmonize\OUTPUT_scripts_full"))
