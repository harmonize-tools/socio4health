"""Harmonized records and shares of the data paper surveys.

Usage::

    python data_paper.py                                  # records of every survey and year, then shares
    python data_paper.py records GEIH ENAHO               # records of some surveys
    python data_paper.py records PNADC --years 2023 2024 --force --workers 4
    python data_paper.py shares                           # shares, codebook and DuckDB database only

The surveys (files, keys, weights and rules) are defined in ``surveys.json`` and the harmonized variables in
``mapping/harmonized_mapping.json``; the column and value mappings of each survey are in ``mapping/``. The root
folder is ``--output``, or ``S4H_SCRIPTS_OUTPUT``, or
``<DATA_ROOT>/OUTPUT_scripts_full``.

``records`` writes, for each survey and year (``socio4health.s4h_build_records``):

- ``<output>/<SURVEY>/households/<year>/``: one row per household (housing variables, household weight)
- ``<output>/<SURVEY>/persons/<year>/``: one row per person (person variables, person weight)

``WEIGHT`` is the expansion factor: ``SUM(WEIGHT)`` estimates the number of households / persons of the year.
A year is skipped on later runs unless its rules, mappings, dictionaries or the pipeline changed. Each year's
details go to ``<output>/<SURVEY>/logs/<year>.log``; ``run_report.csv`` summarizes the run.

``shares`` reads those records and writes, in ``<output>``:

- ``shares.csv`` / ``shares/`` (Parquet): for each unit and year, the weighted share of each category of every
  harmonized variable (categories, including Missing, add up to 1), POPULATION and HOUSEHOLDS (sums of the
  weights; empty when the survey only published relative weights) and P_AGE_mean. Each variable is summarized
  on the records of its UNIVERSE (education: people aged 25 or over).
- ``codebook.csv`` and ``record_checks.csv``.
- ``socio4health_records.duckdb``: tables households, persons (all surveys and years), shares, codebook and
  record_checks.
"""
import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from socio4health import (Schema, s4h_build_records, s4h_check_records, s4h_export, s4h_load_surveys, s4h_shares,
                          schema)

DATA_ROOT = Path(os.environ.get("S4H_DATA_ROOT", r"D:\EQUIPO\Documents HDD\Harmonize"))
MAPPINGS_DIR = Path(__file__).resolve().parent   # surveys.json; the mappings it lists are in mapping/
DEFAULT_OUTPUT = Path(os.environ.get("S4H_SCRIPTS_OUTPUT", DATA_ROOT / "OUTPUT_scripts_full"))

SCHEMA = Schema.from_json(MAPPINGS_DIR / "mapping" / "harmonized_mapping.json")
SURVEYS = s4h_load_surveys(MAPPINGS_DIR / "surveys.json", DATA_ROOT)
# Codebook descriptions of the record columns, with the details of these surveys
RECORD_COLUMNS = {
    **schema.RECORD_COLUMNS,
    "HOUSEHOLD_ID": ("households, persons", "Household identifier (survey keys joined with '-'); unique per survey and year. "
                                            "Empty for persons of ENHOGAR 2006-2013, whose public files have no identifiers"),
    "ADMIN_DIVISION": ("households, persons", "First-level unit code, the unit of the shares (Colombia and Peru: "
                                              "department; Brazil: state; Dominican Republic: region)"),
    "ADMIN_DIVISION_2": ("households, persons", "Second-level unit code where available (Peru: district UBIGEO; "
                                                "Brazil: state capital; Dominican Republic: province)"),
}
IDS = {"households": "HOUSEHOLD_ID", "persons": "PERSON_ID"}
KEY_COLS = ["COUNTRY", "SURVEY", "YEAR", "ADMIN_LEVEL", "ADMIN_DIVISION", "POPULATION", "HOUSEHOLDS"]


def build_records(output, surveys=(), years=None, force=False, workers=1):
    """Household and person records of each survey year; returns the run report."""
    reports = []
    for name in surveys or list(SURVEYS):
        survey = SURVEYS[name]
        selected = [y for y in survey.years if not years or y in years]
        reports.append(s4h_build_records(survey, SCHEMA, output / name, years=selected, resume=not force, workers=workers))
    report = pd.concat(reports, ignore_index=True)
    print(report.reindex(columns=["survey", "year", "status", "households", "persons", "seconds", "error"]).to_string(index=False))
    return report


def year_shares(root, survey, year, checks):
    """Shares of one survey year; appends the checks of its record tables to ``checks``."""
    tables = {}
    for level, ids in IDS.items():
        folder = root / survey / level / str(year)
        if folder.exists():
            tables[level], summary = s4h_check_records(pd.read_parquet(folder), SCHEMA, ids)
            invalid = summary.pop("codes not in codebook")
            checks.append({"survey": survey, "year": year, "table": level, **summary,
                           "without weight": round(summary["without weight"], 4),
                           "codes not in codebook": ", ".join(f"{k} ({n})" for k, n in invalid.items())})
    group = SURVEYS[survey].group_col
    shares = s4h_shares(tables, SCHEMA, by=["YEAR", group]).rename(columns={group: "ADMIN_DIVISION"})
    return shares.assign(COUNTRY=SURVEYS[survey].country, SURVEY=survey, ADMIN_LEVEL=group)


def build_shares(root):
    """Shares, codebook, record checks and a DuckDB database with every record."""
    surveys = [s for s in SURVEYS if any((root / s / level).exists() for level in IDS)]
    print("surveys found:", surveys)

    shares, checks = [], []
    for survey in surveys:
        years = sorted({int(p.name) for level in IDS for p in (root / survey / level).glob("*") if p.name.isdigit()})
        for year in years:
            shares.append(year_shares(root, survey, year, checks))
            print(f"{survey} {year}: shares for {len(shares[-1])} units")

    shares = pd.concat(shares, ignore_index=True)
    shares = shares[KEY_COLS + sorted(c for c in shares if c not in KEY_COLS)]
    for c in ["COUNTRY", "SURVEY", "ADMIN_LEVEL", "ADMIN_DIVISION"]:
        shares[c] = shares[c].astype("string")
    checks = pd.DataFrame(checks)
    book = SCHEMA.codebook(RECORD_COLUMNS)

    s4h_export({"shares": shares, "codebook": book, "record_checks": checks}, root, format="csv", overwrite=True)
    s4h_export({"shares": shares}, root, format="parquet", overwrite=True)

    # One database with every record: DuckDB reads the yearly Parquet files directly
    db = root / "socio4health_records.duckdb"
    if db.exists():
        db.unlink()
    with duckdb.connect(str(db)) as con:
        for level in IDS:
            files = [str(f).replace("\\", "/") for s in surveys for f in sorted((root / s / level).glob("*/*.parquet"))]
            con.execute(f"CREATE TABLE {level} AS SELECT * FROM read_parquet({files!r}, union_by_name = true)")
        for name, df in {"shares": shares, "codebook": book, "record_checks": checks}.items():
            con.register("_df", df)
            con.execute(f"CREATE TABLE {name} AS SELECT * FROM _df")
            con.unregister("_df")
        sizes = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("households", "persons", "shares", "codebook")}

    (root / "shares_metadata.json").write_text(json.dumps({
        "created": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "universes": {v: SCHEMA.universe(v) for v in SCHEMA.variables if SCHEMA.universe(v)},
        "surveys": surveys, "rows": sizes,
    }, indent=2), encoding="utf-8")
    print("database:", db, sizes)
    print(checks.groupby(["survey", "table"]).agg(records=("records", "sum"), duplicate_ids=("duplicate IDs", "sum")).to_string())
    return shares


def main(argv=None):
    parser = argparse.ArgumentParser(description="Harmonized records and shares of the data paper surveys.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help="root folder; each survey goes to <output>/<SURVEY> (default: %(default)s)")
    commands = parser.add_subparsers(dest="command", help="default: records, then shares")
    records = commands.add_parser("records", help="household and person records of each survey year")
    records.add_argument("surveys", nargs="*", default=[], help="surveys to run (default: all)")
    records.add_argument("--years", type=int, nargs="+", help="only these years (default: all of each survey)")
    records.add_argument("--force", action="store_true", help="recompute years that are up to date")
    records.add_argument("--workers", type=int, default=1,
                         help="years processed at the same time; each one holds a year in memory (default: 1)")
    commands.add_parser("shares", help="shares, codebook, checks and DuckDB database from the records")
    args = parser.parse_args(argv)

    if args.command in (None, "records"):
        surveys = getattr(args, "surveys", [])
        unknown = [s for s in surveys if s not in SURVEYS]
        if unknown:
            parser.error(f"unknown survey(s) {unknown}; choose from {list(SURVEYS)}")
        build_records(args.output, surveys, getattr(args, "years", None), getattr(args, "force", False),
                      getattr(args, "workers", 1))
    if args.command in (None, "shares"):
        build_shares(args.output)


if __name__ == "__main__":
    main()
