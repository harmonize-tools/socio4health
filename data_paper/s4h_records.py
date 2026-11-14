"""Record-level harmonization of the data paper surveys.

Usage::

    python s4h_records.py                       # every survey and year
    python s4h_records.py GEIH ENAHO            # some surveys
    python s4h_records.py PNADC --years 2023 2024 --force
    python make_shares.py                       # then: proportions per unit and year + DuckDB database

The root folder is ``--output``, or ``S4H_SCRIPTS_OUTPUT``, or ``<DATA_ROOT>/OUTPUT_scripts_full``.
For each survey and year it writes two tables of harmonized records:

- ``<output>/households/<year>/``: one row per household (housing variables, household weight)
- ``<output>/persons/<year>/``: one row per person (person variables, person weight)

Every harmonized variable holds its harmonized code (``harmonized_mapping.json``); age is a number.
``WEIGHT`` is the expansion factor: ``SUM(WEIGHT)`` estimates the number of households / persons of the
year. Proportions are computed separately by ``make_shares.py``.

Each year is written as soon as it is processed and skipped on the next run unless the survey rules,
mappings, dictionaries or this code changed (see ``fingerprint``). Each year's details go to
``<output>/logs/<year>.log``; ``<output>/run_report.csv`` summarizes the run.
"""
import contextlib
import hashlib
import json
import os
import re
import shutil
import time
import traceback
from pathlib import Path

import pandas as pd

from socio4health import Extractor, Harmonizer, s4h_export, s4h_set_log_level
from socio4health.utils import extractor_utils, harmonizer_utils
from socio4health.utils.harmonizer_utils import (
    _clean_column_name, _normalize_columns, apply_value_mappings, harmonize_columns_by_year,
    mapping_source_columns, merge_factor, s4h_code_from_flags, use_person_factor,
)

DATA_ROOT = Path(os.environ.get("S4H_DATA_ROOT", r"D:\EQUIPO\Documents HDD\Harmonize"))
MAPPINGS_DIR = Path(__file__).resolve().parent
CACHE_PARQUET = True

HARMONIZED = json.loads((MAPPINGS_DIR / "harmonized_mapping.json").read_text(encoding="utf-8"))
HOUSEHOLD_VARS = ["AREA"] + [v for v in HARMONIZED if v.startswith("H_")]
PERSON_VARS = [v for v in HARMONIZED if v.startswith("P_")]
NUMERIC_VARS = {"P_AGE"}
ADMIN_COLS = ["ADMIN_DIVISION", "ADMIN_DIVISION_2"]
RENAMES = {"P_ETHNICITY": "P_ETHNIC"}

GEIH_AREA_FILES = [   # 2007-2019 files are split by area; the area is only in the file name
    {"pattern": r"(?i)[._]csv_\s*Cabecera\s*-", "assign": {"CLASE": "1"}},   # municipal seats: urban
    {"pattern": r"(?i)[._]csv_\s*Resto\s*-", "assign": {"CLASE": "2"}},      # rest: rural
    {"pattern": r"(?i)^(?!.*[._]csv_\s*(Cabecera|Resto|.rea)\s*-)"},         # not split ("Área" = Cabecera duplicates: skipped)
]

SURVEYS = {
    "GEIH": {
        "country": "COL", "years": list(range(2007, 2026)),
        "parts": [{"path": "GEIH/{year}"}],
        "reader": {"down_ext": [".csv"], "sep": ";", "on_bad_lines": "skip"},
        "file_groups": GEIH_AREA_FILES,
        "column_mapping": "col_mapping/column_mapping.json",
        "value_mapping": "col_mapping/geih_mapping.json",
        # FEX_C18 (CNPV 2018 projection); in 2020 a separate projection file misses 8 new departments: FEX_C there
        "factor": {"factor_col": "FEX_C18", "id_col": "DIRECTORIO", "fallback_col": "FEX_C"},
        "household_key": [["DIRECTORIO", "SECUENCIA_P", "MES"], ["DIRECTORIO", "SECUENCIA_P"]],
        "person_key": [["ORDEN"]],
        "periods_per_year": 12,   # each monthly sample is expanded to the whole population
        "group_col": "ADMIN_DIVISION",
    },
    "ENAHO": {
        "country": "PER", "years": list(range(2004, 2025)),
        "parts": [{"path": "ENAHO/{year}"}],
        "reader": {"down_ext": [".sav"]},
        "column_mapping": "per_mapping/column_mapping.json",
        "value_mapping": "per_mapping/enaho_mapping.json",
        "factor": {"factor_col": "FACTOR07", "id_col": ["CONGLOME", "VIVIENDA", "HOGAR"]},
        "person_factor": {"factor_col": "FACPOB07", "id_col": ["CONGLOME", "VIVIENDA", "HOGAR", "CODPERSO"]},
        # Only completed or partly completed interviews (RESULT 1-2), as INEI does: refusals, absences and
        # empty dwellings have no data
        "keep_rows": {"RESULT": [1, 2]},
        # Usual residents only, as INEI does: members not away 30+ days (P204 = 1, P205 = 2) and non-members
        # present 30+ days (P204 = 2, P206 = 1). Every person module carries P204-P206
        "keep_rows_any": [{"P204": [1], "P205": [2]}, {"P204": [2], "P206": [1]}],
        # Literacy (P302) is only asked below completed primary: in the table that asks it, the people who were
        # not asked and completed primary or more (P301A 4-11) take that level, which the value mapping counts
        # as literate. Other blanks (e.g. 2020 phone interviews) stay Missing
        "fill_missing": {"P302": {"from": "P301A", "values": [4, 5, 6, 7, 8, 9, 10, 11]}},
        "household_key": [["CONGLOME", "VIVIENDA", "HOGAR"]],
        "person_key": [["CODPERSO"]],
        "group_col": "ADMIN_DIVISION",
    },
    "PNAD": {
        "country": "BRA", "years": [2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2011],
        "parts": [   # households (dom) and persons (pes) are separate fixed-width files
            {"path": "PNAD/dom/{year}", "ibge_dictionary": "PNAD/original/{year}/Dicion*/*domic*.xls"},
            {"path": "PNAD/pes/{year}", "ibge_dictionary": "PNAD/original/{year}/Dicion*/*pessoas*.xls"},
        ],
        "reader": {"down_ext": [".txt"]},
        "column_mapping": "pnad_mapping/column_mapping.json",
        "value_mapping": "pnad_mapping/pnad_mapping.json",
        "household_key": [["V0102", "V0103"]],
        "person_key": [["V0301"]],
        "vertical_merge": False,
        "group_col": "ADMIN_DIVISION",
    },
    "PNADC": {
        "country": "BRA", "years": list(range(2012, 2026)),
        "parts": [{"path": "PNADC/{year}", "ibge_dictionary": "PNADC/{year}/dicionario*.xls",
                   "shared_dictionary": "PNADC/dicionario*_a_*.xls"}],
        "reader": {"down_ext": [".txt"]},
        "column_mapping": "pnadc_mapping/column_mapping.json",
        "value_mapping": "pnadc_mapping/pnadc_mapping.json",
        "household_key": [["UPA", "V1008", "V1014"]],
        "person_key": [["V2003"]],
        # Cooking fuel: S01016B ("most used") is only asked when 2+ fuels are used; otherwise the fuel is the
        # only "yes" among S01016A1-A5. S01016F holds it, and the mapping uses "S01016B|S01016F"
        "derive_from_flags": {"S01016F": {"S01016A1": 1, "S01016A2": 2, "S01016A3": 3, "S01016A4": 4, "S01016A5": 5}},
        "group_col": "ADMIN_DIVISION_2",
    },
    "ENHOGAR": {
        "country": "DOM",
        "years": [2006, 2007, 2008, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2024],
        "parts": [{"path": "ENHOGAR/{year}"}],
        "reader": {"down_ext": [".csv"], "sep": ","},
        "column_mapping": "rd_mappings/column_mapping.json",
        "value_mapping": "rd_mappings/enhogar{year}_mapping.json",
        "vertical_merge": False,
        "household_key": [["HH1", "HH2"], ["UPM", "HVIVIEN", "HHOGAR"], ["HHUPM", "HHVIV", "HHOGAR"]],
        "person_key": [["HL1"], ["LN"], ["HLINEA"]],
        # the 2006-2013 public files have no identifiers: rows get their own ID and persons are not linked
        "row_ids_without_key": True,
        "region_from_province": {"years": [2006], "mapping": "rd_mappings/region_mapping.json"},
        # Years whose files only have relative (normalized) weights, which average 1: shares are right but
        # their sums are not population totals, so POPULATION / HOUSEHOLDS are left empty in the shares
        "relative_weight_years": {"households": [2006, 2008, 2010, 2011, 2012, 2014, 2019],
                                  "persons": [2006, 2008, 2010, 2014, 2019]},
        "group_col": "ADMIN_DIVISION",
    },
}


class NoData(Exception):
    pass


def read_json(path):
    return json.loads((MAPPINGS_DIR / path).read_text(encoding="utf-8"))


def int_keys(obj):
    if isinstance(obj, dict):
        return {(int(k) if isinstance(k, str) and k.isdigit() else k): int_keys(v) for k, v in obj.items()}
    return obj


def column_mapping_for(spec, year):
    # Blocks keyed by year ("2024") or by period ("2007-2025"): use the one that contains the year
    for key, mapping in read_json(spec["column_mapping"]).items():
        start, _, end = key.partition("-")
        if int(start) <= year <= int(end or start):
            return mapping
    raise NoData(f"No column mapping for {year}")


def value_mapping_for(spec, year):
    data = read_json(spec["value_mapping"].format(year=year))
    return int_keys(data.get("mapping", data))


def extra_columns(spec):
    # Raw columns needed besides the mapped variables: keys and weights
    cols = {c for alt in spec["household_key"] + spec["person_key"] for c in alt}
    for f in (spec.get("factor"), spec.get("person_factor")):
        if f:
            cols |= {f["factor_col"], *([f["id_col"]] if isinstance(f["id_col"], str) else f["id_col"])}
            cols |= {f["fallback_col"]} if f.get("fallback_col") else set()
    cols |= {c for rule in spec.get("keep_rows_any", []) for c in rule}
    cols |= set(spec.get("keep_rows", {})) | {c for t, rule in spec.get("fill_missing", {}).items() for c in (t, rule["from"])}
    cols |= {c for flags in spec.get("derive_from_flags", {}).values() for c in flags}
    return sorted(cols)


def ibge_layout(part, year, variables):
    # Layout of this year's fixed-width file, from the IBGE dictionary published with it
    found = sorted(DATA_ROOT.glob(part["ibge_dictionary"].format(year=year)))
    if not found and part.get("shared_dictionary"):   # e.g. ".._2012_a_2014_.." used by several years
        found = [f for f in sorted(DATA_ROOT.glob(part["shared_dictionary"]))
                 if any(int(a) <= year <= int(b) for a, b in re.findall(r"(\d{4})_a_(\d{4})", f.name))]
    if not found:
        raise NoData(f"No IBGE dictionary {part['ibge_dictionary'].format(year=year)}")
    return extractor_utils.s4h_parse_ibge_layout(found[0], variables=variables)


def unify_directorio(df):
    # Some GEIH files spell the household ID differently (BOM, lowercase)
    alias = [c for c in df.columns if _clean_column_name(c) == "DIRECTORIO" and c != "DIRECTORIO"]
    if alias:
        df = df.drop(columns=[c for c in df.columns if c == "DIRECTORIO"]).rename(columns={alias[0]: "DIRECTORIO"})
    return df


def extract(spec, year, variables, scratch_dir):
    dfs = []
    for part in spec["parts"]:
        folder = DATA_ROOT / part["path"].format(year=year)
        if not folder.is_dir():
            raise NoData(f"Folder not found: {folder}")
        reader = dict(spec["reader"])
        if "ibge_dictionary" in part:
            colnames, colspecs = ibge_layout(part, year, variables)
            reader.update(is_fwf=True, colnames=colnames, colspecs=colspecs)
        for group in spec.get("file_groups", [{}]):
            extractor = Extractor(input_path=str(folder), output_path=str(scratch_dir),
                                  cache_parquet=CACHE_PARQUET, file_pattern=group.get("pattern"), **reader)
            for df in extractor.s4h_extract():
                for col, value in group.get("assign", {}).items():
                    df = df.drop(columns=[c for c in df.columns if _clean_column_name(c) == col]).assign(**{col: value})
                dfs.append(unify_directorio(df))
    return dfs


def prune(dfs, needed):
    # Keep only the columns the pipeline uses, before anything is computed
    needed = {_clean_column_name(c) for c in needed} | {"YEAR"}
    out = []
    for df in dfs:
        cols = [c for c in df.columns if _clean_column_name(c) in needed]
        if any(_clean_column_name(c) != "YEAR" for c in cols):
            out.append(df[cols])
    return out


def clean_code(series):
    # "01", "1.0" and 1 are the same code: compare them as text without leading zeros or decimals
    s = series.astype("string").str.strip().replace("", pd.NA).str.replace(r"\.0+$", "", regex=True)
    return s.str.replace(r"^0+(?=\d)", "", regex=True)


def add_ids(dfs, spec):
    # HOUSEHOLD_ID from the household key and the raw person key (_PKEY), before columns are renamed.
    # Tables without any key get row IDs when the survey allows it (_ROWID: persons are not linked).
    out = []
    for i, df in enumerate(dfs):
        df = _normalize_columns(df.compute() if hasattr(df, "compute") else df).copy()
        hkey = next((k for k in spec["household_key"] if all(c in df.columns for c in k)), None)
        if hkey:
            df["HOUSEHOLD_ID"] = clean_code(df[hkey[0]]).str.cat([clean_code(df[c]) for c in hkey[1:]], sep="-")
            pkey = next((k for k in spec["person_key"] if all(c in df.columns for c in k)), None)
            if pkey:
                df["_PKEY"] = clean_code(df[pkey[0]]).str.cat([clean_code(df[c]) for c in pkey[1:]], sep="-")
        elif spec.get("row_ids_without_key"):
            df["HOUSEHOLD_ID"] = f"t{i}-r" + pd.Series(range(1, len(df) + 1), index=df.index).astype(str)
            df["_ROWID"] = True
        out.append(df)
    return out


def region_from_province(dfs, spec, year):
    # ENHOGAR 2006 has provinces (ADMIN_DIVISION_2) but not regions: derive ADMIN_DIVISION
    rule = spec.get("region_from_province")
    if not rule or year not in rule["years"]:
        return dfs
    regions = {str(k): v for k, v in read_json(rule["mapping"]).items()}
    for df in dfs:
        if "ADMIN_DIVISION_2" in df.columns and "ADMIN_DIVISION" not in df.columns:
            df["ADMIN_DIVISION"] = clean_code(df["ADMIN_DIVISION_2"]).map(regions)
    return dfs


def row_rules(dfs, spec):
    # Survey rules on raw records: keep only some rows (keep_rows), build a code from yes/no flags
    out = []
    for df in dfs:
        for col, values in spec.get("keep_rows", {}).items():
            match = next((c for c in df.columns if _clean_column_name(c) == col), None)
            if match is not None:
                df = df[clean_code(df[match]).isin([str(v) for v in values]).fillna(False)]
        any_rules = spec.get("keep_rows_any", [])
        names = {_clean_column_name(c): c for c in df.columns}
        if any_rules and all(col in names for rule in any_rules for col in rule):   # tables with every column
            keep = pd.Series(False, index=df.index)
            for rule in any_rules:   # a row is kept when it meets every condition of at least one rule
                match = pd.Series(True, index=df.index)
                for col, values in rule.items():
                    match &= clean_code(df[names[col]]).isin([str(v) for v in values]).fillna(False)
                keep |= match
            df = df[keep]
        for target, rule in spec.get("fill_missing", {}).items():
            t = next((c for c in df.columns if _clean_column_name(c) == target), None)
            s = next((c for c in df.columns if _clean_column_name(c) == rule["from"]), None)
            if t is not None and s is not None:   # only in tables that ask the question
                source = clean_code(df[s])
                source = source.where(source.isin([str(v) for v in rule["values"]]))   # only these source codes
                df = df.assign(**{t: clean_code(df[t]).fillna(source)})
        for target, flags in spec.get("derive_from_flags", {}).items():
            df = s4h_code_from_flags(df, flags, target)
        out.append(df)
    return out


def assemble(tables, variables, level, spec):
    # One row per household or person. Every table holding these variables is stacked (a module can be
    # split over several tables, e.g. months with slightly different columns) and each ID keeps, for each
    # column, the first non-empty value, larger tables first. Households come from tables without person
    # variables when there are any (a members file may repeat the housing variables on every person).
    id_col = "HOUSEHOLD_ID" if level == "households" else "PERSON_ID"
    real = [v for v in variables if v != "AREA"]   # AREA alone does not make a household table
    candidates = []
    for df in tables:
        if "HOUSEHOLD_ID" not in df.columns or "EXP_FACTOR" not in df.columns or not any(v in df.columns for v in real):
            continue
        df = df.dropna(subset=["HOUSEHOLD_ID"])
        if level == "persons":
            order = df["HOUSEHOLD_ID"] + "-#" + (df.groupby("HOUSEHOLD_ID").cumcount() + 1).astype(str)
            # line number when there is one; otherwise (or when it is empty) the order within the household
            df = df.assign(PERSON_ID=(df["HOUSEHOLD_ID"] + "-" + df["_PKEY"]).fillna(order) if "_PKEY" in df.columns else order)
            if "_ROWID" in df.columns:   # no key: the row ID is not a household
                df = df.assign(HOUSEHOLD_ID=pd.NA)
        candidates.append(df)
    if level == "households":
        pure = [df for df in candidates if not any(v in df.columns for v in PERSON_VARS)]
        candidates = pure or candidates
    if not candidates:
        return None
    candidates.sort(key=len, reverse=True)
    cols = ["YEAR", "HOUSEHOLD_ID", "PERSON_ID", *ADMIN_COLS, "EXP_FACTOR", *variables]
    stacked = pd.concat([df[[c for c in cols if c in df.columns]] for df in candidates], ignore_index=True)
    return stacked.groupby(id_col, sort=False).first().reset_index()


def weight_type(spec, level, year):
    # "relative" when the survey only published normalized weights for this table and year
    return "relative" if year in spec.get("relative_weight_years", {}).get(level, []) else "expansion"


def finish(records, spec, variables, level, year):
    # Annual weight, its type, codes as numbers, harmonized column order
    df = records.copy()
    weight = pd.to_numeric(df.pop("EXP_FACTOR").astype("string").str.replace(",", ".", regex=False), errors="coerce")
    df["WEIGHT"] = weight / spec.get("periods_per_year", 1)
    for v in variables:
        if v in df.columns:
            df[v] = pd.to_numeric(clean_code(df[v]), errors="coerce").astype("Int16")
    for c in ADMIN_COLS:
        if c in df.columns:
            # integer part of the unit code: ENHOGAR 2024 households use "10.2" (region 10, sub-region 2)
            # while its persons use "10"; the shares are by region
            df[c] = clean_code(df[c]).str.replace(r"^(\d+)\.\d+$", r"\1", regex=True)
    df["YEAR"] = pd.to_numeric(df["YEAR"]).astype("Int16")
    df["WEIGHT_TYPE"] = weight_type(spec, level, year)
    first = [c for c in ["YEAR", *ADMIN_COLS, "HOUSEHOLD_ID", "PERSON_ID", "WEIGHT", "WEIGHT_TYPE"] if c in df.columns]
    return df[first + [v for v in variables if v in df.columns]]


def process_year(survey, year, scratch_dir):
    spec = SURVEYS[survey]
    column_map = column_mapping_for(spec, year)
    raw_columns = [*mapping_source_columns(column_map), *extra_columns(spec)]
    dfs = extract(spec, year, raw_columns, scratch_dir)
    if not dfs:
        raise NoData(f"No {spec['reader']['down_ext']} files for {year}")
    for df in dfs:
        df["YEAR"] = year

    if spec.get("vertical_merge", True):
        dfs = Harmonizer().s4h_vertical_merge(dfs, overlap_threshold=0.8, method="union", overlap_basis="larger")
    dfs = prune(dfs, raw_columns)
    factor, person = spec.get("factor"), spec.get("person_factor")
    if person:
        dfs = use_person_factor(dfs, factor["factor_col"], person["factor_col"], person["id_col"])
    if factor:
        dfs = merge_factor(dfs, factor["factor_col"], factor["id_col"], fallback_col=factor.get("fallback_col"))
    dfs = row_rules(add_ids(dfs, spec), spec)

    dfs = harmonize_columns_by_year(dfs, year, column_map)
    dfs = apply_value_mappings(dfs, year, value_mapping_for(spec, year), column_aliases=column_map)
    dfs = [d.rename(columns={k: v for k, v in RENAMES.items() if k in d.columns}) for d in dfs]
    dfs = region_from_province(dfs, spec, year)

    households = assemble(dfs, HOUSEHOLD_VARS, "households", spec)
    persons = assemble(dfs, ["AREA", *PERSON_VARS], "persons", spec)
    if households is None and persons is None:
        raise NoData("No table has a household key, a weight and harmonized variables")
    # Persons keep the AREA of their own file; their household's AREA fills the gaps (e.g. PNADC 2020-2021 has
    # no household table, ENHOGAR 2006-2013 persons cannot be linked to households)
    if persons is not None and households is not None and "AREA" in households.columns:
        household_area = (households[["HOUSEHOLD_ID", "AREA"]].dropna().drop_duplicates("HOUSEHOLD_ID")
                          .rename(columns={"AREA": "_HOUSEHOLD_AREA"}))
        persons = persons.merge(household_area, on="HOUSEHOLD_ID", how="left")
        filler = persons.pop("_HOUSEHOLD_AREA")
        persons["AREA"] = persons["AREA"].fillna(filler) if "AREA" in persons.columns else filler
    return {"households": finish(households, spec, HOUSEHOLD_VARS, "households", year) if households is not None else None,
            "persons": finish(persons, spec, ["AREA", *PERSON_VARS], "persons", year) if persons is not None else None}


def fingerprint(survey):
    # Changes when the survey rules, mappings, IBGE dictionaries or the pipeline code change
    spec = SURVEYS[survey]
    h = hashlib.sha256(json.dumps(spec, sort_keys=True, default=str).encode())
    h.update(Path(harmonizer_utils.__file__).read_bytes())
    h.update(Path(harmonizer_utils.__file__).parent.parent.joinpath("harmonizer.py").read_bytes())
    for fn in (extract, prune, clean_code, add_ids, row_rules, region_from_province, assemble, weight_type, finish, process_year):
        h.update(fn.__code__.co_code)   # pipeline code only: docs or command-line changes keep the results
    files = [MAPPINGS_DIR / "harmonized_mapping.json", MAPPINGS_DIR / spec["column_mapping"]]
    files += sorted(MAPPINGS_DIR.glob(spec["value_mapping"].replace("{year}", "*")))
    for part in spec["parts"]:
        for pattern in (part.get("ibge_dictionary"), part.get("shared_dictionary")):
            if pattern:
                files += sorted(DATA_ROOT.glob(pattern.replace("{year}", "*")))
    for f in files:
        h.update(f.read_bytes())
    return h.hexdigest()[:12]


def run_records(survey, output_dir, years=None, resume=True):
    """Harmonize every year of ``survey`` into ``output_dir/households`` and ``output_dir/persons``."""
    spec = SURVEYS[survey]
    output_dir = Path(output_dir)
    for sub in ("logs", "_extractor"):
        (output_dir / sub).mkdir(parents=True, exist_ok=True)
    s4h_set_log_level("WARNING")
    print_ = lambda *a: print(*a, flush=True)

    # Each year is stored with the fingerprint of the code and mappings that produced it
    stamp_file = output_dir / "fingerprints.json"
    stamps = json.loads(stamp_file.read_text()) if stamp_file.exists() else {}
    current = fingerprint(survey)
    report = []
    for year in (years or spec["years"]):
        row, start = {"survey": survey, "year": year}, time.time()
        if resume and stamps.get(str(year)) == current:
            row["status"] = "up to date"
            report.append(row)
            print_(f"{survey} {year}: up to date")
            continue
        log_file = output_dir / "logs" / f"{year}.log"
        try:
            with open(log_file, "w", encoding="utf-8") as log, contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
                try:
                    tables = process_year(survey, year, output_dir / "_extractor")
                except Exception:
                    traceback.print_exc()
                    raise
            for level, df in tables.items():
                target = output_dir / level / str(year)
                if target.exists():
                    shutil.rmtree(target)
                if df is not None:
                    df.insert(0, "ADMIN_LEVEL", spec["group_col"])
                    df.insert(0, "SURVEY", survey)
                    df.insert(0, "COUNTRY", spec["country"])
                    s4h_export(df, output_dir / level, names=[str(year)], overwrite=True)
                    row[level] = len(df)
            stamps[str(year)] = current
            stamp_file.write_text(json.dumps(stamps, indent=2))
            row["status"] = "ok"
        except NoData as exc:
            row["status"], row["error"] = "no data", str(exc)
        except Exception as exc:
            row["status"], row["error"] = "failed", f"{type(exc).__name__}: {exc}"
        row["seconds"] = round(time.time() - start, 1)
        report.append(row)
        print_(f"{survey} {year}: {row['status']} ({row.get('households', 0)} households, "
               f"{row.get('persons', 0)} persons, {row['seconds']} s) {row.get('error', '')}")

    report = pd.DataFrame(report)
    report.to_csv(output_dir / "run_report.csv", index=False)
    return report


DEFAULT_OUTPUT = Path(os.environ.get("S4H_SCRIPTS_OUTPUT", DATA_ROOT / "OUTPUT_scripts_full"))


def main(argv=None):
    import argparse

    parser = argparse.ArgumentParser(
        description="Harmonized record-level households and persons for the data paper surveys.")
    parser.add_argument("surveys", nargs="*", default=[],
                        help="surveys to run (default: all)")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help="root folder; each survey goes to <output>/<SURVEY> (default: %(default)s)")
    parser.add_argument("--years", type=int, nargs="+", help="only these years (default: all of each survey)")
    parser.add_argument("--force", action="store_true", help="recompute years that are up to date")
    args = parser.parse_args(argv)
    unknown = [s for s in args.surveys if s not in SURVEYS]
    if unknown:
        parser.error(f"unknown survey(s) {unknown}; choose from {list(SURVEYS)}")

    reports = []
    for survey in args.surveys or list(SURVEYS):
        years = [y for y in SURVEYS[survey]["years"] if not args.years or y in args.years]
        reports.append(run_records(survey, args.output / survey, years=years, resume=not args.force))
    report = pd.concat(reports, ignore_index=True)
    print(report.reindex(columns=["survey", "year", "status", "households", "persons", "seconds", "error"]).to_string(index=False))
    return report


if __name__ == "__main__":
    main()
