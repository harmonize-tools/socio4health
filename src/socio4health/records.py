"""
Record-level harmonization: one table of households and one of persons per survey year.

:func:`s4h_harmonize_records` turns the raw files of a :class:`~socio4health.survey.Survey` year into
``households`` (one row per household) and ``persons`` (one row per person) with the harmonized variables of a
:class:`~socio4health.Schema`, IDs and an annual weight. :func:`s4h_build_records` does it for every year of a
survey, writing each year as soon as it is done and skipping, on later runs, the years whose inputs did not
change.
"""

import contextlib
import hashlib
import json
import shutil
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Union

import pandas as pd

from socio4health.extractor import Extractor
from socio4health.harmonizer import Harmonizer
from socio4health.loader import s4h_export
from socio4health.schema import Schema
from socio4health.survey import NoDataError, Survey
from socio4health.utils import extractor_utils
from socio4health.utils.log import logger, s4h_set_log_level as set_log_level
from socio4health.utils.harmonizer_utils import (
    _clean_column_name, _normalize_columns, apply_value_mappings, harmonize_columns_by_year, merge_factor,
    prune_columns, s4h_code_from_flags, use_person_factor,
)

LEVELS = {"households": "HOUSEHOLD_ID", "persons": "PERSON_ID"}
ADMIN_COLS = ["ADMIN_DIVISION", "ADMIN_DIVISION_2"]


def s4h_clean_codes(series: pd.Series) -> pd.Series:
    """Codes as text without spaces, leading zeros or decimals: ``"01"``, ``"1.0"`` and ``1`` become ``"1"``."""
    # Each distinct value is cleaned once: a column has millions of values but far fewer distinct ones
    codes, uniques = pd.factorize(series)
    s = pd.Series(uniques).astype("string").str.strip().replace("", pd.NA).str.replace(r"\.0+$", "", regex=True)
    s = s.str.replace(r"^0+(?=\d)", "", regex=True)
    return pd.Series(s.array.take(codes, allow_fill=True), index=series.index, name=series.name)


def _column(df, name):
    return next((c for c in df.columns if _clean_column_name(c) == name), None)


def _extract(survey: Survey, year: int, columns, scratch_dir, cache_parquet):
    dfs = []
    for part in survey.parts:
        folder = Path(survey.data_root) / part["path"].format(year=year)
        if not folder.is_dir():
            raise NoDataError(f"Folder not found: {folder}")
        reader = dict(survey.reader)
        if "ibge_dictionary" in part:
            colnames, colspecs = extractor_utils.s4h_parse_ibge_layout(survey.ibge_dictionary(part, year), variables=columns)
            reader.update(is_fwf=True, colnames=colnames, colspecs=colspecs)
        for group in survey.file_groups:
            extractor = Extractor(input_path=str(folder), output_path=str(scratch_dir), cache_parquet=cache_parquet,
                                  file_pattern=group.get("pattern"), **reader)
            for df in extractor.s4h_extract():
                for col, value in group.get("assign", {}).items():   # e.g. the area, only in the file name
                    df = df.drop(columns=[c for c in df.columns if _clean_column_name(c) == col]).assign(**{col: value})
                for name in survey.unify_columns:
                    variants = [c for c in df.columns if _clean_column_name(c) == name and c != name]
                    if variants:
                        df = df.drop(columns=[c for c in df.columns if c == name]).rename(columns={variants[0]: name})
                dfs.append(df)
    return dfs


def _add_ids(dfs, survey: Survey):
    # HOUSEHOLD_ID from the household key and the raw person key (_PKEY), before columns are renamed. Tables
    # without any key get row IDs when the survey allows it (_ROWID: persons are not linked to households).
    out = []
    for i, df in enumerate(dfs):
        df = _normalize_columns(df.compute() if hasattr(df, "compute") else df).copy()
        hkey = next((k for k in survey.household_key if all(c in df.columns for c in k)), None)
        if hkey:
            df["HOUSEHOLD_ID"] = s4h_clean_codes(df[hkey[0]]).str.cat([s4h_clean_codes(df[c]) for c in hkey[1:]], sep="-")
            pkey = next((k for k in survey.person_key if all(c in df.columns for c in k)), None)
            if pkey:
                df["_PKEY"] = s4h_clean_codes(df[pkey[0]]).str.cat([s4h_clean_codes(df[c]) for c in pkey[1:]], sep="-")
        elif survey.row_ids_without_key:
            df["HOUSEHOLD_ID"] = f"t{i}-r" + pd.Series(range(1, len(df) + 1), index=df.index).astype(str)
            df["_ROWID"] = True
        out.append(df)
    return out


def _is_in(series, values):
    return s4h_clean_codes(series).isin([str(v) for v in values]).fillna(False)


def _apply_rules(dfs, survey: Survey):
    # Rules on raw records: rows to keep, values filled from another column, codes built from yes/no flags
    out = []
    for df in dfs:
        for col, values in survey.keep_rows.items():
            match = _column(df, col)
            if match is not None:
                df = df[_is_in(df[match], values)]
        names = {_clean_column_name(c): c for c in df.columns}
        rules = survey.keep_rows_any
        if rules and all(col in names for rule in rules for col in rule):   # only tables with every column
            keep = pd.Series(False, index=df.index)
            for rule in rules:   # a row is kept when it meets every condition of at least one rule
                match = pd.Series(True, index=df.index)
                for col, values in rule.items():
                    match &= _is_in(df[names[col]], values)
                keep |= match
            df = df[keep]
        for target, rule in survey.fill_missing.items():
            t, s = _column(df, target), _column(df, rule["from"])
            if t is not None and s is not None:   # only in tables that ask the question
                source = s4h_clean_codes(df[s])
                source = source.where(source.isin([str(v) for v in rule["values"]]))
                df = df.assign(**{t: s4h_clean_codes(df[t]).fillna(source)})
        for target, flags in survey.derive_from_flags.items():
            df = s4h_code_from_flags(df, flags, target)
        out.append(df)
    return out


def _map_columns(dfs, survey: Survey, year: int):
    # Fill a column from another one with a mapping (e.g. regions from provinces)
    for rule in survey.map_columns:
        if year in rule["years"]:
            mapping = {str(k): v for k, v in survey.read_json(rule["mapping"]).items()}
            dfs = [df.assign(**{rule["target"]: s4h_clean_codes(df[rule["source"]]).map(mapping)})
                   if rule["source"] in df.columns and rule["target"] not in df.columns else df for df in dfs]
    return dfs


def _assemble(tables, variables, level, shared, person_vars):
    # One row per household or person. Every table holding these variables is stacked (a module can be split
    # over several tables, e.g. months with slightly different columns) and each ID keeps, for each column, the
    # first non-empty value, larger tables first. Households come from tables without person variables when
    # there are any (a members file may repeat the housing variables on every person).
    id_col = LEVELS[level]
    own = [v for v in variables if v not in shared]   # AREA alone does not make a household table
    candidates = []
    for df in tables:
        if "HOUSEHOLD_ID" not in df.columns or "EXP_FACTOR" not in df.columns or not any(v in df.columns for v in own):
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
        candidates = [df for df in candidates if not any(v in df.columns for v in person_vars)] or candidates
    if not candidates:
        return None
    candidates.sort(key=len, reverse=True)
    cols = ["YEAR", "HOUSEHOLD_ID", "PERSON_ID", *ADMIN_COLS, "EXP_FACTOR", *variables]
    stacked = pd.concat([df[[c for c in cols if c in df.columns]] for df in candidates], ignore_index=True)
    return stacked.groupby(id_col, sort=False).first().reset_index()


def _finalize(records, survey: Survey, variables, level, year):
    # Annual weight and its type, codes as numbers, harmonized column order
    df = records.copy()
    weight = pd.to_numeric(df.pop("EXP_FACTOR").astype("string").str.replace(",", ".", regex=False), errors="coerce")
    df["WEIGHT"] = weight / survey.periods_per_year
    for v in variables:
        if v in df.columns:
            df[v] = pd.to_numeric(s4h_clean_codes(df[v]), errors="coerce").astype("Int16")
    for c in ADMIN_COLS:
        if c in df.columns:
            # integer part of the unit code: ENHOGAR 2024 households use "10.2" (region 10, sub-region 2)
            df[c] = s4h_clean_codes(df[c]).str.replace(r"^(\d+)\.\d+$", r"\1", regex=True)
    df["YEAR"] = pd.to_numeric(df["YEAR"]).astype("Int16")
    df["WEIGHT_TYPE"] = survey.weight_type(level, year)
    first = [c for c in ["YEAR", *ADMIN_COLS, "HOUSEHOLD_ID", "PERSON_ID", "WEIGHT", "WEIGHT_TYPE"] if c in df.columns]
    return df[first + [v for v in variables if v in df.columns]]


def s4h_harmonize_records(survey: Survey, schema: Schema, year: int, scratch_dir: Union[str, Path],
                          cache_parquet: bool = True) -> Dict[str, Optional[pd.DataFrame]]:
    """
    Harmonized household and person records of one survey year.

    Steps: read the raw files, stack tables with nearly the same columns, keep the columns used, attach
    weights, build IDs, apply the survey rules, rename columns and map codes (column and value mappings of the
    year), then build one row per household and per person. Persons take their household's value of the
    variables of both tables (e.g. ``AREA``) when theirs is missing.

    Parameters
    ----------
    survey : Survey
    schema : Schema
        Harmonized variables and the table each one belongs to.
    year : int
    scratch_dir : str or Path
        Folder for files extracted from archives.
    cache_parquet : bool
        Convert text files to Parquet once (see :class:`~socio4health.Extractor`).

    Returns
    -------
    dict
        ``{"households": DataFrame or None, "persons": DataFrame or None}`` with ``YEAR``, the unit columns, IDs,
        ``WEIGHT`` (households or persons represented in the year), ``WEIGHT_TYPE`` and the variables (codes as
        ``Int16``).

    Raises
    ------
    NoDataError
        If the year has no folder, files, mapping or usable table.
    """
    column_map = survey.column_map(year)
    raw_columns = survey.raw_columns(year)
    dfs = _extract(survey, year, raw_columns, scratch_dir, cache_parquet)
    if not dfs:
        raise NoDataError(f"No {survey.reader.get('down_ext')} files for {year}")
    for df in dfs:
        df["YEAR"] = year

    if survey.vertical_merge:
        dfs = Harmonizer().s4h_vertical_merge(dfs, overlap_threshold=0.8, method="union", overlap_basis="larger")
    dfs = prune_columns(dfs, [*raw_columns, "YEAR"])
    factor, person = survey.factor, survey.person_factor
    if person:
        dfs = use_person_factor(dfs, factor["factor_col"], person["factor_col"], person["id_col"])
    if factor:
        dfs = merge_factor(dfs, factor["factor_col"], factor["id_col"], fallback_col=factor.get("fallback_col"))
    dfs = _apply_rules(_add_ids(dfs, survey), survey)
    dfs = harmonize_columns_by_year(dfs, year, column_map)
    dfs = apply_value_mappings(dfs, year, survey.value_map(year), column_aliases=column_map)
    dfs = _map_columns(dfs, survey, year)

    variables = {level: schema.variables_of(level) for level in LEVELS}
    shared = [v for v in variables["households"] if v in variables["persons"]]
    person_vars = [v for v in variables["persons"] if v not in shared]
    tables = {level: _assemble(dfs, variables[level], level, shared, person_vars) for level in LEVELS}
    households, persons = tables["households"], tables["persons"]
    if households is None and persons is None:
        raise NoDataError("No table has a household key, a weight and harmonized variables")
    # Persons keep their own value of the shared variables; their household's value fills the gaps
    if persons is not None and households is not None:
        for v in [v for v in shared if v in households.columns]:
            values = households[["HOUSEHOLD_ID", v]].dropna().drop_duplicates("HOUSEHOLD_ID").rename(columns={v: "_FILL"})
            persons = persons.merge(values, on="HOUSEHOLD_ID", how="left")
            filler = persons.pop("_FILL")
            persons[v] = persons[v].fillna(filler) if v in persons.columns else filler
    tables = {"households": households, "persons": persons}
    return {level: _finalize(df, survey, variables[level], level, year) if df is not None else None
            for level, df in tables.items()}


_PIPELINE_FILES = ("records.py", "survey.py", "schema.py", "harmonizer.py", "utils/harmonizer_utils.py")


def s4h_fingerprint(survey: Survey, schema: Schema) -> str:
    """
    Short hash of everything a survey's records depend on: its rules, the schema, its mapping files and IBGE
    dictionaries, and the pipeline code. :func:`s4h_build_records` recomputes the years whose fingerprint changed.
    """
    h = hashlib.sha256(json.dumps([survey.rules(), schema.variables], sort_keys=True, default=str).encode())
    package = Path(__file__).parent
    for f in [*(package / name for name in _PIPELINE_FILES), *survey.input_files()]:
        h.update(Path(f).read_bytes())
    return h.hexdigest()[:12]


def _build_year(survey: Survey, schema: Schema, year: int, output_dir: Path, cache_parquet: bool, log_level: int) -> dict:
    # One year: harmonize, write its tables and report. Runs in the main process or in a worker process.
    set_log_level(log_level)
    row, start = {"survey": survey.name, "year": year}, time.time()
    try:
        with open(output_dir / "logs" / f"{year}.log", "w", encoding="utf-8") as log, \
                contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
            try:
                tables = s4h_harmonize_records(survey, schema, year, output_dir / "_extractor" / str(year), cache_parquet)
                for level, df in tables.items():
                    target = output_dir / level / str(year)
                    if target.exists():
                        shutil.rmtree(target)
                    if df is not None:
                        df.insert(0, "ADMIN_LEVEL", survey.group_col)
                        df.insert(0, "SURVEY", survey.name)
                        df.insert(0, "COUNTRY", survey.country)
                        s4h_export(df, output_dir / level, names=[str(year)], overwrite=True)
                        row[level] = len(df)
            except Exception:
                traceback.print_exc()
                raise
        row["status"] = "ok"
    except NoDataError as exc:
        row["status"], row["error"] = "no data", str(exc)
    except Exception as exc:
        row["status"], row["error"] = "failed", f"{type(exc).__name__}: {exc}"
    row["seconds"] = round(time.time() - start, 1)
    return row


def s4h_build_records(survey: Survey, schema: Schema, output_dir: Union[str, Path], years: Optional[Sequence[int]] = None,
                      resume: bool = True, cache_parquet: bool = True, workers: int = 1) -> pd.DataFrame:
    """
    Harmonize the years of a survey into ``<output_dir>/households/<year>/`` and ``<output_dir>/persons/<year>/``.

    Each year is written (Parquet, with ``COUNTRY``, ``SURVEY`` and ``ADMIN_LEVEL`` columns) as soon as it is
    done; what it prints goes to ``<output_dir>/logs/<year>.log``. With ``resume``, years written with the same
    :func:`s4h_fingerprint` are skipped. A year that fails is reported and the others go on.

    With ``workers > 1`` several years are processed at the same time, each in its own process. Each process
    holds one year in memory, so the memory needed grows with ``workers``. On Windows and macOS, call it from
    a script under ``if __name__ == "__main__":``.

    Parameters
    ----------
    survey : Survey
    schema : Schema
    output_dir : str or Path
    years : list of int, optional
        Defaults to ``survey.years``.
    resume : bool
        Skip the years that are up to date (default ``True``).
    cache_parquet : bool
        Convert text files to Parquet once (see :class:`~socio4health.Extractor`).
    workers : int
        Years processed at the same time (default 1: one after the other).

    Returns
    -------
    pandas.DataFrame
        One row per year: ``status`` (``ok``, ``up to date``, ``no data``, ``failed``), rows written, seconds and
        error. Also saved as ``<output_dir>/run_report.csv``.
    """
    output_dir = Path(output_dir)
    (output_dir / "logs").mkdir(parents=True, exist_ok=True)
    stamp_file = output_dir / "fingerprints.json"
    stamps = json.loads(stamp_file.read_text()) if stamp_file.exists() else {}
    current = s4h_fingerprint(survey, schema)
    years = list(survey.years if years is None else years)
    rows = {}
    for year in years:
        if resume and stamps.get(str(year)) == current:
            rows[year] = {"survey": survey.name, "year": year, "status": "up to date"}
            print(f"{survey.name} {year}: up to date", flush=True)

    def done(row):
        rows[row["year"]] = row
        if row["status"] == "ok":   # stamps are written by this process only, one year at a time
            stamps[str(row["year"])] = current
            stamp_file.write_text(json.dumps(stamps, indent=2))
        print(f"{survey.name} {row['year']}: {row['status']} ({row.get('households', 0)} households, "
              f"{row.get('persons', 0)} persons, {row['seconds']} s) {row.get('error', '')}", flush=True)

    pending = [y for y in years if y not in rows]
    args = (survey, schema)
    rest = (output_dir, cache_parquet, logger.getEffectiveLevel())
    if workers > 1 and len(pending) > 1:
        with ProcessPoolExecutor(max_workers=min(workers, len(pending))) as pool:
            for future in as_completed([pool.submit(_build_year, *args, year, *rest) for year in pending]):
                done(future.result())
    else:
        for year in pending:
            done(_build_year(*args, year, *rest))
    report = pd.DataFrame([rows[y] for y in years])
    report.to_csv(output_dir / "run_report.csv", index=False)
    return report
