"""Audit the GEIH, ENAHO and ENHOGAR mappings: documented codes and labels, and codes found in the data.

Usage::

    python audit/audit_mappings.py ENHOGAR > audit_enhogar.txt     # one or more of GEIH ENAHO ENHOGAR
    python audit/audit_mappings.py GEIH --years 2007 2016 2024      # years whose data codes are read

For each survey, mapped categorical variable and year: the code -> documented label -> harmonized code / label
table (years with the same table are grouped), documented codes the value mapping does not cover, and, for the
data years, codes found in the raw data (Parquet caches or .sav files) that the mapping does not cover. Labels come
from the official documentation read by survey_docs.py. Flags are marked '<<<'.
"""
import argparse
import glob
import os
import sys
from collections import OrderedDict
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from survey_docs import codes_only, norm, survey_docs  # noqa: E402

sys.stdout.reconfigure(encoding="utf-8")
parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
parser.add_argument("surveys", nargs="*", default=["GEIH", "ENAHO", "ENHOGAR"])
parser.add_argument("--years", type=int, nargs="+", default=[2007, 2016, 2024],
                    help="years whose raw data codes are checked (default: %(default)s)")
args = parser.parse_args()
mappings_dir = Path(__file__).resolve().parent.parent
root = Path(os.environ.get("S4H_DATA_ROOT", r"D:\EQUIPO\Documents HDD\Harmonize"))
only = args.surveys
from socio4health import Schema, s4h_load_surveys  # noqa: E402

SURVEYS = s4h_load_surveys(mappings_dir / "surveys.json", root)
SCHEMA = Schema.from_json(mappings_dir / "mapping" / "harmonized_mapping.json")
DATA_YEARS = args.years


# ---------------------------------------------------------------- codes in the data
def data_codes(survey, year, columns):
    """{column: set of codes} found in the Parquet caches of the year's raw files."""
    found = {c.upper(): set() for c in columns}
    con = duckdb.connect()
    for part in survey.parts:
        folder = root / part["path"].format(year=year)
        for cache in glob.glob(str(folder / "**" / "*.parquet"), recursive=True):
            p = Path(cache)
            if p.is_dir() or p.parent.suffix == ".parquet" and p.name.startswith("part"):
                src = p.parent if p.parent.suffix == ".parquet" else p
            else:
                src = p
            pattern = (src / "*.parquet").as_posix() if src.is_dir() else src.as_posix()
            try:
                cols = {r[0].upper(): r[0] for r in con.sql(f"DESCRIBE SELECT * FROM read_parquet('{pattern}')").fetchall()}
            except Exception:  # noqa: BLE001
                continue
            for c in found:
                if c in cols:
                    vals = con.sql(f'SELECT DISTINCT CAST("{cols[c]}" AS VARCHAR) FROM read_parquet(\'{pattern}\')').fetchall()
                    found[c] |= {norm(v[0]) for v in vals if v[0] is not None and str(v[0]).strip() not in ("", "nan")}
        # SPSS files (ENAHO) have no Parquet cache: read the columns directly
        import pyreadstat
        for sav in glob.glob(str(folder / "**" / "*.sav"), recursive=True):
            try:
                _, meta = pyreadstat.read_sav(sav, metadataonly=True)
            except Exception:  # noqa: BLE001
                continue
            present = {c.upper(): c for c in meta.column_names}
            use = [present[c] for c in found if c in present]
            if use:
                df, _ = pyreadstat.read_sav(sav, usecols=use)
                for col in use:
                    found[col.upper()] |= {norm(v) for v in df[col].dropna().unique() if str(v).strip() not in ("", "nan")}
    return found


# ---------------------------------------------------------------- audit
def categorical(doc):
    """A column with labelled categories; numeric columns (grades, years) only label special codes (0, 98, 99)
    or repeat the number as label."""
    labelled = [k for k, v in doc.items() if k.isdigit() and int(k) < 90 and str(v).strip() != k]
    return len(labelled) >= 2


def rule_rows(rules, docs):
    """(output code, conditions with their labels, problems) for each rule of a derive_from_rules target."""
    out = []
    for rule in rules:
        conditions, missing = [], []
        for col, codes in rule["when"].items():
            doc = docs.get(col.upper())
            if not doc:
                if col.upper() not in docs:
                    missing.append(col)
                conditions.append(f"{col} in {codes}")
                continue
            undocumented = [str(c) for c in codes if norm(c) not in doc]
            if undocumented and len(undocumented) == len(codes) and categorical(doc):
                missing.append(f"{col} codes {undocumented}")
            shown = ", ".join(f"{c} {doc.get(norm(c), '?')[:20]}" for c in codes[:3]) + (" ..." if len(codes) > 3 else "")
            conditions.append(f"{col} in [{shown}]")
        out.append((norm(rule["code"]), " and ".join(conditions), missing))
    return out


for name in only:
    survey = SURVEYS[name]
    groups, data_flags = OrderedDict(), []
    seen_cache = {}
    for year in survey.years:
        try:
            cmap, vmap = survey.column_map(year), survey.value_map(year)
        except Exception as e:  # noqa: BLE001
            print(f"  ! {name} {year}: {e}")
            continue
        docs = codes_only(survey_docs(survey, root, year))
        fill = {t.upper(): r for t, r in survey.fill_missing.items()}
        for key, target in cmap.items():
            if target not in SCHEMA.variables or SCHEMA.is_numeric(target):
                continue
            values = {norm(k): norm(v) for k, v in vmap.get(key, {}).items()}
            raw, absent, sources = OrderedDict(), [], []
            for col in str(key).split("|"):
                if col in survey.derive_from_rules:   # derived from conditions: one row per output code
                    for code, text, missing in rule_rows(survey.derive_from_rules[col], docs):
                        raw[code] = f"{raw[code]} | {text}" if code in raw else text
                        absent.extend(m for m in missing if m not in absent)
                    continue
                flags = survey.derive_from_flags.get(col)
                if flags:
                    for f, code in flags.items():
                        raw.setdefault(norm(code), f"{f} = yes")
                        sources.append(f)
                    continue
                sources.append(col)
                if col.upper() in docs:
                    for code, label in docs[col.upper()].items():
                        raw.setdefault(code, f"{col}: {label}")
                else:
                    absent.append(col)
                if col.upper() in fill:   # values taken from another column (e.g. P302 <- P301A)
                    src = fill[col.upper()]["from"]
                    for code in map(norm, fill[col.upper()]["values"]):
                        raw.setdefault(code, f"{src} (fill_missing): {docs.get(src.upper(), {}).get(code, '?')}")
            labels = SCHEMA.labels(target)
            rows = tuple((c, raw[c], values.get(c, "UNMAPPED"),
                          labels.get(int(values[c]), "?") if c in values and values[c].lstrip("-").isdigit() else "")
                         for c in raw)
            extra = tuple(sorted(set(values) - set(raw), key=lambda x: (len(x), x)))
            sig = (key, target, tuple(absent), rows, extra)
            groups.setdefault(sig, []).append(year)
            if year in DATA_YEARS:
                cache_key = (year, tuple(sources))
                found = seen_cache.get(cache_key) or data_codes(survey, year, sources)
                seen_cache[cache_key] = found
                codes = set().union(*[found.get(s.upper(), set()) for s in sources if s.upper() in found])
                if any(s in survey.derive_from_flags or s in survey.derive_from_rules for s in str(key).split("|")):
                    codes = set()   # derived columns: their codes come from the rules, checked above
                unmapped = sorted(codes - set(values) - {""}, key=lambda x: (len(x), x))
                if unmapped:
                    data_flags.append(f"[{year}] {key} -> {target}: codes in the data not mapped: {unmapped[:15]}"
                                      + (" ..." if len(unmapped) > 15 else ""))

    print(f"\n{'=' * 100}\n{name}")
    for (key, target, absent, rows, extra), years in groups.items():
        span = f"{years[0]}-{years[-1]}" if len(years) > 1 else str(years[0])
        notes = []
        if absent:
            notes.append(f"NOT DOCUMENTED: {list(absent)}")
        unmapped = [r[0] for r in rows if r[2] == "UNMAPPED"]
        if unmapped:
            notes.append(f"UNMAPPED documented codes: {unmapped}")
        if extra:
            notes.append(f"mapped codes not documented: {list(extra)}")
        print(f"\n[{span}] {key} -> {target}" + (f"   <<< {'; '.join(notes)}" if notes else ""))
        for code, label, to, tolabel in rows:
            print(f"    {code:>4}  {label if ' in [' in label else label[:72]:72} -> {to:>8} {tolabel}")
    print(f"\n--- {name}: codes found in the data ({DATA_YEARS}) that the mapping does not cover")
    for line in data_flags or ["none"]:
        print("  " + line)
