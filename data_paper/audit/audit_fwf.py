"""Audit the mappings of the fixed-width surveys (PNAD, PNADC) against the IBGE dictionary of every year.

Usage::

    python audit/audit_fwf.py > audit_fwf.txt        # S4H_DATA_ROOT or the default data folder

For each survey, mapped variable and year: is the raw column in the dictionary, which dictionary codes the value
mapping does not cover (they would pass through unchanged), which mapped codes the dictionary does not have, and
the table code -> IBGE label -> harmonized code / label. Years with the same dictionary codes and mapping are
grouped. Flags are marked '<<<'.
"""
import os
import sys
from collections import OrderedDict
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
mappings_dir = Path(__file__).resolve().parent.parent
data_root = Path(os.environ.get("S4H_DATA_ROOT", r"D:\EQUIPO\Documents HDD\Harmonize"))
sys.path.insert(0, str(mappings_dir))
from survey_docs import codes_only, ibge_docs, norm  # noqa: E402

from socio4health import Schema, s4h_load_surveys  # noqa: E402

SURVEYS = s4h_load_surveys(mappings_dir / "surveys.json", data_root)
SCHEMA = Schema.from_json(mappings_dir / "mapping" / "harmonized_mapping.json")


def dictionaries(survey, year):
    try:
        return codes_only(ibge_docs(survey, year))
    except Exception as e:  # noqa: BLE001
        print(f"  ! {survey.name} {year}: dictionary not read ({e})")
        return {}


def raw_sources(survey, key):
    """Raw columns behind a mapping key: coalesce parts, and the flag columns of derived ones."""
    cols = []
    for part in str(key).split("|"):
        flags = survey.derive_from_flags.get(part)
        cols.append((part, flags))
    return cols


def rule_rows(rules, docs):
    """(output code, conditions with their labels, problems) for each rule of a derive_from_rules target."""
    out = []
    for rule in rules:
        conditions, missing = [], []
        for col, codes in rule["when"].items():
            doc = docs.get(col.upper())
            if doc is None:
                missing.append(col)
                conditions.append(f"{col} in {codes}")
                continue
            undocumented = [str(c) for c in codes if norm(c) not in doc]
            if undocumented and len(undocumented) == len(codes) and len(doc) >= 2:
                missing.append(f"{col} codes {undocumented}")
            shown = ", ".join(f"{c} {doc.get(norm(c), '?')[:20]}" for c in codes[:3]) + (" ..." if len(codes) > 3 else "")
            conditions.append(f"{col} in [{shown}]")
        out.append((norm(rule["code"]), " and ".join(conditions), missing))
    return out


for name in ("PNAD", "PNADC"):
    survey = SURVEYS[name]
    groups = OrderedDict()   # (key, signature) -> [years]
    details = {}
    for year in survey.years:
        dic = dictionaries(survey, year)
        if not dic:
            continue
        cmap, vmap = survey.column_map(year), survey.value_map(year)
        for key, target in cmap.items():
            if target not in SCHEMA.variables or SCHEMA.is_numeric(target):
                missing = [c for c in str(key).split("|") if c.upper() not in dic and c not in survey.derive_from_flags
                           and c not in survey.derive_from_rules]
                sig = ("exists", tuple(missing))
                groups.setdefault((key, target, sig), []).append(year)
                details[(key, target, sig)] = None
                continue
            values = {norm(k): norm(v) for k, v in vmap.get(key, vmap.get(int(key) if str(key).isdigit() else key, {})).items()}
            raw = OrderedDict()
            absent = []
            for col, flags in raw_sources(survey, key):
                if col in survey.derive_from_rules:   # derived from conditions: one row per output code
                    for code, text, missing in rule_rows(survey.derive_from_rules[col], dic):
                        raw[code] = f"{raw[code]} | {text}" if code in raw else text
                        absent.extend(m for m in missing if m not in absent)
                    continue
                if flags:   # derived: its codes are the flag codes
                    present = [f for f in flags if f.upper() in dic]
                    if not present:
                        absent.append(f"{col} (flags {list(flags)})")
                    for f, code in flags.items():
                        if f.upper() in dic:
                            raw.setdefault(norm(code), f"{f} = yes (only one)")
                elif col.upper() in dic:
                    for code, label in dic[col.upper()].items():
                        raw.setdefault(code, f"{col}: {label}")
                else:
                    absent.append(col)
            labels = SCHEMA.labels(target)
            rows = tuple((c, raw[c], values.get(c, "UNMAPPED"),
                          labels.get(int(values[c]), "?") if c in values and values[c].isdigit() else "")
                         for c in raw)
            extra = tuple(sorted(set(values) - set(raw), key=lambda x: (len(x), x)))
            sig = (tuple(absent), rows, extra)
            groups.setdefault((key, target, sig), []).append(year)

    print(f"\n{'=' * 100}\n{name}")
    for (key, target, sig), years in groups.items():
        span = f"{years[0]}-{years[-1]}" if len(years) > 1 else str(years[0])
        if sig[0] == "exists":
            if sig[1]:
                print(f"\n[{span}] {key} -> {target}: NOT IN DICTIONARY: {list(sig[1])}")
            continue
        absent, rows, extra = sig
        flags = []
        if absent:
            flags.append(f"NOT IN DICTIONARY: {list(absent)}")
        unmapped = [r[0] for r in rows if r[2] == "UNMAPPED"]
        if unmapped:
            flags.append(f"UNMAPPED codes: {unmapped}")
        if extra:
            flags.append(f"mapped codes not in dictionary: {list(extra)}")
        print(f"\n[{span}] {key} -> {target}" + (f"   <<< {'; '.join(flags)}" if flags else ""))
        for code, label, to, tolabel in rows:
            print(f"    {code:>3}  {label if ' in [' in label else label[:70]:70} -> {to:>8} {tolabel}")
