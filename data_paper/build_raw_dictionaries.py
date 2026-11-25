"""Build the raw-variable dictionaries of the data paper surveys (dictionaries/raw_<survey>_dict.xlsx).

Usage::

    python build_raw_dictionaries.py                 # every survey (S4H_DATA_ROOT or the default data folder)
    python build_raw_dictionaries.py ENHOGAR PNADC   # some surveys

For every raw variable the pipeline reads (mapped variables, inputs of the survey rules, weights, keys, units and
filters), each workbook lists, per period with the same documentation and mapping:

- 'Variables': the question, its role, the harmonized variable it feeds and, for fixed-width files, its position.
- 'Codes': every documented code and label, and the harmonized code it is mapped to.
- 'Sources': the documentation read for each year.

Questions and labels come from the official documentation (see survey_docs.py); the roles and harmonized codes from
surveys.json and the mapping files, so the workbooks always match the current mappings.
"""
import argparse
import os
import sys
from collections import OrderedDict
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from survey_docs import norm, survey_docs  # noqa: E402

from socio4health import Schema, s4h_load_surveys  # noqa: E402
from socio4health.survey import NoDataError  # noqa: E402

DATA_ROOT = Path(os.environ.get("S4H_DATA_ROOT", r"D:\EQUIPO\Documents HDD\Harmonize"))
SURVEYS = s4h_load_surveys(HERE / "surveys.json", DATA_ROOT)
SCHEMA = Schema.from_json(HERE / "mapping" / "harmonized_mapping.json")
OUT = HERE / "dictionaries"
ROLE_OF_TARGET = {"EXP_FACTOR": "weight", "ID": "identifier", "ADMIN_DIVISION": "unit", "ADMIN_DIVISION_2": "unit"}


def raw_variables(survey, year):
    """OrderedDict raw column -> (role, harmonized variable, value-mapping key) of one year."""
    cmap = survey.column_map(year)
    out = OrderedDict()

    def add(col, role, target, key=None):
        out.setdefault(col.upper(), (role, target, key))

    for key, target in cmap.items():
        for part in str(key).split("|"):
            if part in survey.derive_from_rules:
                for rule in survey.derive_from_rules[part]:
                    for col in rule["when"]:
                        add(col, f"rule input ({part})", target)
            elif part in survey.derive_from_flags:
                for col in survey.derive_from_flags[part]:
                    add(col, f"flag ({part})", target)
            else:
                add(part, ROLE_OF_TARGET.get(target, "mapped" if "|" not in key else "mapped (first non-empty)"),
                    target, key)
    for col, rule in survey.fill_missing.items():
        key = next((k for k in cmap if col in str(k).split("|")), None)
        add(rule["from"], f"fills {col}", cmap.get(key, ""), key)
    for col in list(survey.keep_rows) + [c for rule in survey.keep_rows_any for c in rule]:
        add(col, "filter (rows kept)", "")
    for factor in (survey.factor, survey.person_factor):
        if factor:
            add(factor["factor_col"], "weight", "EXP_FACTOR")
            if factor.get("fallback_col"):
                add(factor["fallback_col"], "weight (fallback)", "EXP_FACTOR")
    for alternatives in survey.household_key + survey.person_key:
        for col in alternatives:
            add(col, "key", "")
    return out


def compress(years):
    """[2007, 2008, 2009, 2011] -> '2007–2009, 2011'."""
    parts, start, prev = [], None, None
    for y in sorted(years):
        if start is None:
            start = prev = y
        elif y == prev + 1:
            prev = y
        else:
            parts.append(str(start) if start == prev else f"{start}–{prev}")
            start = prev = y
    if start is not None:
        parts.append(str(start) if start == prev else f"{start}–{prev}")
    return ", ".join(parts)


def build(name):
    survey = SURVEYS[name]
    groups = OrderedDict()   # (variable, signature) -> years
    sources = OrderedDict()
    for year in survey.years:
        try:
            variables, vmap = raw_variables(survey, year), survey.value_map(year)
        except NoDataError:
            continue
        docs = survey_docs(survey, DATA_ROOT, year)
        used = set()
        for col, (role, target, key) in variables.items():
            doc = docs.get(col)
            if role == "key" and doc is None:
                continue   # alternative keys of other years
            values = {norm(k): norm(v) for k, v in vmap.get(key, {}).items()} if key else {}
            labels = SCHEMA.labels(target) if target in SCHEMA.variables and not SCHEMA.is_numeric(target) else {}
            codes = OrderedDict((c, lab) for c, lab in (doc.codes.items() if doc else []))
            for c in values:   # mapped codes the documentation does not list
                codes.setdefault(c, None)
            code_rows = tuple((c, lab, values.get(c), labels.get(int(values[c])) if c in values
                               and values[c].lstrip("-").isdigit() else None) for c, lab in codes.items())
            sig = (role, target, doc.question if doc else None, doc.start if doc else None,
                   doc.size if doc else None, code_rows)
            groups.setdefault((col, sig), []).append(year)
            if doc:
                used.add(doc.source)
        sources[year] = sorted(used) or ["no local documentation"]
    return groups, sources


# ---------------------------------------------------------------- workbook
DARK, LIGHT = "1F3864", "D9E2F3"
thin = Side(style="thin", color="BFBFBF")
BORDER = Border(left=thin, right=thin, top=thin, bottom=thin)


def header(ws, row, titles, widths):
    for c, (title, width) in enumerate(zip(titles, widths), start=1):
        cell = ws.cell(row, c, title)
        cell.font = Font(bold=True, size=10, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor=DARK)
        cell.border = BORDER
        cell.alignment = Alignment(wrap_text=True, vertical="center")
        ws.column_dimensions[cell.column_letter].width = width
    ws.freeze_panes = ws.cell(row + 1, 1)


def write_row(ws, row, values, bold_first=False):
    for c, v in enumerate(values, start=1):
        cell = ws.cell(row, c, v)
        cell.font = Font(size=9, bold=bold_first and c == 1)
        cell.border = BORDER
        cell.alignment = Alignment(wrap_text=True, vertical="top")


def save(name, groups, sources):
    wb = Workbook()
    ws = wb.active
    ws.title = "Variables"
    fwf = name in ("PNAD", "PNADC")
    ws["A1"] = f"{name}: raw variables read by the data paper pipeline"
    ws["A1"].font = Font(bold=True, size=13)
    ws["A2"] = ("One row per raw variable and period with the same documentation and mapping. Role: how the pipeline "
                "uses it (mapped to a harmonized variable, input of a derived variable, weight, key, unit or filter). "
                "Questions and labels are the official ones (see 'Sources'); empty question: the variable is not in the "
                "documentation of those years.")
    ws["A2"].font = Font(size=9)
    ws["A2"].alignment = Alignment(wrap_text=True, vertical="top")
    titles = ["Variable", "Years", "Role", "Harmonized variable", "Question"] + (["Start", "Size"] if fwf else [])
    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=len(titles))
    ws.row_dimensions[2].height = 40
    header(ws, 4, titles, [14, 16, 26, 20, 80] + ([7, 6] if fwf else []))
    r = 5
    order = sorted(groups.items(), key=lambda kv: (min(kv[1]), list(groups).index(kv[0])))
    for (col, sig), years in order:
        role, target, question, start, size, _codes = sig
        write_row(ws, r, [col, compress(years), role, target, question] + ([start, size] if fwf else []), True)
        r += 1
    ws.auto_filter.ref = f"A4:{ws.cell(4, len(titles)).column_letter}{r - 1}"

    ws = wb.create_sheet("Codes")
    ws["A1"] = f"{name}: codes of the raw variables and their harmonized codes"
    ws["A1"].font = Font(bold=True, size=13)
    ws["A2"] = ("Documented label of each code and the harmonized code the value mapping gives it. Empty label: a "
                "mapped code the documentation does not list. Empty harmonized code: not mapped (undocumented codes "
                "become Missing in the record checks); inputs of derived variables are used by their rule.")
    ws["A2"].font = Font(size=9)
    ws["A2"].alignment = Alignment(wrap_text=True, vertical="top")
    ws.merge_cells("A2:G2")
    ws.row_dimensions[2].height = 40
    header(ws, 4, ["Variable", "Years", "Code", "Label", "Harmonized variable", "Harmonized code", "Harmonized label"],
           [14, 16, 8, 50, 20, 11, 40])
    r = 5
    for (col, sig), years in order:
        role, target, _q, _s, _z, code_rows = sig
        for code, label, hcode, hlabel in code_rows:
            write_row(ws, r, [col, compress(years), code, label, target if hcode is not None else None,
                              int(hcode) if hcode is not None and hcode.lstrip("-").isdigit() else hcode, hlabel], True)
            r += 1
    ws.auto_filter.ref = f"A4:G{max(r - 1, 4)}"

    ws = wb.create_sheet("Sources")
    ws["A1"] = f"{name}: documentation read per year"
    ws["A1"].font = Font(bold=True, size=13)
    header(ws, 3, ["Year", "Documentation"], [8, 110])
    for r, (year, files) in enumerate(sources.items(), start=4):
        write_row(ws, r, [year, "; ".join(files)], True)
    OUT.mkdir(exist_ok=True)
    path = OUT / f"raw_{name.lower()}_dict.xlsx"
    wb.save(path)
    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build dictionaries/raw_<survey>_dict.xlsx.")
    parser.add_argument("surveys", nargs="*", default=list(SURVEYS))
    for name in parser.parse_args().surveys:
        groups, sources = build(name)
        path = save(name, groups, sources)
        print(f"{name}: {path.relative_to(HERE).as_posix()} ({len({c for c, _ in groups})} variables, "
              f"{len(groups)} variable-periods)")
