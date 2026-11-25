"""Rebuild harmonization_dictionary.xlsx from the current mappings and survey rules.

Usage::

    python build_dictionary.py            # S4H_DATA_ROOT or the default data folder

Sheets: 'Variable mapping' (harmonized variable -> raw variable per survey and period), 'Harmonized codes'
(codes, labels and universes of mapping/harmonized_mapping.json), 'Rules' (every survey rule, in plain words) and
'Mapping decisions' (choices of variables and categories, with their reason). 'Variable mapping' and 'Harmonized
codes' are read from the mapping files; RULES and DECISIONS below are written by hand: update them when a rule or
a mapping decision changes.
"""
import os
import sys
from pathlib import Path

from openpyxl import Workbook
from openpyxl.comments import Comment
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

d = Path(__file__).resolve().parent
DATA_ROOT = Path(os.environ.get("S4H_DATA_ROOT", r"D:\EQUIPO\Documents HDD\Harmonize"))
from socio4health import Schema, s4h_load_surveys  # noqa: E402
from socio4health.survey import NoDataError  # noqa: E402

SURVEYS = s4h_load_surveys(d / "surveys.json", DATA_ROOT)
SCHEMA = Schema.from_json(d / "mapping" / "harmonized_mapping.json")
LABEL = {"GEIH": "GEIH (Colombia)", "ENAHO": "ENAHO (Peru)", "PNAD": "PNAD (Brazil)", "PNADC": "PNADC (Brazil)",
         "ENHOGAR": "ENHOGAR (Dominican Republic)"}
EXTRA = {   # mapped columns that are not harmonized variables
    "ADMIN_DIVISION": "First-level subnational unit",
    "ADMIN_DIVISION_2": "Second-level subnational unit (where available)",
    "EXP_FACTOR": "Expansion factor (WEIGHT in the record tables)",
    "ID": "Household / dwelling identifier in the mapping (records use their own HOUSEHOLD_ID)",
}

# ---------------------------------------------------------------- rules, in plain words
# (survey, years, variable, rule type, raw variables, rule)
RULES = [
    ("All", "All", "All categorical", "Missing codes",
     "-", "Code 0 = Missing. Raw codes the value mapping does not list (undocumented codes, e.g. 9 / 99 without a label) "
          "are set to Missing by the record checks, which report how many there are."),
    ("All", "All", "P_EDUCATION, P_LITERACY", "Universe",
     "P_AGE", "Shares of P_EDUCATION count people aged 25 and over; shares of P_LITERACY, people aged 15 and over."),
    ("All", "All", "EXP_FACTOR", "Weights",
     "-", "WEIGHT is the expansion factor of each record: SUM(WEIGHT) estimates households / persons. Years with only "
          "relative weights (WEIGHT_TYPE = relative) are valid for proportions, not for totals."),
    ("All", "All", "Several", "First non-empty",
     "A (else B)", "A mapping key 'A|B' (shown 'A (else B)'): the harmonized variable takes A, and B where A is empty."),

    ("GEIH", "2007–2019", "AREA", "Files by area",
     "CLASE", "Files are split by area and the area is only in the file name: Cabecera -> CLASE 1 (urban), "
              "Resto -> CLASE 2 (rural). 'Área' files repeat Cabecera households and are skipped."),
    ("GEIH", "All", "ID", "Column names",
     "DIRECTORIO", "Some files spell the household ID differently (BOM, lowercase): the variants are read as DIRECTORIO."),
    ("GEIH", "All", "EXP_FACTOR", "Weights",
     "FEX_C18, FEX_C", "FEX_C18 (CNPV 2018 projection); FEX_C where FEX_C18 is missing (8 departments in 2020). Each "
                       "monthly sample is expanded to the whole population: weights are divided by 12."),
    ("GEIH", "All", "H_WATER", "Derived variable",
     "P4030S5, P5050", "P4030S5F = 1 (aqueduct) when the dwelling has the aqueduct service (P4030S5 = 1). H_WATER takes "
                       "P4030S5F, else the drinking-water source P5050."),
    ("GEIH", "2007–2020", "P_EDUCATION", "Derived variable",
     "P6210, P6210S1, P6220", "Completed education (P6210C), first rule met: tertiary title (P6220 3-5) -> 4; bachiller "
                              "title (P6220 2) -> 3; secundaria or a higher level (P6210 4+), or primaria with 5th grade "
                              "approved (P6210 3 and P6210S1 5) -> 2; none, preschool or primaria (P6210 1-3) -> 1."),
    ("GEIH", "2021–2025", "P_EDUCATION", "Derived variable",
     "P3042, P3042S1, P3043", "Completed education (P3042C), first rule met: normalista, técnico, tecnólogo, universitario "
                              "or postgraduate title (P3043 4-10) -> 4; bachiller title (P3043 2-3) -> 3; secundaria or a "
                              "higher level (P3042 4+), or primaria with 5th grade approved (P3042 3 and P3042S1 5) -> 2; "
                              "none, preschool or primaria (P3042 1-3) -> 1."),

    ("ENAHO", "All", "All", "Rows kept",
     "RESULT", "Only completed or partly completed interviews (RESULT 1-2)."),
    ("ENAHO", "All", "All (persons)", "Rows kept",
     "P204, P205, P206", "Only usual residents: members not away 30+ days (P204 = 1 and P205 = 2) and non-members present "
                         "30+ days (P204 = 2 and P206 = 1)."),
    ("ENAHO", "All", "ADMIN_DIVISION", "Derived variable",
     "UBIGEO", "Department: the first two digits of the district UBIGEO, which is kept in ADMIN_DIVISION_2 "
               "(mapping/per_mapping/ubigeo_department.json). Shares are computed by department, the finest level at "
               "which ENAHO is representative."),
    ("ENAHO", "All", "EXP_FACTOR", "Weights",
     "FACTOR07, FACPOB07", "Households: FACTOR07. Persons: FACPOB07 (module 200), joined by CONGLOME, VIVIENDA, HOGAR and "
                           "CODPERSO."),
    ("ENAHO", "All", "P_LITERACY", "Filled from another variable",
     "P302, P301A", "P302 is only asked below completed primary: where it is empty, people with P301A 4-11 take that code, "
                    "which the value mapping counts as literate."),

    ("PNAD", "All", "All", "Files",
     "-", "Households (dom) and persons (pes) are separate fixed-width files, read with the IBGE dictionary of each year."),
    ("PNAD", "All", "All (households)", "Rows kept",
     "V0104", "Only interviewed dwellings (V0104 = 1)."),
    ("PNAD", "All", "H_WATER", "Derived variable",
     "V0211, V0213", "V0213F = 2 (piped outside the dwelling) when the plot gets piped network water (V0213 = 1). H_WATER "
                     "takes V0213F, else V0211 (piped into at least one room: 1 yes, 3 no)."),
    ("PNAD", "2007–2011", "P_EDUCATION", "Derived variable",
     "V4745, V4803", "Completed education (V4745C), first rule met: superior completo (V4745 7) -> 4; médio completo or "
                     "superior incompleto (5-6) -> 3; 4+ years of study (V4803 5+) or fundamental completo / médio "
                     "incompleto (3-4) -> 2; less (1-2) -> 1."),
    ("PNAD", "2001–2006", "P_EDUCATION", "Thresholds",
     "V4703", "Years of study: 0-3 -> 1, 4-10 -> 2, 11-14 -> 3, 15+ -> 4."),

    ("PNADC", "All", "All", "Files",
     "-", "Fixed-width files read with the IBGE dictionary of each year (visit 1; visit 5 in 2020-2021, without the "
          "housing module). Housing variables start in 2016."),
    ("PNADC", "All", "ADMIN_DIVISION_2", "Subnational unit",
     "Capital", "Shares are computed by state (UF), as for PNAD; the state capital is kept in ADMIN_DIVISION_2."),
    ("PNADC", "All", "H_WATER", "First non-empty",
     "S01010, S01007", "H_WATER takes where the water arrives (S01010), else its source (S01007)."),
    ("PNADC", "2019–2025", "H_COOK", "Derived variable",
     "S01016A1-A5, S01016B", "S01016F = the only fuel marked 'yes' among S01016A1-A5. H_COOK takes the most used fuel "
                             "(S01016B, asked when 2+ fuels are used), else S01016F."),
    ("PNADC", "2016–2018", "H_COOK", "Derived variable",
     "S010161-S010164", "No 'most used' question: one column per fuel (S010161F gas, S010162F wood, S010163F "
                        "electricity, S010164F other); H_COOK takes the first one marked in that order. Wood is "
                        "underestimated (households using gas and wood count as gas)."),
    ("PNADC", "All", "P_EDUCATION", "Derived variable",
     "VD3004, VD3005", "Completed education (VD3004C), first rule met: superior completo (VD3004 7) -> 4; médio completo "
                       "or superior incompleto (5-6) -> 3; 5+ years of study (VD3005) or fundamental completo / médio "
                       "incompleto (3-4) -> 2; less (1-2) -> 1."),

    ("ENHOGAR", "All", "All (households)", "Rows kept",
     "HRESUL, HRESULT, HH46", "Only completed interviews (HRESUL / HRESULT = 1; HH46 = 1 in 2019)."),
    ("ENHOGAR", "2006–2013", "ID", "Identifiers",
     "-", "The public files have no identifiers: rows get their own ID and persons are not linked to households."),
    ("ENHOGAR", "2006, 2008", "ADMIN_DIVISION", "Derived variable",
     "HPROVI", "Region from the province (mapping/rd_mappings/region_mapping.json): 2006 has no region variable and "
               "the 2008 REGION has 4 codes that are not the 10 planning regions."),
    ("ENHOGAR", "2006, 2008, 2010, 2011, 2012, 2014, 2019", "EXP_FACTOR", "Weights",
     "-", "Relative (normalized) weights (persons: 2006, 2008, 2010, 2014, 2019): valid for proportions; POPULATION / "
          "HOUSEHOLDS are left empty in the shares."),
    ("ENHOGAR", "2019", "H_WATER", "First non-empty",
     "WS2, WS1", "H_WATER takes the source of water for household use (WS2), else the drinking-water source (WS1)."),
    ("ENHOGAR", "All except 2006", "P_EDUCATION", "Derived variable",
     "level + last grade approved", "Completed education, one rule set per year (<level>C), first rule met: postgraduate, "
                                    "or university with 4+ years -> 4; university with fewer years, or media / secundaria "
                                    "with 4+ years -> 3; media with fewer years, or básica / primaria with 6+ grades -> 2; "
                                    "initial, none or básica with fewer grades -> 1; never attended school (where asked: "
                                    "2011-2013, 2015, 2016, 2018, 2022, 2024) -> 1."),
    ("ENHOGAR", "2006", "P_EDUCATION", "Derived variable",
     "P2015, P2016, P2017, P2018", "As the other years with the grade attended (P2016) and its level (P2017); a grade "
                                   "counts only when passed (P2018 = 1). Never attended (P2015 = 2) -> 1."),
]


# ---------------------------------------------------------------- mapping decisions and their reasons
# (survey, years, variable, decision, reason)
DECISIONS = [
    ("All", "All", "H_WATER",
     "H_WATER is where / how the dwelling receives its water (piped inside, piped outside, not piped, other), from "
     "the question on water for household use, not the drinking-water source.",
     "Definition of the variable. Drinking water counts bottled or rain water as 'other' even in dwellings connected "
     "to the network (e.g. San Andrés and Vaupés in Colombia)."),
    ("All", "All", "H_WATER",
     "Public taps and water in the yard, the plot or a neighbour's tap -> piped outside the dwelling; wells, springs "
     "and rivers -> not piped; trucks, rain and bottled water -> other.",
     "Same criterion in every survey."),
    ("All", "All", "H_ROOF", "Zinc, aluminium, calamina and metal sheets -> Other; clay tiles -> tiles.",
     "Same criterion in every survey."),
    ("All", "All", "H_SANITARY",
     "Latrines (with or without seat) -> private disposal; no sanitation service -> not connected.",
     "Same criterion in every survey; 'no service' is an answer, not missing data."),
    ("All", "All", "P_EDUCATION",
     "Completed level, not the level reached or attended. Primary completed = 5-6 grades (Brazil: the old 4-year "
     "primário, 4 years of study in PNAD and 5 on the 9-year scale of PNADC).",
     "Comparability: the level reached counts people who did not finish it."),
    ("All", "All", "All categorical",
     "Documented codes 'sin información', 'no sabe', 'ignorado' -> Missing in the value mapping; undocumented codes are "
     "not mapped (the record checks set them to Missing).",
     "Only documented answers are mapped."),

    ("GEIH", "All", "H_WATER",
     "Households with the aqueduct service (P4030S5 = 1) count as piped inside; the others keep the drinking-water "
     "source P5050.",
     "P5050 is the source of drinking water and GEIH does not ask where the water arrives."),
    ("GEIH", "All", "H_WATER", "Wells with or without a pump (P5050 3-4) -> not piped; public tap (7) -> piped outside.",
     "Same criterion as the other surveys."),
    ("GEIH", "All", "H_SANITARY / H_PROPERTY", "'No tiene servicio sanitario' -> not connected; tenure 'Otra' -> Other.",
     "They were Missing."),
    ("GEIH", "All", "P_EDUCATION", "Title and last grade approved instead of the level alone (P6210 / P3042).",
     "The level is the highest reached, not completed."),

    ("ENAHO", "2004–2011 / 2012–2024", "H_SANITARY", "P111 up to 2011, P111A from 2012 (separate column blocks).",
     "Different questions and codes (P111A adds latrine and other)."),
    ("ENAHO", "2011", "H_SANITARY", "P111 = 7 -> not connected.",
     "Code not in the 2011 dictionary (range 1-6, 246 households); 7 is 'Otra' in P111A from 2012."),
    ("ENAHO", "2004–2011", "P_ETHNIC", "Not mapped before 2012.",
     "The question is not asked; in 2004 P558C is another question."),
    ("ENAHO", "2004–2006", "H_WALLS", "Mapped with the 2007+ codes.",
     "P102 had 8 codes in 2004-2006 and 9 from 2007 (adobe and tapia split): 2004-2006 walls are not comparable."),
    ("ENAHO", "All", "H_COOK / H_PROPERTY / P_ETHNIC / P_EDUCATION",
     "Kerosene -> Other fuel; 'Propia, por invasión' -> Other tenure; 'Perteneciente a otro pueblo indígena' -> "
     "Indigenous; 'Básica especial' -> Other.",
     "Kerosene was unmapped and passed as code 4 (Coal); the others were in a wrong category."),

    ("PNAD", "All", "H_WATER", "V0211 (piped into a room) and V0213 (piped to the plot) instead of V0212.",
     "V0212 (source of the piped water) is only asked when the water is piped inside: the others were Missing."),
    ("PNAD", "All", "P_EDUCATION", "V4745 (2007-2011) and years of study V4703 (2001-2006) instead of V0607 / V6007.",
     "V0607 / V6007 are the highest course attended, only asked to people not attending school."),
    ("PNAD", "All", "H_ROOF", "'Outro material' -> Other.", "It was Missing."),

    ("PNADC", "All", "H_WATER", "S01010 (where the water arrives) instead of S01007.", "S01007 is the source of the water."),
    ("PNADC", "All", "H_FLOOR / H_ROOF / H_ELECTRICITY",
     "Floor: Terra -> earth, Madeira -> wood. Roof: laje -> concrete, madeira -> wood, zinc / other -> Other. "
     "Electricity: 2 (não utiliza) -> No.",
     "The codes were shifted with respect to the IBGE dictionary (earth floor came out as 0%)."),
    ("PNADC", "2012–2018 / 2019–2025", "H_SANITARY / H_COOK",
     "S01012 and S010161-S010164 up to 2018; S01012A and S01016A1-A5 / S01016B from 2019 (separate column blocks).",
     "The questions changed in 2019."),
    ("PNADC", "2016–2018", "H_COOK", "Households using several fuels take the first in the order gas > wood > "
     "electricity > other.",
     "No 'most used' question; in 2024 this order matches the most used fuel for 94% of households but gives 0.7% wood "
     "instead of 4.1%."),
    ("PNADC", "All", "P_EDUCATION", "VD3004 and VD3005 instead of V3009A.",
     "V3009A is the highest course attended, only asked to people not attending school."),

    ("ENHOGAR", "2006, 2008, 2010, 2012, 2013, 2015", "H_WATER",
     "Water for household use: P3013, H2026, WS2, H211, H106, H109.",
     "The mapped questions (P3012, H2023, WS1, H210, H105, H108) were drinking water (e.g. 57% bottled water in 2008)."),
    ("ENHOGAR", "2024", "H_WATER", "V109 (how the water arrives) instead of V110.", "V110 is only the source."),
    ("ENHOGAR", "All", "H_TYPE / H_WALLS / H_ROOF / H_ELECTRICITY",
     "Casa independiente, dúplex, en hilera and compartida con negocio -> House; pieza en cuartería, barracón -> Room; "
     "local no destinado, en construcción -> Other. Zinc walls -> scrap / miscellaneous, zinc roofs -> Other. Other "
     "lighting -> no electricity; solar panels -> electricity.",
     "Each year had its own criterion."),
    ("ENHOGAR", "All", "H_SANITARY",
     "Inodoro -> sewer; latrines (con or sin cajón) -> private disposal; no service -> not connected.",
     "'Letrina con cajón' and 'sin cajón' were mapped to different categories; 'no service' was Missing or invalid. "
     "ENHOGAR does not say whether a toilet goes to a sewer or a septic tank."),
    ("ENHOGAR", "2014", "H_COOK / H_FLOOR / H_ROOF",
     "LPG -> tanked gas, natural gas -> piped gas; cement floor -> cement; metal / zinc roof -> Other.",
     "They were swapped or in a wrong category."),
    ("ENHOGAR", "2012, 2015", "P_EDUCATION", "Highest level attended by everyone (H327B, H212).",
     "H329B and H215 are the level of current students: people not studying were Missing."),
    ("ENHOGAR", "2006", "P_EDUCATION", "Level P2017 with grade P2016 and passed P2018.", "P2016 is the grade, not the level."),
    ("ENHOGAR", "2010", "P_EDUCATION", "ED6A kept.", "The public file has no other level variable."),
    ("ENHOGAR", "2008", "All", "Kept, with partial coverage.",
     "The 2008 sample covers only 7 of the 32 provinces (Distrito Nacional, Santiago, Valverde, Duarte, Azua, "
     "Elías Piña, La Romana): it is not nationally representative and each region is only partly covered, so its "
     "national and regional shares are not comparable with the other years."),
]


def years_of(spec, survey):
    if spec in ("All",):
        return set(SURVEYS[survey].years) if survey in SURVEYS else set()
    out = set()
    for part in spec.replace("All except ", "").split(","):
        part = part.strip()
        if "–" in part:
            a, b = part.split("–")
            out |= set(range(int(a), int(b) + 1))
        elif part.isdigit():
            out.add(int(part))
    if spec.startswith("All except"):
        out = set(SURVEYS[survey].years) - out
    return out


# ---------------------------------------------------------------- mapping table
def describe(survey, key):
    """Raw variables behind a mapping key, as shown in the sheet."""
    parts = []
    for col in str(key).split("|"):
        if col in survey.derive_from_rules:
            cols = []
            for rule in survey.derive_from_rules[col]:
                cols += [c for c in rule["when"] if c not in cols]
            parts.append(" + ".join(cols))
        elif col in survey.derive_from_flags:
            flags = list(survey.derive_from_flags[col])
            parts.append(flags[0] if len(flags) == 1 else f"{flags[0]}–{flags[-1][-2:]}")
        elif col.upper() in {t.upper() for t in survey.fill_missing}:
            parts.append(f"{col} (else {survey.fill_missing[col]['from']})")
        else:
            parts.append(col)
    return parts[0] + (f" (else {', '.join(parts[1:])})" if len(parts) > 1 else "")


rows = list(SCHEMA.variables) + list(EXTRA)
columns = []   # (survey, period label, {variable: text}, years)
for name, survey in SURVEYS.items():
    by_year = []
    for year in survey.years:
        try:
            cmap = survey.column_map(year)
        except NoDataError:
            continue
        cells = {}
        for key, target in cmap.items():
            cells.setdefault(target, []).append(describe(survey, key))
        for rule in survey.map_columns:   # a column filled from another one with a mapping file
            if year in rule["years"] and rule["source"] in cells and rule["target"] not in cells:
                cells[rule["target"]] = [f"{' / '.join(cells[rule['source']])} (mapped)"]
        by_year.append((year, {t: " / ".join(v) for t, v in cells.items()}))
    for year, cells in by_year:   # consecutive years with the same mapping share a column (ENHOGAR: one per year)
        same = columns and columns[-1][0] == name and columns[-1][2] == cells
        if same and name != "ENHOGAR":
            columns[-1][3].append(year)
        else:
            columns.append([name, None, cells, [year]])
for col in columns:
    ys = col[3]
    col[1] = str(ys[0]) if len(ys) == 1 else f"{ys[0]}–{ys[-1]}"

# ---------------------------------------------------------------- styles
DARK, LIGHT = "1F3864", "D9E2F3"
thin = Side(style="thin", color="BFBFBF")
BORDER = Border(left=thin, right=thin, top=thin, bottom=thin)
HEAD = dict(font=Font(bold=True, size=10, color="FFFFFF"), fill=PatternFill("solid", fgColor=DARK), border=BORDER)
SUB = dict(font=Font(bold=True, size=10), fill=PatternFill("solid", fgColor=LIGHT), border=BORDER)


def style(cell, font=None, fill=None, border=BORDER, h="left", wrap=True):
    if font:
        cell.font = font
    if fill:
        cell.fill = fill
    cell.border = border
    cell.alignment = Alignment(horizontal=h, vertical="center" if h == "center" else "top", wrap_text=wrap)


wb = Workbook()
ws = wb.active
ws.title = "Variable mapping"
ncol = 2 + len(columns)
ws["A1"] = "Harmonized variable → original variable name, per survey and period"
ws["A1"].font = Font(bold=True, size=13)
ws["A2"] = ("Each cell is the raw variable mapped to the harmonized one in that survey and period. 'X (else Y)': X when "
            "it has a value, otherwise Y. 'A + B + C': a variable derived from several columns. Several names separated "
            "by ' / ': different files of the same survey-year. A red triangle marks a survey rule (see the cell comment "
            "or the Rules sheet). Empty: not available.")
ws["A2"].font = Font(size=9)
ws["A2"].alignment = Alignment(wrap_text=True, vertical="top")
ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=ncol)
ws.row_dimensions[2].height = 42
for c, text in ((1, "Harmonized variable"), (2, "Description")):
    ws.cell(4, c, text)
    ws.merge_cells(start_row=4, start_column=c, end_row=5, end_column=c)
    style(ws.cell(4, c), **HEAD)
    style(ws.cell(5, c), **HEAD)
start = 3
for name in SURVEYS:
    span = [i for i, col in enumerate(columns) if col[0] == name]
    a, b = start + span[0], start + span[-1]
    ws.cell(4, a, LABEL[name])
    if b > a:
        ws.merge_cells(start_row=4, start_column=a, end_row=4, end_column=b)
    for c in range(a, b + 1):
        style(ws.cell(4, c), h="center", **HEAD)
for i, col in enumerate(columns):
    ws.cell(5, start + i, col[1])
    style(ws.cell(5, start + i), h="center", **SUB)
for r, var in enumerate(rows, start=6):
    desc = SCHEMA.variables[var]["DESCRIPTION"].strip() if var in SCHEMA.variables else EXTRA[var]
    ws.cell(r, 1, var)
    style(ws.cell(r, 1), font=Font(bold=True, size=10))
    ws.cell(r, 2, desc)
    style(ws.cell(r, 2), font=Font(size=9))
    for i, (name, _, cells, years) in enumerate(columns):
        cell = ws.cell(r, start + i, cells.get(var))
        style(cell, font=Font(size=9), h="center")
        notes = [text for s, ys, v, _t, _raw, text in RULES
                 if s == name and v.split(" ")[0] in (var,) and years_of(ys, name) & set(years)]
        if cells.get(var) and notes:
            cell.comment = Comment("\n\n".join(notes), "socio4health", width=420, height=160)
ws.freeze_panes = "C6"
ws.column_dimensions["A"].width = 19
ws.column_dimensions["B"].width = 42
for i in range(len(columns)):
    ws.column_dimensions[ws.cell(5, start + i).column_letter].width = 15

# ---------------------------------------------------------------- harmonized codes
ws = wb.create_sheet("Harmonized codes")
ws["A1"] = "Harmonized variables and their codes"
ws["A1"].font = Font(bold=True, size=13)
ws["A2"] = ("Code 0 = Missing in every categorical variable. Universe: the people the shares of a variable count "
            "(empty: everyone).")
ws["A2"].font = Font(size=9)
r = 4
for level, title in (("households", "Households (households table)"), ("persons", "Persons (persons table)")):
    ws.cell(r, 1, title).font = Font(bold=True, size=11)
    r += 1
    for c, text in enumerate(("Variable", "Description", "Universe", "Code", "Option"), start=1):
        ws.cell(r, c, text)
        style(ws.cell(r, c), **HEAD)
    r += 1
    for var in SCHEMA.variables_of(level):
        if level == "persons" and var in SCHEMA.variables_of("households"):
            continue
        meta = SCHEMA.variables[var]
        universe = ", ".join(f"{c} {lo}+" if hi is None else f"{c} {lo}-{hi}"
                             for c, (lo, hi) in SCHEMA.universe(var).items())
        codes = [("—", "Number")] if SCHEMA.is_numeric(var) else \
            sorted(((int(k), v) for k, v in meta.get("VALUES", {}).items()), key=lambda kv: (kv[0] == 0, kv[0]))
        first = r
        for code, label in codes:
            for c, text in ((4, code), (5, label)):
                ws.cell(r, c, text)
                style(ws.cell(r, c), font=Font(size=9), h="center" if c == 4 else "left")
            r += 1
        for c, text in ((1, var), (2, meta["DESCRIPTION"].strip()), (3, universe)):
            ws.cell(first, c, text)
            if r - 1 > first:
                ws.merge_cells(start_row=first, start_column=c, end_row=r - 1, end_column=c)
            for rr in range(first, r):
                style(ws.cell(rr, c), font=Font(bold=(c == 1), size=10 if c == 1 else 9))
    r += 1
for col, width in zip("ABCDE", (17, 46, 14, 8, 60)):
    ws.column_dimensions[col].width = width

# ---------------------------------------------------------------- rules
ws = wb.create_sheet("Rules")
ws["A1"] = "Survey rules"
ws["A1"].font = Font(bold=True, size=13)
ws["A2"] = ("Rules applied to the raw records before or while harmonizing (defined in surveys.json). "
            "Value mappings (raw code -> harmonized code) are in the mapping files.")
ws["A2"].font = Font(size=9)
for c, text in enumerate(("Survey", "Years", "Variable", "Rule type", "Raw variables", "Rule"), start=1):
    ws.cell(4, c, text)
    style(ws.cell(4, c), **HEAD)
for r, row in enumerate(RULES, start=5):
    for c, text in enumerate(row, start=1):
        ws.cell(r, c, text)
        style(ws.cell(r, c), font=Font(size=9, bold=(c == 1)))
for col, width in zip("ABCDEF", (11, 16, 18, 18, 24, 110)):
    ws.column_dimensions[col].width = width
ws.freeze_panes = "A5"
ws.auto_filter.ref = f"A4:F{4 + len(RULES)}"

# ---------------------------------------------------------------- mapping decisions
ws = wb.create_sheet("Mapping decisions")
ws["A1"] = "Mapping decisions"
ws["A1"].font = Font(bold=True, size=13)
ws["A2"] = ("Choices of raw variables and of harmonized categories, and the reason for each. The resulting mappings are "
            "in 'Variable mapping' and in the mapping files; the rules applied to the records are in 'Rules'.")
ws["A2"].font = Font(size=9)
for c, text in enumerate(("Survey", "Years", "Variable", "Decision", "Reason"), start=1):
    ws.cell(4, c, text)
    style(ws.cell(4, c), **HEAD)
for r, row in enumerate(DECISIONS, start=5):
    for c, text in enumerate(row, start=1):
        ws.cell(r, c, text)
        style(ws.cell(r, c), font=Font(size=9, bold=(c == 1)))
for col, width in zip("ABCDE", (11, 18, 22, 70, 70)):
    ws.column_dimensions[col].width = width
ws.freeze_panes = "A5"
ws.auto_filter.ref = f"A4:E{4 + len(DECISIONS)}"

out = d / "harmonization_dictionary.xlsx"
wb.save(out)
print("saved", out, "| mapping columns:", [(c[0], c[1]) for c in columns])
