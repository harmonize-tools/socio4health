"""Readers of the official documentation of the data paper surveys.

Each reader returns ``{VARIABLE: Doc}`` for one survey year, where ``Doc`` holds the question, the code -> label
pairs and, for fixed-width files, the position of the variable. Used by ``build_raw_dictionaries.py`` and the
audit scripts in ``audit/``.

Sources (under the data root):

- PNAD / PNADC: the IBGE dictionary of each year (``.xls``), as listed in ``surveys.json``.
- ENAHO: the variable and value labels of the ``.sav`` files of each year.
- ENHOGAR: the ``.htm`` codebooks of each year; variables they lack (no codebook in 2017, some modules missing) come
  from the compiled dictionary ``ENHOGAR/raw_enhogar_dict.xlsx`` when it is there.
- GEIH: for the 2005 frame (2007-2020), the DDI codebook of each year from the DANE microdata catalog
  (``GEIH/ddi/GEIH_<year>.xml``, see ``GEIH_DDI``); for the 2018 frame (2021+), the DANE data dictionary
  ``GEIH/DICCIONARIO_DATOS_GEIH_2023 (3).xlsx``.
"""
import glob
import html
import re
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd


@dataclass
class Doc:
    question: str = ""
    codes: "OrderedDict[str, str]" = field(default_factory=OrderedDict)
    start: int = None   # 1-based position in fixed-width files
    size: int = None
    source: str = ""


def norm(code):
    """Codes as text without decimals or leading zeros: '01', '1.0' and 1 are '1'."""
    code = str(code).strip()
    return str(int(float(code))) if re.fullmatch(r"-?\d+(\.0+)?", code) else code


def codes_only(docs):
    """{VARIABLE: {code: label}} from {VARIABLE: Doc}."""
    return {k: v.codes for k, v in docs.items()}


# ---------------------------------------------------------------- IBGE (PNAD, PNADC)
_CODE = re.compile(r"^[A-Z][A-Z0-9_]*\d[A-Z0-9_]*$|^UF$|^Capital$|^Estrato$|^UPA$", re.I)


def read_ibge(path):
    """{VARIABLE: Doc} from an IBGE dictionary: position (column 0), size (1), variable (2), question (4), code (5)
    and label (6)."""
    df = pd.read_excel(path, header=None, dtype=str)
    out, current = {}, None
    for row in df.itertuples(index=False):
        cells = ["" if pd.isna(c) else str(c).strip() for c in row]
        cells += [""] * (8 - len(cells))
        if cells[2] and _CODE.match(cells[2]) and cells[0]:
            current = cells[2].upper()
            start = int(float(cells[0])) if re.fullmatch(r"\d+(\.0+)?", cells[0]) else None
            size = int(float(cells[1])) if re.fullmatch(r"\d+(\.0+)?", cells[1]) else None
            out[current] = Doc(question=re.sub(r"\s+", " ", cells[4]), start=start, size=size, source=Path(path).name)
        elif cells[0] and not cells[1]:   # section header ("Parte 3 - ...")
            current = None
        if current and cells[5] and re.fullmatch(r"\d+", cells[5]):
            out[current].codes[norm(cells[5])] = cells[6]
    return out


def ibge_docs(survey, year):
    """Docs of every part (households / persons file) of a PNAD or PNADC year."""
    merged = {}
    for part in survey.parts:
        if "ibge_dictionary" in part:
            for k, v in read_ibge(survey.ibge_dictionary(part, year)).items():
                merged.setdefault(k, v)
    return merged


# ---------------------------------------------------------------- ENAHO
def enaho_docs(root, year):
    import pyreadstat
    out = {}
    for f in sorted(glob.glob(str(Path(root) / f"ENAHO/{year}/*/*.sav"))):
        try:
            _, meta = pyreadstat.read_sav(f, metadataonly=True)
        except Exception:  # noqa: BLE001
            continue
        for col in meta.column_names:
            if col.upper() in out:
                continue
            labels = meta.variable_value_labels.get(col, {})
            out[col.upper()] = Doc(question=str(meta.column_names_to_labels.get(col) or ""),
                                   codes=OrderedDict((norm(k), str(v)) for k, v in labels.items()),
                                   source=Path(f).name)
    return out


# ---------------------------------------------------------------- ENHOGAR
def read_htm(path):
    """{VARIABLE: Doc} from an SPSS codebook exported to .htm (name, 'Valor', 'Atributos estándar', 'Etiqueta',
    question, then 'Valores válidos' / 'Valores perdidos' with code and label pairs)."""
    raw = Path(path).read_bytes()
    text = raw.decode("utf-8", errors="replace")
    if text.count("�") > 20:
        text = raw.decode("cp1252", errors="replace")
    tokens = [html.unescape(t).strip() for t in re.split(r"<[^>]+>", text)]
    tokens = [t for t in tokens if t and t != "\xa0"]
    out, i = {}, 0
    while i < len(tokens) - 2:
        if tokens[i + 1] == "Valor" and tokens[i + 2].startswith("Atributos"):
            # the cell may hold just the name or the name and a table caption ("V108 V108, tabla, ...")
            var = tokens[i].split()[0].rstrip(",").upper()
            question = tokens[i + 4] if i + 4 < len(tokens) and tokens[i + 3] == "Etiqueta" else ""
            doc, j, mode = Doc(question=question, source=Path(path).name), i + 3, None
            while j < len(tokens) - 2 and not (tokens[j + 1] == "Valor" and tokens[j + 2].startswith("Atributos")):
                t = tokens[j]
                if t.startswith("Valores"):
                    mode = t
                elif mode and re.fullmatch(r"-?\d+(\.\d+)?", t) and j + 1 < len(tokens):
                    doc.codes[norm(t)] = tokens[j + 1] + (" [perdido]" if "perdidos" in mode else "")
                    j += 1
                j += 1
            out.setdefault(var, doc)
            i = j
        else:
            i += 1
    return out


def read_compiled_enhogar(path, year):
    """{VARIABLE: Doc} from the hand-compiled ENHOGAR dictionary (sheets hog<year> and per<year>)."""
    out = {}
    if not Path(path).exists():
        return out
    book = pd.ExcelFile(path)
    for sheet in (s for s in book.sheet_names if s.split()[0] in (f"hog{year}", f"per{year}")):
        var = None
        df = book.parse(sheet, dtype=str)
        df.columns = ["variable_name", *df.columns[1:]]   # the first header is blank in some sheets (per2021)
        for r in df.itertuples(index=False):
            if isinstance(r.variable_name, str):
                var = r.variable_name.upper()
                out.setdefault(var, Doc(question=str(r.question) if isinstance(r.question, str) else "",
                                        source=f"{Path(path).name} [{sheet}]"))
            elif var and isinstance(r.value, str):
                out[var].codes[norm(r.value)] = str(r.description)
    return out


def enhogar_docs(root, year):
    out = {}
    for f in sorted(glob.glob(str(Path(root) / f"ENHOGAR/{year}/*.htm"))):
        for k, v in read_htm(f).items():
            out.setdefault(k, v)
    for k, v in read_compiled_enhogar(Path(root) / "ENHOGAR/raw_enhogar_dict.xlsx", year).items():
        if k not in out or not out[k].codes:
            out[k] = v
    return out


# ---------------------------------------------------------------- GEIH
# DANE microdata catalog studies of the 2005 frame; the DDI of each one is exported at
# https://microdatos.dane.gov.co/index.php/metadata/export/<id>/ddi. 2013 (study 68) has no DDI export: the 2012 one
# is used (same questionnaire).
GEIH_DDI = {2007: 317, 2008: 206, 2009: 207, 2010: 205, 2011: 182, 2012: 77, 2014: 328, 2015: 356, 2016: 427,
            2017: 458, 2018: 547, 2019: 599, 2020: 780}
_CACHE = {}


def _text(el):
    return re.sub(r"\s+", " ", el.text or "").strip() if el is not None else ""


def read_ddi(path):
    """{VARIABLE: Doc} from a DDI 1.2 codebook (var name, labl, catgry/catValu, catgry/labl). A variable repeated in
    several data files keeps its first entry with categories."""
    import xml.etree.ElementTree as ET
    out = {}
    for _, el in ET.iterparse(path):
        if el.tag.rsplit("}", 1)[-1] != "var":
            continue
        name = (el.get("name") or "").strip().upper()
        codes = OrderedDict()
        for cat in el.findall("{*}catgry"):
            code = _text(cat.find("{*}catValu"))
            if re.fullmatch(r"-?\d+(\.0+)?", code):
                codes[norm(code)] = _text(cat.find("{*}labl"))
        codes = OrderedDict(sorted(codes.items(), key=lambda kv: int(kv[0])))
        if name and (name not in out or codes and not out[name].codes):
            out[name] = Doc(question=_text(el.find("{*}labl")), codes=codes, source=Path(path).name)
        el.clear()
    return out


def geih_docs(root, year):
    """DDI of the year (2007-2020) or DANE data dictionary of the 2018 frame (2021+)."""
    if year < 2021:
        ddi = year if year in GEIH_DDI else max(y for y in GEIH_DDI if y < year)
        path = Path(root) / f"GEIH/ddi/GEIH_{ddi}.xml"
        if not path.exists():
            return {}
        if path not in _CACHE:
            _CACHE[path] = read_ddi(path)
        if ddi == year:
            return _CACHE[path]
        return {k: Doc(v.question, v.codes, v.start, v.size, f"{v.source} (no DDI for {year})")
                for k, v in _CACHE[path].items()}
    path = Path(root) / "GEIH/DICCIONARIO_DATOS_GEIH_2023 (3).xlsx"
    if path not in _CACHE:
        out = _CACHE[path] = {}
        df = pd.read_excel(path, sheet_name="Diccionario de datos", header=None, dtype=str)
        for r in df.itertuples(index=False):
            var = r[6].strip().upper() if isinstance(r[6], str) else None
            if not var or not re.fullmatch(r"[A-Z][A-Z0-9_]*", var):
                continue
            doc = out.setdefault(var, Doc(question=re.sub(r"^\d+\.\s*", "", str(r[7] or "")).strip(),
                                          source=path.name))
            code, label = r[10], r[11]
            if isinstance(code, str) and re.fullmatch(r"\s*-?\d+\s*", code):
                doc.codes[norm(code)] = str(label).strip()
    return _CACHE[path]


def survey_docs(survey, root, year):
    """Docs of one survey year, whatever the survey."""
    if survey.name in ("PNAD", "PNADC"):
        return ibge_docs(survey, year)
    return {"ENAHO": enaho_docs, "ENHOGAR": enhogar_docs, "GEIH": geih_docs}[survey.name](root, year)
