"""
Survey definitions: where the files of each year are, how to read them, their keys, weights and rules.

Surveys are described in a JSON file (one entry per survey) and loaded with :func:`s4h_load_surveys`::

    {
      "GEIH": {
        "country": "COL", "years": [2023, 2024],
        "parts": [{"path": "GEIH/{year}"}],
        "reader": {"down_ext": [".csv"], "sep": ";"},
        "column_mapping": "col_mapping/column_mapping.json",
        "value_mapping": "col_mapping/geih_mapping.json",
        "factor": {"factor_col": "FEX_C18", "id_col": "DIRECTORIO"},
        "household_key": [["DIRECTORIO", "SECUENCIA_P"]],
        "person_key": [["ORDEN"]],
        "notes": {"factor": "CNPV 2018 projection"}
      }
    }

Data paths (``parts``) are relative to the data root; mapping files are relative to the JSON file.
"""

import dataclasses
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Union

from socio4health.utils.harmonizer_utils import mapping_source_columns


class NoDataError(Exception):
    """A survey year has no files, no mapping or no table with keys, weights and harmonized variables."""


def _int_keys(obj):
    if isinstance(obj, dict):
        return {(int(k) if isinstance(k, str) and k.isdigit() else k): _int_keys(v) for k, v in obj.items()}
    return obj


@dataclass
class Survey:
    """
    One survey. Every rule is optional except the files, mappings and keys.

    Attributes
    ----------
    name, country : str
        Survey and country codes (e.g. ``"GEIH"``, ``"COL"``).
    years : list of int
        Years to process.
    parts : list of dict
        Folders of each year, relative to ``data_root`` (``{"path": "PNAD/dom/{year}"}``). Fixed-width files add
        ``"ibge_dictionary"`` (glob of the IBGE dictionary of the year) and ``"shared_dictionary"`` (glob of a
        dictionary covering several years, named ``..._2012_a_2014_...``).
    reader : dict
        :class:`~socio4health.Extractor` arguments (``down_ext``, ``sep``...).
    column_mapping : str
        JSON ``{"<year>" or "<first>-<last>": {raw column: harmonized name}}``.
    value_mapping : str
        JSON ``{raw column: {raw code: harmonized code}}``, optionally inside ``"mapping"``; ``{year}`` in the
        name gives one file per year.
    household_key, person_key : list of list of str
        Alternative raw keys, the first one a table has is used. ``HOUSEHOLD_ID`` joins the household key with
        ``-``; ``PERSON_ID`` adds the person key (or the order within the household).
    group_col : str
        Unit column used for the shares (``ADMIN_DIVISION`` or ``ADMIN_DIVISION_2``).
    factor, person_factor : dict, optional
        Weights: ``{"factor_col", "id_col", "fallback_col"}``. ``person_factor`` weights person tables with a
        population factor (see :func:`~socio4health.utils.harmonizer_utils.use_person_factor`).
    file_groups : list of dict
        Read the files in groups: ``{"pattern": regex on the file name, "assign": {column: value}}``.
    unify_columns : list of str
        Columns spelled differently in some files (BOM, case): the variant replaces the exact name.
    periods_per_year : int
        Samples per year expanded to the whole population (12 for monthly samples): weights are divided by it.
    vertical_merge : bool
        Stack the tables with nearly the same columns before harmonizing.
    keep_rows : dict
        ``{column: [codes]}``: only rows with these codes.
    keep_rows_any : list of dict
        Rows meeting every condition of at least one rule; applied to tables that have every column.
    fill_missing : dict
        ``{column: {"from": column, "values": [codes]}}``: empty values take these codes of another column.
    derive_from_flags : dict
        ``{column: {flag column: code}}``: code of the only yes/no flag that is yes
        (:func:`~socio4health.utils.harmonizer_utils.s4h_code_from_flags`).
    map_columns : list of dict
        ``{"years", "source", "target", "mapping"}``: fill ``target`` from ``source`` with a JSON mapping when a
        table has the source but not the target (e.g. regions from provinces).
    row_ids_without_key : bool
        Tables without any key get row IDs; their persons are not linked to households.
    relative_weight_years : dict
        ``{"households": [years], "persons": [years]}`` whose weights are normalized (average 1).
    notes : dict
        Free explanations of the rules; not used by the pipeline.
    """
    name: str
    country: str
    years: List[int]
    parts: List[dict]
    reader: dict
    column_mapping: str
    value_mapping: str
    household_key: List[List[str]]
    person_key: List[List[str]]
    group_col: str = "ADMIN_DIVISION"
    factor: Optional[dict] = None
    person_factor: Optional[dict] = None
    file_groups: List[dict] = field(default_factory=lambda: [{}])
    unify_columns: List[str] = field(default_factory=list)
    periods_per_year: int = 1
    vertical_merge: bool = True
    keep_rows: Dict[str, list] = field(default_factory=dict)
    keep_rows_any: List[dict] = field(default_factory=list)
    fill_missing: Dict[str, dict] = field(default_factory=dict)
    derive_from_flags: Dict[str, dict] = field(default_factory=dict)
    map_columns: List[dict] = field(default_factory=list)
    row_ids_without_key: bool = False
    relative_weight_years: Dict[str, List[int]] = field(default_factory=dict)
    notes: dict = field(default_factory=dict)
    data_root: Path = Path(".")
    mappings_dir: Path = Path(".")

    def read_json(self, path: str):
        """Read a mapping file (relative to ``mappings_dir``)."""
        return json.loads((Path(self.mappings_dir) / path).read_text(encoding="utf-8"))

    def column_map(self, year: int) -> dict:
        """``{raw column: harmonized name}`` of the block (year or period) that contains ``year``."""
        for key, mapping in self.read_json(self.column_mapping).items():
            start, _, end = key.partition("-")
            if int(start) <= year <= int(end or start):
                return mapping
        raise NoDataError(f"No column mapping for {year}")

    def value_map(self, year: int) -> dict:
        """``{raw column: {code: harmonized code}}`` for ``year``."""
        data = self.read_json(self.value_mapping.format(year=year))
        return _int_keys(data.get("mapping", data))

    def raw_columns(self, year: int) -> List[str]:
        """Raw columns the pipeline reads: mapped variables, keys, weights and the columns of the rules."""
        cols = {c for alt in self.household_key + self.person_key for c in alt}
        for f in (self.factor, self.person_factor):
            if f:
                cols |= {f["factor_col"], *([f["id_col"]] if isinstance(f["id_col"], str) else f["id_col"])}
                cols |= {f["fallback_col"]} if f.get("fallback_col") else set()
        cols |= {c for rule in self.keep_rows_any for c in rule} | set(self.keep_rows)
        cols |= {c for target, rule in self.fill_missing.items() for c in (target, rule["from"])}
        cols |= {c for flags in self.derive_from_flags.values() for c in flags}
        return [*mapping_source_columns(self.column_map(year)), *sorted(cols)]

    def ibge_dictionary(self, part: dict, year: int) -> Path:
        """IBGE dictionary of a fixed-width part: the one of the year, else a shared one covering it."""
        found = sorted(Path(self.data_root).glob(part["ibge_dictionary"].format(year=year)))
        if not found and part.get("shared_dictionary"):
            found = [f for f in sorted(Path(self.data_root).glob(part["shared_dictionary"]))
                     if any(int(a) <= year <= int(b) for a, b in re.findall(r"(\d{4})_a_(\d{4})", f.name))]
        if not found:
            raise NoDataError(f"No IBGE dictionary {part['ibge_dictionary'].format(year=year)}")
        return found[0]

    def weight_type(self, level: str, year: int) -> str:
        """``"relative"`` when the table only has normalized weights that year, else ``"expansion"``."""
        return "relative" if year in self.relative_weight_years.get(level, []) else "expansion"

    def input_files(self) -> List[Path]:
        """Mapping files and IBGE dictionaries the results depend on."""
        mappings = Path(self.mappings_dir)
        files = [mappings / self.column_mapping, *sorted(mappings.glob(self.value_mapping.replace("{year}", "*")))]
        files += [mappings / rule["mapping"] for rule in self.map_columns]
        for part in self.parts:
            for pattern in (part.get("ibge_dictionary"), part.get("shared_dictionary")):
                if pattern:
                    files += sorted(Path(self.data_root).glob(pattern.replace("{year}", "*")))
        return files

    def rules(self) -> dict:
        """The definition without paths or notes (what the results depend on)."""
        return {k: v for k, v in dataclasses.asdict(self).items() if k not in ("data_root", "mappings_dir", "notes")}


def s4h_load_surveys(path: Union[str, Path], data_root: Union[str, Path]) -> Dict[str, Survey]:
    """
    Read survey definitions from a JSON file.

    Parameters
    ----------
    path : str or Path
        JSON ``{name: {attribute: value}}`` (see :class:`Survey`). Mapping files are relative to its folder.
    data_root : str or Path
        Folder the ``parts`` paths are relative to.

    Returns
    -------
    dict of str to Survey

    Raises
    ------
    TypeError
        If an entry has an unknown attribute (e.g. a typo in a rule name).
    """
    path = Path(path)
    entries = json.loads(path.read_text(encoding="utf-8"))
    return {name: Survey(name=name, data_root=Path(data_root), mappings_dir=path.parent, **entry)
            for name, entry in entries.items()}
