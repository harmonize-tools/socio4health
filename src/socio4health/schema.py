"""
Harmonized variables: the table each one belongs to, its type, the label of each code and the codebook.

A schema is read from a JSON mapping such as::

    {
      "AREA":   {"DESCRIPTION": "Housing area type", "LEVEL": ["households", "persons"],
                 "VALUES": {"1": "Urban", "2": "Rural", "0": "Missing"}},
      "H_TYPE": {"DESCRIPTION": "Type of housing", "VALUES": {"1": "House", "0": "Missing"}},
      "P_AGE":  {"DESCRIPTION": "Age in years", "TYPE": "numeric", "UNIT": "years"}
    }

``LEVEL`` (a table name or a list of them) is optional: names starting with ``H_`` belong to ``households``
and names starting with ``P_`` to ``persons``. ``TYPE`` is ``"numeric"`` for numbers (age); every other
variable is categorical and its ``VALUES`` map each code to its label. ``UNIVERSE`` (optional) limits the
records a variable is summarized on, as ``{column: [min, max]}`` with ``null`` for an open end, e.g.
``"UNIVERSE": {"P_AGE": [25, null]}`` for people aged 25 or over.
"""

import json
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Tuple, Union

import pandas as pd

PREFIX_LEVELS = {"H_": "households", "P_": "persons"}

# Columns the record tables carry besides the harmonized variables: (tables, description)
RECORD_COLUMNS: Dict[str, Tuple[str, str]] = {
    "HOUSEHOLD_ID": ("households, persons", "Household identifier (survey keys joined with '-'); unique per survey and year"),
    "PERSON_ID": ("persons", "Person identifier (household ID plus the person's line number)"),
    "WEIGHT": ("households, persons", "Expansion factor: number of households (households table) or persons "
                                      "(persons table) the record represents in its year"),
    "WEIGHT_TYPE": ("households, persons", "'expansion': WEIGHT adds up to the population; 'relative': the survey "
                                           "only published normalized weights (average 1), valid for shares but not for totals"),
    "ADMIN_DIVISION": ("households, persons", "First-level administrative unit code"),
    "ADMIN_DIVISION_2": ("households, persons", "Second-level administrative unit code, where available"),
    "ADMIN_LEVEL": ("shares", "Unit column used in `shares` for this survey"),
}


class Schema:
    """
    Harmonized variables read from a JSON mapping (see the module documentation for its format).

    Parameters
    ----------
    variables : dict
        ``{name: {"DESCRIPTION": ..., "VALUES": {...}, "LEVEL": ..., "TYPE": ..., "UNIT": ...}}``.

    Raises
    ------
    ValueError
        If a variable has no ``LEVEL`` and its name does not start with ``H_`` or ``P_``.

    Examples
    --------
    >>> schema = Schema.from_json("harmonized_mapping.json")
    >>> schema.variables_of("households")        # ['AREA', 'H_TYPE', ...]
    >>> schema.label("H_TYPE", 1)                # 'House'
    >>> schema.codebook()                        # one row per code and per record column
    """

    def __init__(self, variables: Mapping[str, dict]):
        self.variables = dict(variables)
        self._levels = {name: self._level_of(name) for name in self.variables}

    @classmethod
    def from_json(cls, path: Union[str, Path]) -> "Schema":
        """Read the schema from a JSON file."""
        return cls(json.loads(Path(path).read_text(encoding="utf-8")))

    def _level_of(self, name: str) -> List[str]:
        level = self.variables[name].get("LEVEL")
        if level is None:
            level = next((lvl for prefix, lvl in PREFIX_LEVELS.items() if name.startswith(prefix)), None)
        if level is None:
            raise ValueError(f"Variable {name!r} needs a LEVEL: its name does not start with {list(PREFIX_LEVELS)}")
        return [level] if isinstance(level, str) else list(level)

    def levels(self, variable: str) -> List[str]:
        """Tables (``households``, ``persons``...) the variable belongs to."""
        return list(self._levels[variable])

    def variables_of(self, level: str) -> List[str]:
        """Variables of a table, in the order of the mapping."""
        return [v for v in self.variables if level in self._levels[v]]

    def is_numeric(self, variable: str) -> bool:
        """Whether the variable is a number (e.g. age) rather than a set of codes."""
        return self.variables[variable].get("TYPE") == "numeric"

    def universe(self, variable: str) -> Dict[str, list]:
        """``{column: [min, max]}`` limits of the records the variable is summarized on (empty: all)."""
        return dict(self.variables[variable].get("UNIVERSE", {}))

    def labels(self, variable: str) -> Dict[int, str]:
        """``{code: label}`` of a categorical variable, sorted by code (empty for numeric variables)."""
        if self.is_numeric(variable):
            return {}
        return dict(sorted((int(k), v) for k, v in self.variables[variable].get("VALUES", {}).items()))

    def label(self, variable: str, code) -> str:
        """Label of a code, or ``"code <code>"`` when the mapping does not have it."""
        return self.labels(variable).get(int(code), f"code {code}")

    def valid_codes(self, variable: str) -> set:
        """Codes defined for a categorical variable."""
        return set(self.labels(variable))

    def codebook(self, columns: Optional[Mapping[str, Tuple[str, str]]] = None) -> pd.DataFrame:
        """
        Codebook: one row per code of each categorical variable, one row per numeric variable and one row
        per record column.

        Parameters
        ----------
        columns : dict, optional
            ``{column: (tables, description)}`` for the columns that are not harmonized variables. Defaults to
            :data:`RECORD_COLUMNS`; pass ``{**RECORD_COLUMNS, "ADMIN_DIVISION": (...)}`` to adapt descriptions.

        Returns
        -------
        pandas.DataFrame
            Columns ``variable``, ``table``, ``code`` (nullable integer), ``label`` and ``description``.
        """
        rows = []
        for v, entry in self.variables.items():
            base = {"variable": v, "table": ", ".join(self._levels[v]), "description": entry.get("DESCRIPTION", "").strip()}
            if self.is_numeric(v):
                unit = entry.get("UNIT")
                rows.append({**base, "code": None, "label": f"number ({unit})" if unit else "number"})
            else:
                rows += [{**base, "code": code, "label": lab} for code, lab in self.labels(v).items()]
        for c, (table, description) in (RECORD_COLUMNS if columns is None else columns).items():
            rows.append({"variable": c, "table": table, "code": None, "label": None, "description": description})
        out = pd.DataFrame(rows, columns=["variable", "table", "code", "label", "description"])
        out["code"] = out["code"].astype("Int16")
        return out
