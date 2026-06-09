"""Helpers for loading Dominican Republic ENHOGAR mapping fixtures."""

from __future__ import annotations

import json
from pathlib import Path


_DATA_DIR = Path(__file__).resolve().parent / "rd_year_mappings"


def _load_json(name: str):
    with (_DATA_DIR / name).open(encoding="utf-8") as handle:
        return json.load(handle)


def _coerce_int_keys(value):
    if isinstance(value, dict):
        coerced = {}
        for key, item in value.items():
            new_key = int(key) if isinstance(key, str) and key.isdigit() else key
            coerced[new_key] = _coerce_int_keys(item)
        return coerced
    if isinstance(value, list):
        return [_coerce_int_keys(item) for item in value]
    return value

COLUMN_MAPPING_BY_YEAR = _coerce_int_keys(_load_json("column_mapping_by_year.json"))
HARMONIZED_MAPPING = _coerce_int_keys(_load_json("harmonized_mapping.json"))


def get_value_mapping_path(year: int) -> Path:
    """Return the file path for the value mapping of a given year."""
    filename = f"enhogar{year}_mapping.json"
    return _coerce_int_keys(_load_json(filename))


def get_columns_for_year(year: int) -> list:
    """Return the original columns to select for a given year."""
    mapping = COLUMN_MAPPING_BY_YEAR.get(year, {})
    return list(mapping.keys())


def get_column_rename_map(year: int) -> dict:
    """Return the column rename map for a given year."""
    return COLUMN_MAPPING_BY_YEAR.get(year, {})


def get_harmonized_columns() -> list:
    """Infer harmonized columns from the harmonized mapping keys."""
    return list(HARMONIZED_MAPPING.keys())
