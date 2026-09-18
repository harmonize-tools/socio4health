from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _coerce_int_keys(value: Any):
    if isinstance(value, dict):
        coerced = {}
        for key, item in value.items():
            new_key = int(key) if isinstance(key, str) and key.isdigit() else key
            coerced[new_key] = _coerce_int_keys(item)
        return coerced
    if isinstance(value, list):
        return [_coerce_int_keys(item) for item in value]
    return value


def load_json_mapping(data_dir: Path | str, filename: str):
    data_path = Path(data_dir)
    with (data_path / filename).open(encoding="utf-8") as handle:
        return json.load(handle)


def load_int_key_mapping(data_dir: Path | str, filename: str):
    return _coerce_int_keys(load_json_mapping(data_dir, filename))


def _resolve_mapping_path(base_dir: Path, filename_or_path: str | Path) -> Path:
    candidate = Path(filename_or_path)
    if candidate.is_absolute():
        return candidate

    direct = base_dir / candidate
    if direct.exists():
        return direct

    parent = base_dir.parent / candidate
    if parent.exists():
        return parent

    return direct


def build_value_mapping_filename(year: int, prefix: str = "enhogar", suffix: str = "_mapping.json") -> str:
    return f"{prefix}{year}{suffix}"


def load_mapping_bundle(
    data_dir: Path | str,
    *,
    column_mapping_file: str = "column_mapping_by_year.json",
    harmonized_mapping_file: str = "harmonized_mapping.json",
    value_mapping_prefix: str = "enhogar",
    value_mapping_suffix: str = "_mapping.json",
    years: list[int] | None = None,
):
    data_path = Path(data_dir)
    column_mapping_by_year = load_int_key_mapping(data_path, column_mapping_file)
    harmonized_mapping_path = _resolve_mapping_path(data_path, harmonized_mapping_file)
    harmonized_mapping = _coerce_int_keys(load_json_mapping(harmonized_mapping_path.parent, harmonized_mapping_path.name))

    if years is None:
        years = list(column_mapping_by_year.keys())

    value_mapping_by_year = {}
    for year in years:
        filename = build_value_mapping_filename(year, prefix=value_mapping_prefix, suffix=value_mapping_suffix)
        value_mapping_by_year[year] = load_int_key_mapping(data_path, filename)

    return {
        "column_mapping_by_year": column_mapping_by_year,
        "harmonized_mapping": harmonized_mapping,
        "value_mapping_by_year": value_mapping_by_year,
    }


def get_columns_for_year(column_mapping_by_year: dict, year: int) -> list:
    mapping = column_mapping_by_year.get(year, {})
    return list(mapping.keys())


def get_column_rename_map(column_mapping_by_year: dict, year: int) -> dict:
    return column_mapping_by_year.get(year, {})


def get_harmonized_columns(harmonized_mapping: dict) -> list:
    return list(harmonized_mapping.keys())


def get_value_mapping(value_mapping_by_year: dict, year: int) -> dict:
    return value_mapping_by_year.get(year, {})
