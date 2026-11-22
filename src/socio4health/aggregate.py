"""
Summaries of harmonized record tables: weighted shares per unit and data checks.

Record tables have one row per household or person, a weight column and the harmonized variables of a
:class:`~socio4health.Schema` (codes for categorical variables, numbers for numeric ones).
"""

import re
from typing import Dict, Mapping, Optional, Sequence, Tuple, Union

import pandas as pd

from socio4health.schema import Schema

TOTALS = {"persons": "POPULATION", "households": "HOUSEHOLDS"}


def _slug(text) -> str:
    return re.sub(r"\W+", "_", str(text)).strip("_")


def _in_universe(df: pd.DataFrame, universe: Mapping[str, list]) -> pd.Series:
    keep = pd.Series(True, index=df.index)
    for column, (low, high) in universe.items():
        values = df[column]
        if low is not None:
            keep &= (values >= low).fillna(False).astype(bool)
        if high is not None:
            keep &= (values <= high).fillna(False).astype(bool)
    return keep


def s4h_shares(tables: Mapping[str, Optional[pd.DataFrame]], schema: Schema, by: Sequence[str],
               weight: str = "WEIGHT", totals: Optional[Mapping[str, str]] = None) -> pd.DataFrame:
    """
    Weighted share of each category of every harmonized variable, per unit.

    Parameters
    ----------
    tables : dict
        Record tables by level, e.g. ``{"households": households, "persons": persons}``. Missing levels
        (``None``) are skipped.
    schema : Schema
        Harmonized variables. Each variable is summarized on the first table of its ``LEVEL`` (``AREA`` in
        both tables is summarized on households), only on the records of its ``UNIVERSE``.
    by : list of str
        Columns that identify a unit (e.g. ``["YEAR", "ADMIN_DIVISION"]``). Records without them or without a
        weight are left out.
    weight : str
        Weight column (default ``"WEIGHT"``).
    totals : dict, optional
        Column with the sum of the weights of each level (default ``{"persons": "POPULATION",
        "households": "HOUSEHOLDS"}``). A total is left empty for units with a ``WEIGHT_TYPE`` of
        ``"relative"``: normalized weights do not add up to the population.

    Returns
    -------
    pandas.DataFrame
        One row per unit: the ``by`` columns, the totals, ``<variable>_<label>`` shares (the categories of a
        variable, including *Missing*, add up to 1) and ``<variable>_mean`` for numeric variables.

    Examples
    --------
    >>> shares = s4h_shares({"households": h, "persons": p}, Schema.from_json("harmonized_mapping.json"),
    ...                     by=["YEAR", "ADMIN_DIVISION"])
    """
    by = list(by)
    totals = TOTALS if totals is None else totals
    tables = {level: df.dropna(subset=[*by, weight]) for level, df in tables.items() if df is not None}

    parts = []
    for level, name in totals.items():
        df = tables.get(level)
        if df is None:
            continue
        total = df.groupby(by)[weight].sum().rename(name)
        if "WEIGHT_TYPE" in df.columns:
            total = total.mask(df["WEIGHT_TYPE"].eq("relative").groupby([df[k] for k in by]).any())
        parts.append(total)

    for v in schema.variables:
        df = tables.get(schema.levels(v)[0])
        if df is None or v not in df.columns or df[v].isna().all():
            continue
        d = df.dropna(subset=[v])
        if schema.is_numeric(v):
            parts.append((d[v] * d[weight]).groupby([d[k] for k in by]).sum()
                         .div(d.groupby(by)[weight].sum()).rename(f"{v}_mean"))
            continue
        universe = schema.universe(v)
        if universe:
            d = d[_in_universe(d, universe)]
        t = d.groupby(by + [v])[weight].sum().unstack(v)
        t = t.div(t.sum(axis=1), axis=0)
        t.columns = [f"{v}_{_slug(schema.label(v, c))}" for c in t.columns]
        parts.append(t)

    out = pd.concat(parts, axis=1).reset_index() if parts else pd.DataFrame(columns=by)
    for name in totals.values():
        if name not in out.columns:
            out[name] = pd.NA
    return out


def s4h_check_records(df: pd.DataFrame, schema: Schema, ids: Union[str, Sequence[str]],
                      weight: str = "WEIGHT") -> Tuple[pd.DataFrame, Dict]:
    """
    Check a record table and set the codes that are not in the schema to 0 (*Missing*).

    Parameters
    ----------
    df : pandas.DataFrame
        Record table.
    schema : Schema
        Harmonized variables; their ``VALUES`` are the valid codes.
    ids : str or list of str
        Columns that identify a record (e.g. ``"PERSON_ID"``, or ``["YEAR", "PERSON_ID"]`` for several years).
    weight : str
        Weight column (default ``"WEIGHT"``).

    Returns
    -------
    tuple of (pandas.DataFrame, dict)
        The table with invalid codes set to 0, and ``{"records", "duplicate IDs", "without weight",
        "codes not in codebook"}``: number of rows, IDs repeated, share of rows without weight and
        ``{variable: number of invalid codes}``.
    """
    ids = [ids] if isinstance(ids, str) else list(ids)
    invalid_counts = {}
    for v in schema.variables:
        if v in df.columns and not schema.is_numeric(v):
            invalid = df[v].notna() & ~df[v].isin(schema.valid_codes(v))
            if invalid.any():
                invalid_counts[v] = int(invalid.sum())
                df = df.assign(**{v: df[v].mask(invalid, 0)})
    summary = {"records": len(df),
               "duplicate IDs": int(df[ids].dropna().duplicated().sum()),
               "without weight": float(df[weight].isna().mean()) if len(df) else 0.0,
               "codes not in codebook": invalid_counts}
    return df, summary
