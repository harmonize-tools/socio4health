import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from socio4health import Harmonizer


def _table():
    """Four rows: EMPTY is all NaN, HALF is 50% NaN, QUARTER is 25% NaN, FULL has no NaN."""
    return dd.from_pandas(pd.DataFrame({
        "full": [1, 2, 3, 4],
        "quarter": [1, 2, 3, np.nan],
        " Half ": [1, 2, np.nan, np.nan],
        "empty": [np.nan] * 4,
    }), npartitions=2)


def test_drops_columns_above_the_threshold():
    out = Harmonizer(nan_threshold=0.5).s4h_drop_nan_columns(_table())
    assert isinstance(out, dd.DataFrame)                 # still lazy
    assert list(out.columns) == ["FULL", "QUARTER", "HALF"]   # names are upper-cased and stripped
    assert out.compute()["HALF"].isna().sum() == 2       # exactly at the threshold: kept


def test_lower_threshold_drops_more():
    out = Harmonizer(nan_threshold=0.2).s4h_drop_nan_columns(_table())
    assert list(out.columns) == ["FULL"]


def test_default_threshold_drops_only_empty_columns():
    out = Harmonizer().s4h_drop_nan_columns(_table())
    assert list(out.columns) == ["FULL", "QUARTER", "HALF"]


def test_empty_columns_are_dropped_whatever_the_threshold():
    out = Harmonizer(nan_threshold=1).s4h_drop_nan_columns(_table())
    assert "EMPTY" not in out.columns and "HALF" in out.columns


def test_list_in_list_out():
    other = dd.from_pandas(pd.DataFrame({"a": [1, np.nan], "b": [np.nan, np.nan]}), npartitions=1)
    out = Harmonizer(nan_threshold=0.5).s4h_drop_nan_columns([_table(), other])
    assert isinstance(out, list) and [list(d.columns) for d in out] == [["FULL", "QUARTER", "HALF"], ["A"]]


def test_sample_frac_estimates_on_a_sample():
    out = Harmonizer(nan_threshold=0.5, sample_frac=1.0).s4h_drop_nan_columns(_table())
    assert list(out.columns) == ["FULL", "QUARTER", "HALF"]


@pytest.mark.parametrize("kwargs", [{"nan_threshold": 1.5}, {"nan_threshold": -0.1}, {"sample_frac": 0}, {"sample_frac": 2}])
def test_invalid_parameters_are_rejected(kwargs):
    with pytest.raises(ValueError):
        Harmonizer(**kwargs)
