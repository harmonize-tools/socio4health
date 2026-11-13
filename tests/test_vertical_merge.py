import dask.dataframe as dd
import pandas as pd
import pytest

from socio4health import Harmonizer


def _tables():
    """Three tables of the same survey, each missing a different column, plus an unrelated one."""
    a = pd.DataFrame({"ID": ["1", "2"], "AGE": ["30", "40"], "SEX": ["1", "2"], "INCOME": ["10", "20"]})
    b = pd.DataFrame({"ID": ["3"], "AGE": ["50"], "SEX": ["1"], "REGION": ["N"]})
    c = pd.DataFrame({"ID": ["4"], "SEX": ["2"], "AGE": ["60"], "INCOME": ["30"], "REGION": ["S"]})
    other = pd.DataFrame({"CODE": ["x"], "NAME": ["y"]})
    return [dd.from_pandas(df, npartitions=1) for df in (a, b, c, other)]


def _merge(dfs, **kwargs):
    return Harmonizer().s4h_vertical_merge(dfs, overlap_threshold=0.5, **kwargs)


def test_inputs_are_not_modified():
    dfs = _tables()
    before = [list(df.columns) for df in dfs]
    _merge(dfs)
    assert [list(df.columns) for df in dfs] == before


def test_pandas_inputs_are_not_modified():
    dfs = [df.compute() for df in _tables()]
    before = [list(df.columns) for df in dfs]
    _merge(dfs)
    assert [list(df.columns) for df in dfs] == before


def test_calling_twice_gives_the_same_result():
    dfs = _tables()
    first = _merge(dfs)
    second = _merge(dfs)
    assert [list(d.columns) for d in first] == [list(d.columns) for d in second]
    for x, y in zip(first, second):
        pd.testing.assert_frame_equal(x.compute(), y.compute())


def test_union_keeps_order_of_first_appearance_and_fills_missing_with_nan():
    merged = _merge(_tables())
    assert len(merged) == 2
    out = merged[0].compute().reset_index(drop=True)
    assert list(out.columns) == ["ID", "AGE", "SEX", "INCOME", "REGION"]
    assert out["ID"].tolist() == ["1", "2", "3", "4"]
    assert out["REGION"].isna().tolist() == [True, True, False, False]
    assert out["INCOME"].isna().tolist() == [False, False, True, False]


def test_intersection_keeps_order_of_first_table():
    merged = _merge(_tables(), method="intersection")
    assert list(merged[0].columns) == ["ID", "AGE", "SEX"]


def test_invalid_method_is_rejected_even_without_groups_to_merge():
    with pytest.raises(ValueError, match="method"):
        _merge(_tables()[:1], method="unoin")
