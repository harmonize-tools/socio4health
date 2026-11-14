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


def test_overlap_basis_larger_keeps_other_modules_apart():
    # A small module made mostly of household keys must not be stacked into the person table
    persons = pd.DataFrame({"ID": ["1"], "ORDEN": ["1"], "DPTO": ["5"], "FEX": ["1"], "AGE": ["30"], "SEX": ["1"],
                            "EDU": ["2"], "ETHNIC": ["1"]})
    keys_module = pd.DataFrame({"ID": ["1"], "ORDEN": ["1"], "DPTO": ["5"], "FEX": ["1"], "MIGRATED": ["2"]})
    dfs = [dd.from_pandas(df, npartitions=1) for df in (persons, keys_module)]
    assert len(Harmonizer().s4h_vertical_merge(dfs, overlap_threshold=0.8)) == 1            # smaller: merged
    assert len(Harmonizer().s4h_vertical_merge(dfs, overlap_threshold=0.8, overlap_basis="larger")) == 2


def test_overlap_basis_larger_still_merges_same_table_with_extra_columns():
    month1 = pd.DataFrame({c: ["1"] for c in "ABCDEFGHIJ"})
    month2 = pd.DataFrame({c: ["2"] for c in "ABCDEFGHI"})      # one column fewer: 9/10 = 0.9
    dfs = [dd.from_pandas(df, npartitions=1) for df in (month1, month2)]
    out = Harmonizer().s4h_vertical_merge(dfs, overlap_threshold=0.8, overlap_basis="larger")
    assert len(out) == 1 and len(out[0]) == 2


def test_invalid_overlap_basis():
    with pytest.raises(ValueError):
        Harmonizer().s4h_vertical_merge(_tables(), overlap_basis="median")


def test_columns_that_differ_only_in_case_are_stacked_together():
    # GEIH 2007: some months spell the key "Secuencia_p", others "SECUENCIA_P"
    a = pd.DataFrame({"DIRECTORIO": ["1"], "SECUENCIA_P": ["1"], "ORDEN": ["1"], "P6040": ["30"]})
    b = pd.DataFrame({"DIRECTORIO": ["2"], "Secuencia_p": ["1"], "Orden": ["2"], "P6040": ["40"]})
    out = Harmonizer().s4h_vertical_merge([dd.from_pandas(a, npartitions=1), dd.from_pandas(b, npartitions=1)],
                                          overlap_threshold=0.8, overlap_basis="larger")
    assert len(out) == 1
    merged = out[0].compute()
    assert list(merged.columns) == ["DIRECTORIO", "SECUENCIA_P", "ORDEN", "P6040"]
    assert merged["SECUENCIA_P"].tolist() == ["1", "1"] and merged["ORDEN"].tolist() == ["1", "2"]
