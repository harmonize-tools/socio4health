import dask.dataframe as dd
import pandas as pd

from socio4health.utils.harmonizer_utils import (
    _normalize_columns,
    harmonize_columns_by_year,
    merge_factor,
    select_and_filter_columns,
)


# ---------- merge_factor ----------

def _months():
    jan = pd.DataFrame({"DIRECTORIO": ["1", "2"], "P1": ["a", "b"], "FEX_C18": [10.0, 20.0]})
    feb = pd.DataFrame({"DIRECTORIO": ["3", "4"], "P1": ["c", "d"], "FEX_C18": [30.0, 40.0]})
    persons_feb = pd.DataFrame({"DIRECTORIO": ["3", "3", "4"], "P2": ["x", "y", "z"]})
    return [jan, feb, persons_feb]


def test_tables_keep_their_own_factor():
    out = merge_factor(_months(), "FEX_C18", "DIRECTORIO")
    assert out[0]["FEX_C18"].tolist() == [10.0, 20.0]
    assert out[1]["FEX_C18"].tolist() == [30.0, 40.0]   # used to become NaN (taken from January only)


def test_missing_factor_comes_from_any_table():
    out = merge_factor(_months(), "FEX_C18", "DIRECTORIO")
    assert out[2]["FEX_C18"].tolist() == [30.0, 30.0, 40.0]
    assert len(out[2]) == 3


def test_composite_id():
    households = pd.DataFrame({"CONGLOME": ["1", "1", "2"], "VIVIENDA": ["7", "8", "7"],
                               "HOGAR": ["1", "1", "1"], "FACTOR07": [1.0, 2.0, 3.0]})
    persons = pd.DataFrame({"CONGLOME": ["2", "1"], "VIVIENDA": ["7", "7"], "HOGAR": ["1", "1"]})
    out = merge_factor([households, persons], "FACTOR07", ["CONGLOME", "VIVIENDA", "HOGAR"])
    assert out[1]["FACTOR07"].tolist() == [3.0, 1.0]


def test_accepts_dask_and_does_not_modify_input():
    dfs = [dd.from_pandas(df, npartitions=1) for df in _months()]
    before = [list(df.columns) for df in dfs]
    merge_factor(dfs, "fex_c18", "directorio")
    assert [list(df.columns) for df in dfs] == before


# ---------- duplicate and clashing column names ----------

def test_normalize_keeps_first_of_duplicates():
    df = pd.DataFrame([[1, 2, 3]], columns=["Area", "AREA", "x"])
    out = _normalize_columns(df)
    assert list(out.columns) == ["AREA", "X"]
    assert out["AREA"].tolist() == [1]
    assert list(df.columns) == ["Area", "AREA", "x"]


def test_mapped_column_replaces_raw_column_with_same_name():
    # GEIH: raw AREA (metropolitan area code) and CLASE -> AREA
    df = pd.DataFrame({"CLASE": ["1", "2"], "AREA": ["05", "11"], "DPTO": ["5", "11"]})
    out = harmonize_columns_by_year([df], 2025, {"CLASE": "AREA", "DPTO": "ADMIN_DIVISION"})[0]
    assert out["AREA"].tolist() == ["1", "2"]
    kept = select_and_filter_columns([out], ["AREA", "ADMIN_DIVISION"], num_cols_threshold=1)[0]
    assert list(kept.columns) == ["AREA", "ADMIN_DIVISION"]


# ---------- group_and_onehot_encode / keep_variable_in_largest_table ----------

from socio4health.utils.harmonizer_utils import group_and_onehot_encode, keep_variable_in_largest_table


def test_unit_without_weight_has_missing_shares_not_zero():
    df = pd.DataFrame({"ADMIN": ["1", "1", "2"], "W": [1.0, 3.0, None], "SEX": ["1", "2", "1"]})
    out = group_and_onehot_encode([df], "ADMIN", "W", None)[0].set_index("ADMIN")
    assert out.loc[1, ["SEX_1", "SEX_2"]].tolist() == [0.25, 0.75]
    assert out.loc[2, ["SEX_1", "SEX_2"]].isna().all()


def test_variable_kept_only_in_largest_table():
    everyone = pd.DataFrame({"ADMIN": ["1"] * 4, "W": [1.0] * 4, "AGE": ["5", "20", "30", "70"], "SEX": ["1"] * 4})
    workers = pd.DataFrame({"ADMIN": ["1"] * 2, "W": [1.0] * 2, "AGE": ["20", "30"], "JOB": ["a", "b"]})
    out = keep_variable_in_largest_table([workers, everyone], keys=["ADMIN", "W"])
    assert [list(d.columns) for d in out] == [["ADMIN", "W", "JOB"], ["ADMIN", "W", "AGE", "SEX"]]
    grouped = group_and_onehot_encode(out, "ADMIN", "W", None)
    age = pd.concat(grouped).groupby("ADMIN").first().filter(like="AGE_")
    assert age.sum(axis=1).tolist() == [1.0]


def test_tables_left_with_only_keys_are_dropped():
    a = pd.DataFrame({"ADMIN": ["1", "1"], "W": [1.0, 1.0], "AGE": ["1", "2"]})
    b = pd.DataFrame({"ADMIN": ["1"], "W": [1.0], "AGE": ["1"]})
    assert len(keep_variable_in_largest_table([a, b], keys=["ADMIN", "W"])) == 1


# ---------- prune_columns ----------

from socio4health.utils.harmonizer_utils import prune_columns


def test_prune_keeps_requested_columns_lazily():
    ddf = dd.from_pandas(pd.DataFrame({"Dpto": ["5"], "P6040": ["30"], "OTHER": ["x"], "YEAR": [2024]}), npartitions=1)
    out = prune_columns([ddf], ["DPTO", "p6040", "YEAR"])
    assert isinstance(out[0], dd.DataFrame)
    assert list(out[0].columns) == ["Dpto", "P6040", "YEAR"]


def test_prune_drops_tables_with_only_year():
    a = pd.DataFrame({"X": [1], "YEAR": [2024]})
    b = pd.DataFrame({"P6040": [1], "YEAR": [2024]})
    assert [list(d.columns) for d in prune_columns([a, b], ["P6040", "YEAR"])] == [["P6040", "YEAR"]]


def test_fallback_factor_fills_rows_without_factor():
    # GEIH 2020: FEX_C18 comes from a projection file that misses some departments; FEX_C is used there
    persons = pd.DataFrame({"DIRECTORIO": ["1", "2", "9"], "FEX_C": ["10", "20", "90"]})
    projection = pd.DataFrame({"DIRECTORIO": ["1", "2"], "FEX_C18": ["11", "21"]})
    out = merge_factor([persons, projection], "FEX_C18", "DIRECTORIO", fallback_col="FEX_C")
    assert out[0]["FEX_C18"].tolist() == ["11", "21", "90"]


def test_fallback_factor_without_any_source_table():
    persons = pd.DataFrame({"DIRECTORIO": ["1"], "FEX_C": ["10"]})
    assert merge_factor([persons], "FEX_C18", "DIRECTORIO", fallback_col="FEX_C")[0]["FEX_C18"].tolist() == ["10"]


# ---------- use_person_factor ----------

from socio4health.utils.harmonizer_utils import use_person_factor


def test_person_tables_use_the_person_factor():
    keys = {"CONGLOME": ["1", "1", "1"], "VIVIENDA": ["7", "7", "7"], "HOGAR": ["1", "1", "1"]}
    households = pd.DataFrame({k: v[:1] for k, v in keys.items()} | {"FACTOR07": ["100"], "WALLS": ["2"]})
    members = pd.DataFrame(keys | {"CODPERSO": ["1", "2", "3"], "FACPOB07": ["90", "95", None], "SEX": ["1", "2", "1"]})
    education = pd.DataFrame(keys | {"CODPERSO": ["2", "1", "3"], "FACTOR07": ["100"] * 3, "EDU": ["4", "6", "1"]})
    out = use_person_factor([households, members, education], "FACTOR07", "FACPOB07",
                            ["CONGLOME", "VIVIENDA", "HOGAR", "CODPERSO"])
    assert out[0]["FACTOR07"].tolist() == ["100"]                    # households keep FACTOR07
    assert out[1]["FACTOR07"].tolist()[:2] == ["90", "95"]           # members: FACPOB07
    assert pd.isna(out[1]["FACTOR07"].tolist()[2])                   # no person factor -> no weight
    assert out[2]["FACTOR07"].tolist()[:2] == ["95", "90"]           # other person module: joined FACPOB07
    assert "FACPOB07" not in out[2].columns


# ---------- coalesce keys ("A|B") in column mappings ----------

from socio4health.utils.harmonizer_utils import apply_value_mappings, mapping_source_columns, s4h_coalesce_columns


def test_coalesce_first_non_empty_value():
    df = pd.DataFrame({"WS1": ["11", "91", "91", "13"], "WS2": [" ", "12", None, ""]})
    out = s4h_coalesce_columns(df, ["WS2", "WS1"], "WATER")
    assert out["WATER"].tolist()[:2] == ["11", "12"]
    assert out["WATER"].tolist()[2] == "91" and out["WATER"].tolist()[3] == "13"
    assert list(out.columns) == ["WATER"]


def test_coalesce_key_in_column_and_value_mappings():
    df = pd.DataFrame({"WS1": ["11", "91"], "WS2": [None, "12"], "HH6": ["1", "2"]})
    column_map = {"WS2|WS1": "H_WATER", "HH6": "AREA"}
    out = harmonize_columns_by_year([df], 2019, column_map)[0]
    assert sorted(out.columns) == ["AREA", "H_WATER"]
    out = apply_value_mappings([out], 2019, {"WS2|WS1": {"11": 1, "12": 1, "91": 4}}, column_aliases=column_map)[0]
    assert out["H_WATER"].tolist() == ["1", "1"]
    assert mapping_source_columns(column_map) == ["WS2", "WS1", "HH6"]


def test_coalesce_without_sources_leaves_table_unchanged():
    df = pd.DataFrame({"HH6": ["1"]})
    assert list(harmonize_columns_by_year([df], 2019, {"WS2|WS1": "H_WATER", "HH6": "AREA"})[0].columns) == ["AREA"]


# ---------- skip patterns: literacy fallback and code from yes/no flags ----------

from socio4health.utils.harmonizer_utils import s4h_code_from_flags


def test_coalesce_keeps_source_used_by_another_mapping():
    # ENAHO: P302 (literacy) asked only with low education; otherwise literacy follows P301A, which is
    # also the education variable
    df = pd.DataFrame({"P302": ["1", None, "2"], "P301A": ["3", "6", "1"]})
    column_map = {"P302|P301A": "P_LITERACY", "P301A": "P_EDUCATION"}
    out = harmonize_columns_by_year([df], 2024, column_map)[0]
    assert out["P_LITERACY"].tolist() == ["1", "6", "2"] and out["P_EDUCATION"].tolist() == ["3", "6", "1"]
    out = apply_value_mappings([out], 2024, {"P302|P301A": {"1": 1, "2": 2, "6": 1}, "P301A": {"3": 1, "6": 3, "1": 1}},
                               column_aliases=column_map)[0]
    assert out["P_LITERACY"].tolist() == ["1", "1", "2"] and out["P_EDUCATION"].tolist() == ["1", "3", "1"]


def test_code_from_flags_only_when_exactly_one_yes():
    df = pd.DataFrame({"A1": ["1", "2", "1", "2"], "A2": ["2", "1", "1", "2"], "A3": ["2", "2", "2", None]})
    out = s4h_code_from_flags(df, {"A1": 1, "A2": 2, "A3": 3}, "FUEL")
    assert out["FUEL"].tolist()[:2] == ["1", "2"]
    assert pd.isna(out["FUEL"].iloc[2]) and pd.isna(out["FUEL"].iloc[3])   # two "yes" / none


def test_code_from_flags_without_flag_columns():
    df = pd.DataFrame({"X": ["1"]})
    assert list(s4h_code_from_flags(df, {"A1": 1}, "FUEL").columns) == ["X"]
