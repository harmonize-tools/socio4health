import pandas as pd
import pytest

from socio4health import Schema, s4h_check_records, s4h_shares

SCHEMA = Schema({
    "AREA": {"LEVEL": ["households", "persons"], "VALUES": {"1": "Urban", "2": "Rural", "0": "Missing"}},
    "H_WATER": {"VALUES": {"1": "Piped (inside)", "2": "Well", "0": "Missing"}},
    "P_AGE": {"TYPE": "numeric"},
    "P_EDUCATION": {"UNIVERSE": {"P_AGE": [25, None]}, "VALUES": {"1": "None", "2": "University", "0": "Missing"}},
})


def _households(weight_type="expansion"):
    return pd.DataFrame({"YEAR": [2024] * 4, "UNIT": ["A", "A", "A", "B"], "HOUSEHOLD_ID": ["1", "2", "3", "4"],
                         "WEIGHT": [1.0, 1.0, 2.0, 5.0], "WEIGHT_TYPE": weight_type,
                         "AREA": [1, 2, 1, 2], "H_WATER": [1, 1, 2, 0]})


def _persons():
    return pd.DataFrame({"YEAR": [2024] * 5, "UNIT": ["A", "A", "A", "B", None], "PERSON_ID": list("abcde"),
                         "WEIGHT": [1.0, 3.0, 1.0, 2.0, 9.0], "WEIGHT_TYPE": "expansion",
                         "AREA": [2, 2, 2, 2, 2], "P_AGE": pd.array([30, 10, 40, 60, 50], dtype="Int16"),
                         "P_EDUCATION": [2, 1, 1, 2, 1]})


def test_shares_per_unit():
    out = s4h_shares({"households": _households(), "persons": _persons()}, SCHEMA, by=["YEAR", "UNIT"]).set_index("UNIT")
    assert out.loc["A", "HOUSEHOLDS"] == 4 and out.loc["A", "POPULATION"] == 5   # person without unit is left out
    assert out.loc["A", "H_WATER_Piped_inside"] == pytest.approx(0.5)
    assert out.loc["A", "H_WATER_Well"] == pytest.approx(0.5)
    assert out.loc["A", "AREA_Urban"] == pytest.approx(0.75)        # AREA is summarized on households
    assert out.loc["A", "P_AGE_mean"] == pytest.approx((30 + 30 + 40) / 5)
    # education only for people aged 25+: the 10-year-old is not counted
    assert out.loc["A", "P_EDUCATION_University"] == pytest.approx(0.5)
    shares = out.filter(like="H_WATER_")
    assert shares.sum(axis=1, min_count=1).tolist() == pytest.approx([1.0, 1.0])


def test_relative_weights_leave_totals_empty():
    out = s4h_shares({"households": _households("relative"), "persons": _persons()}, SCHEMA, by=["UNIT"])
    assert out["HOUSEHOLDS"].isna().all()
    assert out["POPULATION"].notna().all()
    assert out["H_WATER_Well"].notna().any()   # shares are still computed


def test_missing_table_gives_empty_totals():
    out = s4h_shares({"households": None, "persons": _persons()}, SCHEMA, by=["UNIT"])
    assert "HOUSEHOLDS" in out.columns and out["HOUSEHOLDS"].isna().all()
    assert not any(c.startswith("H_WATER") for c in out.columns)


def test_check_records_sets_unknown_codes_to_missing():
    df = _households().assign(H_WATER=[1, 7, 7, 2], HOUSEHOLD_ID=["1", "1", "2", None], WEIGHT=[1.0, None, 1.0, 1.0])
    clean, summary = s4h_check_records(df, SCHEMA, "HOUSEHOLD_ID")
    assert clean["H_WATER"].tolist() == [1, 0, 0, 2]
    assert df["H_WATER"].tolist() == [1, 7, 7, 2]          # the input is not modified
    assert summary == {"records": 4, "duplicate IDs": 1, "without weight": 0.25, "codes not in codebook": {"H_WATER": 2}}
