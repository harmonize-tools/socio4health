import json

import pandas as pd
import pytest

from socio4health import NoDataError, Schema, Survey, s4h_build_records, s4h_harmonize_records, s4h_load_surveys
from socio4health.records import s4h_clean_codes

SCHEMA = Schema({
    "AREA": {"LEVEL": ["households", "persons"], "VALUES": {"1": "Urban", "2": "Rural", "0": "Missing"}},
    "H_WATER": {"VALUES": {"1": "Piped", "2": "Well", "0": "Missing"}},
    "P_SEX": {"VALUES": {"1": "Man", "2": "Woman", "0": "Missing"}},
    "P_AGE": {"TYPE": "numeric"},
})


@pytest.fixture
def survey(tmp_path):
    data, maps = tmp_path / "data", tmp_path / "maps"
    (data / "S" / "2024").mkdir(parents=True)
    maps.mkdir()
    pd.DataFrame({"HH": ["01", "02", "03"], "REG": ["5", "5", "7"], "ZONE": ["1", "2", "1"],
                  "WATER": ["10", "20", "99"], "FEX": ["10,5", "20", "30"]}).to_csv(data / "S/2024/hogares.csv", index=False)
    pd.DataFrame({"HH": ["01", "01", "02", "03"], "REG": ["5", "5", "5", "7"], "LINE": ["1", "2", "1", "1"],
                  "SEX": ["1", "2", "2", "1"],
                  "AGE": ["40", "12", "30", "70"], "STATUS": ["1", "1", "1", "9"],
                  "FEX": ["10,5", "10,5", "20", "30"]}).to_csv(data / "S/2024/personas.csv", index=False)
    (maps / "columns.json").write_text(json.dumps({"2020-2025": {
        "REG": "ADMIN_DIVISION", "ZONE": "AREA", "WATER": "H_WATER", "SEX": "P_SEX", "AGE": "P_AGE", "FEX": "EXP_FACTOR"}}))
    (maps / "values.json").write_text(json.dumps({"WATER": {"10": 1, "20": 2}}))
    (maps / "surveys.json").write_text(json.dumps({"S": {
        "country": "XXX", "years": [2024, 2025], "parts": [{"path": "S/{year}"}],
        "reader": {"down_ext": [".csv"], "sep": ","}, "column_mapping": "columns.json", "value_mapping": "values.json",
        "household_key": [["HH"]], "person_key": [["LINE"]], "keep_rows": {"STATUS": [1]},
        "notes": {"keep_rows": "residents only"}}}))
    return s4h_load_surveys(maps / "surveys.json", data)["S"]


def test_clean_codes():
    assert s4h_clean_codes(pd.Series(["01", "1.0", " 2 ", "", None, "0"])).tolist() == ["1", "1", "2", pd.NA, pd.NA, "0"]


def test_load_surveys_rejects_unknown_rules(tmp_path):
    (tmp_path / "s.json").write_text(json.dumps({"S": {"country": "X", "years": [], "parts": [], "reader": {},
                                                       "column_mapping": "", "value_mapping": "", "household_key": [],
                                                       "person_key": [], "keep_row": {}}}))
    with pytest.raises(TypeError, match="keep_row"):
        s4h_load_surveys(tmp_path / "s.json", tmp_path)


def test_harmonize_records(survey, tmp_path):
    tables = s4h_harmonize_records(survey, SCHEMA, 2024, tmp_path / "scratch", cache_parquet=False)
    h = tables["households"].set_index("HOUSEHOLD_ID")
    p = tables["persons"].set_index("PERSON_ID")
    assert list(h.index) == ["1", "2", "3"]
    assert h["H_WATER"].tolist() == [1, 2, 99]          # unmapped codes stay as they are (checked later)
    assert h.loc["1", "WEIGHT"] == 10.5 and (h["WEIGHT_TYPE"] == "expansion").all()
    assert list(p.index) == ["1-1", "1-2", "2-1"]        # keep_rows: the person with STATUS 9 is left out
    assert p["P_AGE"].tolist() == [40, 12, 30] and str(p["P_AGE"].dtype) == "Int16"
    assert p["AREA"].tolist() == [1, 1, 2]               # persons take their household's AREA
    assert p["ADMIN_DIVISION"].tolist() == ["5", "5", "5"]


def test_missing_year_is_no_data(survey, tmp_path):
    with pytest.raises(NoDataError):
        s4h_harmonize_records(survey, SCHEMA, 2025, tmp_path / "scratch", cache_parquet=False)


def test_build_records_writes_years_and_resumes(survey, tmp_path):
    out = tmp_path / "out"
    report = s4h_build_records(survey, SCHEMA, out, cache_parquet=False)
    assert report.set_index("year")["status"].to_dict() == {2024: "ok", 2025: "no data"}
    persons = pd.read_parquet(out / "persons" / "2024")
    assert persons[["COUNTRY", "SURVEY", "ADMIN_LEVEL"]].iloc[0].tolist() == ["XXX", "S", "ADMIN_DIVISION"]
    again = s4h_build_records(survey, SCHEMA, out, years=[2024], cache_parquet=False)
    assert again["status"].tolist() == ["up to date"]


def test_build_records_with_workers_gives_the_same_tables(survey, tmp_path):
    one = s4h_build_records(survey, SCHEMA, tmp_path / "one", cache_parquet=False)
    two = s4h_build_records(survey, SCHEMA, tmp_path / "two", cache_parquet=False, workers=2)
    assert two["year"].tolist() == [2024, 2025] and two["status"].tolist() == one["status"].tolist()
    for level in ("households", "persons"):
        pd.testing.assert_frame_equal(pd.read_parquet(tmp_path / "one" / level / "2024"),
                                      pd.read_parquet(tmp_path / "two" / level / "2024"))


def test_notes_do_not_change_the_fingerprint(survey):
    from socio4health import s4h_fingerprint
    before = s4h_fingerprint(survey, SCHEMA)
    survey.notes = {"keep_rows": "reworded"}
    assert s4h_fingerprint(survey, SCHEMA) == before
    survey.keep_rows = {"STATUS": [1, 9]}
    assert s4h_fingerprint(survey, SCHEMA) != before
