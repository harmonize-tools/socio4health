import json

import pytest

from socio4health import Schema
from socio4health.schema import RECORD_COLUMNS

VARIABLES = {
    "AREA": {"DESCRIPTION": "Area ", "LEVEL": ["households", "persons"], "VALUES": {"1": "Urban", "2": "Rural", "0": "Missing"}},
    "H_TYPE": {"DESCRIPTION": "Type of housing", "VALUES": {"10": "Other", "2": "Apartment", "1": "House", "0": "Missing"}},
    "P_AGE": {"DESCRIPTION": "Age", "TYPE": "numeric", "UNIT": "years", "VALUES": {"0": 0, "1": 1}},
    "P_SEX": {"DESCRIPTION": "Sex", "VALUES": {"1": "Man", "2": "Woman"}},
}


def test_levels_from_prefix_or_level_key():
    schema = Schema(VARIABLES)
    assert schema.variables_of("households") == ["AREA", "H_TYPE"]
    assert schema.variables_of("persons") == ["AREA", "P_AGE", "P_SEX"]
    assert schema.levels("AREA") == ["households", "persons"]


def test_variable_without_level_is_rejected():
    with pytest.raises(ValueError, match="LEVEL"):
        Schema({"REGION": {"VALUES": {"1": "North"}}})


def test_labels_are_sorted_by_code_and_numeric_variables_have_none():
    schema = Schema(VARIABLES)
    assert list(schema.labels("H_TYPE")) == [0, 1, 2, 10]
    assert schema.label("H_TYPE", "2") == "Apartment"
    assert schema.label("H_TYPE", 7) == "code 7"
    assert schema.valid_codes("P_SEX") == {1, 2}
    assert schema.is_numeric("P_AGE") and schema.labels("P_AGE") == {}


def test_codebook():
    book = Schema(VARIABLES).codebook()
    assert list(book.columns) == ["variable", "table", "code", "label", "description"]
    assert str(book["code"].dtype) == "Int16"
    age = book[book["variable"] == "P_AGE"]
    assert len(age) == 1 and age["label"].item() == "number (years)" and age["code"].isna().all()
    area = book[book["variable"] == "AREA"]
    assert area["table"].unique().tolist() == ["households, persons"]
    assert area["description"].unique().tolist() == ["Area"]
    assert book[book["variable"] == "H_TYPE"]["code"].tolist() == [0, 1, 2, 10]
    assert set(RECORD_COLUMNS) <= set(book["variable"])
    assert book.loc[book["variable"] == "PERSON_ID", "table"].item() == "persons"


def test_codebook_with_own_record_columns(tmp_path):
    path = tmp_path / "mapping.json"
    path.write_text(json.dumps(VARIABLES), encoding="utf-8")
    book = Schema.from_json(path).codebook({"WEIGHT": ("households", "Household weight")})
    extra = book[book["code"].isna() & (book["variable"] != "P_AGE")]
    assert extra[["variable", "table", "description"]].values.tolist() == [["WEIGHT", "households", "Household weight"]]
