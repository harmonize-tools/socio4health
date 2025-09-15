import pytest
import numpy as np
import pandas as pd
from socio4health.utils.harmonizer_utils import s4h_standardize_dict, _process_group

def test_basic_standardisation() -> None:
    df = pd.DataFrame(
        {
            "question": ["Do you have a pet?", None],
            "variable_name": ["pet", "pet"],
            "description": ["Yes", "No"],
            "value": ["1", "0"],
        }
    )

    result = s4h_standardize_dict(df)

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == [
        "question",
        "variable_name",
        "description",
        "value",
        "possible_answers",
    ]
    assert result.shape[0] == 1

    row = result.iloc[0]
    assert row["question"] == "do you have a pet?"
    assert row["variable_name"] == "pet"
    assert row["possible_answers"] == "yes; no"
    assert row["value"] == "1; 0"
    assert pd.isna(row["description"])

def test_with_subquestion() -> None:
    df = pd.DataFrame(
        {
            "question": ["Do you own a pet?", np.nan, np.nan],
            "subquestion": pd.Series([np.nan, np.nan, "What kind?"], dtype="string"),
            "variable_name": ["pet", np.nan, "pet_1"],
            "description": ["Yes", "No", "Which"],
            "value": ["1", "2", np.nan],
        }
    )

    result = s4h_standardize_dict(df)

    assert result.shape[0] == 2
    row1 = result.iloc[0]
    row2 = result.iloc[1]
    assert row2["question"] == "do you own a pet? what kind?"
    assert "yes" in row1["possible_answers"]
    assert row1["value"] == "1; 2"

def test_removes_empty_rows() -> None:
    df = pd.DataFrame(
        {
            "question": ["Do you smoke?", "Do you smoke?"],
            "variable_name": ["smoke", None],
            "description": ["Yes", np.nan],
            "value": ["1", np.nan],
        }
    )

    result = s4h_standardize_dict(df)

    assert result.shape[0] == 1
    assert "yes" in result.loc[0, "possible_answers"]

def test_missing_required_columns() -> None:
    df = pd.DataFrame({"foo": [1], "bar": [2]})

    with pytest.raises(ValueError) as exc:
        s4h_standardize_dict(df)

    assert "required columns" in str(exc.value).lower()

def test_invalid_subquestion_type() -> None:
    df = pd.DataFrame(
        {
            "question": ["How many children do you have?"],
            "variable_name": ["children"],
            "description": ["Three"],
            "value": ["3"],
            "subquestion": [123],
        }
    )

    with pytest.raises(TypeError) as exc:
        s4h_standardize_dict(df)

    assert "subquestion" in str(exc.value).lower()


def test_non_dataframe_input() -> None:
    data = [
        {
            "question": "How old are you?",
            "variable_name": "age",
            "description": "Age in years",
            "value": "30",
        }
    ]

    with pytest.raises(TypeError):
        s4h_standardize_dict(data)