import pytest
import pandas as pd
from unittest.mock import patch
from socio4health.utils.harmonizer_utils import s4h_translate_column

@patch("socio4health.utils.harmonizer_utils._translate_texts")
def test_basic_translation(mock_translate) -> None:
    mock_translate.side_effect = lambda values: [f"translated-{value}" for value in values]

    df = pd.DataFrame({"text": ["hola", "mundo"]})
    result = s4h_translate_column(df, column="text", language="en")

    assert "text_en" in result.columns
    assert result.loc[0, "text_en"] == "translated-hola"
    assert result.loc[1, "text_en"] == "translated-mundo"

@patch("socio4health.utils.harmonizer_utils._translate_texts")
def test_handles_null_values(mock_translate) -> None:
    mock_translate.side_effect = lambda values: [f"translated-{value}" for value in values]

    df = pd.DataFrame({"text": ["buenos dias", None, "buenas noches"]})
    result = s4h_translate_column(df, column="text")

    assert pd.isna(result.loc[1, "text_en"])
    assert result.loc[0, "text_en"] == "translated-buenos dias"
    assert result.loc[2, "text_en"] == "translated-buenas noches"

@patch("socio4health.utils.harmonizer_utils._translate_texts")
def test_translates_repeated_values_once(mock_translate) -> None:
    mock_translate.side_effect = lambda values: [f"translated-{value}" for value in values]

    df = pd.DataFrame({"text": ["pregunta repetida", "pregunta repetida", "otra pregunta"]})
    result = s4h_translate_column(df, column="text")

    mock_translate.assert_called_once_with(["pregunta repetida", "otra pregunta"])
    assert result["text_en"].tolist() == [
        "translated-pregunta repetida",
        "translated-pregunta repetida",
        "translated-otra pregunta",
    ]

def test_invalid_input_type() -> None:
    with pytest.raises(TypeError, match="data must be a pandas DataFrame"):
        s4h_translate_column(["not", "a", "DataFrame"], column="text")

def test_invalid_column_name_type() -> None:
    df = pd.DataFrame({"text": ["hola"]})
    with pytest.raises(TypeError, match="column must be a text string"):
        s4h_translate_column(df, column=123)

def test_column_not_found() -> None:
    df = pd.DataFrame({"text": ["hola"]})
    with pytest.raises(ValueError, match="is not found in the DataFrame"):
        s4h_translate_column(df, column="nonexistent")

def test_invalid_language_code() -> None:
    df = pd.DataFrame({"text": ["hola"]})
    with pytest.raises(ValueError, match="language.*2-letter"):
        s4h_translate_column(df, column="text", language="spanish")

def test_unsupported_target_language() -> None:
    df = pd.DataFrame({"text": ["hola"]})
    with pytest.raises(ValueError, match="only supports English"):
        s4h_translate_column(df, column="text", language="fr")
