import pytest
import pandas as pd
from unittest.mock import patch
from socio4health.utils.harmonizer_utils import s4h_translate_column

@patch("socio4health.utils.harmonizer_utils.GoogleTranslator.translate")
def test_basic_translation(mock_translate) -> None:
    mock_translate.side_effect = lambda x: f"translated-{x}"

    df = pd.DataFrame({"text": ["hello", "world"]})
    result = s4h_translate_column(df, column="text", language="fr")

    assert "text_fr" in result.columns
    assert result.loc[0, "text_fr"] == "translated-hello"
    assert result.loc[1, "text_fr"] == "translated-world"

@patch("socio4health.utils.harmonizer_utils.GoogleTranslator.translate")
def test_handles_null_values(mock_translate) -> None:
    mock_translate.side_effect = lambda x: f"translated-{x}"

    df = pd.DataFrame({"text": ["hola", None, "mundo"]})
    result = s4h_translate_column(df, column="text")

    assert pd.isna(result.loc[1, "text_en"])
    assert result.loc[0, "text_en"] == "translated-hola"
    assert result.loc[2, "text_en"] == "translated-mundo"

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