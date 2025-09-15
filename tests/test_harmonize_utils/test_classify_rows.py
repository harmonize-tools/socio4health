import pytest
import importlib
import pandas as pd
from unittest.mock import MagicMock, patch
from socio4health.utils.harmonizer_utils import s4h_get_classifier, _classifier, s4h_classify_rows

MODULE_PATH = "socio4health.utils.harmonizer_utils"

@patch(f"{MODULE_PATH}.torch.cuda.is_available", return_value=False)
@patch(f"{MODULE_PATH}.pipeline")
def test_get_classifier_singleton(mock_pipeline, mock_cuda) -> None:
    """Calling get_classifier twice should invoke `pipeline()` only once."""
    

    module = importlib.import_module(MODULE_PATH)
    module._classifier = None

    mock_pipeline.return_value = MagicMock(name="dummy-pipeline")

    path = "hf/dummy-model"
    clf1 = s4h_get_classifier(path)
    clf2 = s4h_get_classifier(path)

    assert clf1 is clf2
    mock_pipeline.assert_called_once_with(
        "text-classification",
        model=path,
        tokenizer=path,
        device=-1,
    )

def test_get_classifier_invalid_path() -> None:
    with pytest.raises(ValueError, match="MODEL_PATH"):
        s4h_get_classifier("non_existing_model_identifier")

@patch(f"{MODULE_PATH}.get_classifier")
def test_classify_rows_basic(mock_get_classifier) -> None:
    """DataFrame gains a new prediction column with expected labels."""
    dummy_pipeline = MagicMock()
    dummy_pipeline.side_effect = lambda text, truncation, max_length: [
        {"label": "A"}
    ]
    mock_get_classifier.return_value = dummy_pipeline

    df = pd.DataFrame(
        {
            "col_1": ["Hello world"],
            "col_2": ["This is a test"],
            "col_3": ["Ignore me"],
        }
    )

    result = s4h_classify_rows(
        data=df,
        col1="col_1",
        col2="col_2",
        col3="col_3",
        new_column_name="prediction",
        MODEL_PATH="hf/dummy-model",
    )

    assert "prediction" in result.columns
    assert result.loc[0, "prediction"] == "A"
    pd.testing.assert_series_equal(result["col_1"], df["col_1"])

def test_classify_rows_invalid_input_type() -> None:
    with pytest.raises(TypeError):
        s4h_classify_rows(
            data=["not", "a", "DataFrame"],
            col1="a",
            col2="b",
            col3="c",
        )

def test_classify_rows_missing_column() -> None:
    df = pd.DataFrame({"x": ["text"]})
    with pytest.raises(ValueError, match="not found"):
        s4h_classify_rows(df, col1="x", col2="y", col3="z")

def test_classify_rows_duplicate_new_column() -> None:
    df = pd.DataFrame(
        {
            "col1": ["foo"],
            "col2": ["bar"],
            "col3": ["baz"],
            "category": ["existing"],
        }
    )
    with pytest.raises(ValueError, match="already exists"):
        s4h_classify_rows(df, "col1", "col2", "col3", new_column_name="category")