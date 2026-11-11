from unittest.mock import patch

import dask.dataframe as dd
import pandas as pd

from socio4health.harmonizer import Harmonizer


def _make_harmonizer(key_values):
    return Harmonizer(
        dict_df=pd.DataFrame(
            {
                "variable_name": ["income", "ignored"],
                "category": ["Household", "Other"],
            }
        ),
        categories=["Household"],
        key_col="geo",
        key_val=key_values,
        extra_cols=["record"],
        join_key="area",
    )


def _make_source():
    return dd.from_pandas(
        pd.DataFrame(
            {
                "geo": ["CA", "NY"],
                "income": [10, 20],
                "record": ["row-1", "row-2"],
                "area": ["west", "east"],
                "ignored": [100, 200],
            }
        ),
        npartitions=2,
    )


def test_data_selector_does_not_compute_and_projects_required_columns():
    source = _make_source()

    with patch.object(
        dd.DataFrame,
        "compute",
        side_effect=AssertionError("selector unexpectedly computed a Dask DataFrame"),
    ):
        selected = _make_harmonizer(["CA"]).s4h_data_selector([source])

    assert isinstance(selected[0], dd.DataFrame)
    assert list(selected[0].columns) == ["GEO", "INCOME", "RECORD", "AREA"]
    assert selected[0].compute().values.tolist() == [["CA", 10, "row-1", "west"]]


def test_data_selector_returns_empty_lazy_result_without_eager_warning(caplog):
    source = _make_source()

    selected = _make_harmonizer(["missing"]).s4h_data_selector([source])

    assert "No rows found matching key values in DataFrame" not in caplog.text
    assert selected[0].compute().empty
