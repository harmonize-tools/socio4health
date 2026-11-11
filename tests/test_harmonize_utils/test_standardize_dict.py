import warnings

import numpy as np
import pandas as pd

from socio4health.utils.harmonizer_utils import s4h_standardize_dict


def _raw_dict():
    return pd.DataFrame(
        {
            "question": ["Sexo", None, None, "Edad"],
            "variable_name": ["p1", None, None, "p2"],
            "description": ["sexo de la persona", "hombre", "mujer", "edad en años"],
            "value": [np.nan, 1, 2, np.nan],
        }
    )


def test_standardize_dict_emits_no_groupby_future_warning():
    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        s4h_standardize_dict(_raw_dict())


def test_standardize_dict_keeps_grouping_columns_and_order():
    out = s4h_standardize_dict(_raw_dict())

    assert list(out.columns) == ["question", "variable_name", "description", "value", "possible_answers"]
    sexo = out[out["variable_name"] == "P1"].iloc[0]
    assert sexo["question"] == "sexo"
    assert sexo["possible_answers"] == "hombre; mujer"
    assert sexo["value"] == "1.0; 2.0"
