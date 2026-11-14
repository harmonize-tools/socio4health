import pandas as pd

from socio4health.utils.harmonizer_utils import _normalize_value_token, apply_value_mappings


def test_integer_codes_ignore_leading_zeros_and_decimals():
    assert {_normalize_value_token(v) for v in ("02", "2", "2.0", 2, 2.0, " 02 ")} == {"2"}
    assert _normalize_value_token("0") == "0"
    assert _normalize_value_token("00") == "0"


def test_non_numeric_tokens_are_kept():
    assert _normalize_value_token("A1") == "A1"
    assert _normalize_value_token("nan") == "0"
    assert _normalize_value_token(None) is None


def test_zero_padded_data_matches_int_key_mapping():
    # Fixed-width data keeps "02"; mappings loaded with load_int_key_mapping have the key 2.
    df = pd.DataFrame({"V0607": ["02", "04", "1"]})
    out = apply_value_mappings([df], 2024, {"V0607": {2: 1, 4: 2, 1: 9}})[0]
    assert out["V0607"].tolist() == ["1", "2", "9"]


def test_zero_padded_mapping_matches_numeric_data():
    df = pd.DataFrame({"V0607": [2.0, 4.0]})
    out = apply_value_mappings([df], 2024, {"V0607": {"02": 1, "04": 2}})[0]
    assert out["V0607"].tolist() == ["1", "2"]
