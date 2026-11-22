import pandas as pd
import pytest

from socio4health.utils.extractor_utils import s4h_parse_ibge_layout


def _dictionary():
    rows = [
        ["Dicionário das variáveis da PNAD Contínua", "", "", "", "", "", ""],
        ["Posição Inicial", "Tamanho", "Código da variável", "Quesito", "", "Categorias", ""],
        ["1", "4", "Ano", "", "Ano de referência", "", ""],
        ["6", "2", "UF", "", "Unidade da Federação", "11", "Rondônia"],
        ["", "", "", "", "", "12", "Acre"],
        ["8", "2", "Capital", "", "Município da Capital", "11", "Porto Velho"],
        ["460", "1", "S01001", "1", "Este domicílio é do tipo", "1", "Casa"],
        ["", "", "", "", "", "2", "Apartamento"],
    ]
    return pd.DataFrame(rows)


def test_reads_positions_and_skips_category_rows():
    names, specs = s4h_parse_ibge_layout(_dictionary())
    assert names == ["Ano", "UF", "Capital", "S01001"]
    assert specs == [(0, 4), (5, 7), (7, 9), (459, 460)]


def test_only_requested_variables():
    names, specs = s4h_parse_ibge_layout(_dictionary(), variables=["capital", "S01001"])
    assert names == ["Capital", "S01001"] and specs == [(7, 9), (459, 460)]


def test_empty_dictionary():
    with pytest.raises(ValueError):
        s4h_parse_ibge_layout(pd.DataFrame([["nothing here"]]))
