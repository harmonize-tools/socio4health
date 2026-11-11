import os
from unittest.mock import patch

import dask.dataframe as dd
import pytest

from socio4health import Extractor

COLNAMES = ["UF", "N"]
COLSPECS = [(0, 2), (2, 4)]


@pytest.fixture
def census_dir(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "BA_Domicilios.txt").write_text("2903\n2905\n", encoding="latin1")
    (src / "BA_Pessoas.txt").write_text("29XXXXXXXXXXXXXXXXXX\n", encoding="latin1")
    return src


def _extractor(path, **kwargs):
    return Extractor(input_path=str(path), down_ext=[".txt"], output_path=str(path),
                     is_fwf=True, colnames=COLNAMES, colspecs=COLSPECS, **kwargs)


def test_extraction_does_not_count_rows(census_dir):
    with patch.object(dd.DataFrame, "__len__", side_effect=AssertionError("len() read the whole file")):
        ddfs = _extractor(census_dir).s4h_extract()
    assert len(ddfs) == 2


def test_file_pattern_keeps_only_matching_files(census_dir):
    ddfs = _extractor(census_dir, file_pattern=r"Domicilios").s4h_extract()
    assert len(ddfs) == 1
    out = ddfs[0].compute()
    assert out["filename"].unique().tolist() == ["BA_Domicilios.txt"]
    assert out["N"].tolist() == ["03", "05"]


def test_parquet_cache_is_built_once_and_reused(census_dir):
    first = _extractor(census_dir, file_pattern="Domicilios", cache_parquet=True).s4h_extract()
    assert os.path.isdir(census_dir / "BA_Domicilios.txt.parquet")
    expected = first[0].compute()

    with patch.object(Extractor, "_read_fwf", side_effect=AssertionError("text was parsed again")):
        second = _extractor(census_dir, file_pattern="Domicilios", cache_parquet=True).s4h_extract()

    got = second[0].compute()
    assert got.astype(str).values.tolist() == expected.astype(str).values.tolist()
    assert got["N"].astype(str).tolist() == ["03", "05"]


def test_parquet_cache_is_rebuilt_when_layout_changes(census_dir):
    _extractor(census_dir, file_pattern="Domicilios", cache_parquet=True).s4h_extract()

    ddfs = Extractor(input_path=str(census_dir), down_ext=[".txt"], output_path=str(census_dir),
                     is_fwf=True, colnames=["UF"], colspecs=[(0, 2)],
                     file_pattern="Domicilios", cache_parquet=True).s4h_extract()

    assert list(ddfs[0].columns) == ["UF", "filename"]


def test_parquet_cache_is_rebuilt_when_source_changes(census_dir):
    _extractor(census_dir, file_pattern="Domicilios", cache_parquet=True).s4h_extract()
    (census_dir / "BA_Domicilios.txt").write_text("1101\n", encoding="latin1")

    ddfs = _extractor(census_dir, file_pattern="Domicilios", cache_parquet=True).s4h_extract()

    assert ddfs[0]["N"].compute().astype(str).tolist() == ["01"]
