import dask.dataframe as dd
import pandas as pd
import pytest

from socio4health import s4h_export


def _households():
    return pd.DataFrame({"ID": ["1", "2", "3", "4"], "YEAR": ["2023", "2023", "2024", "2024"],
                         "INCOME": ["10", "20", "30", "40"]})


def _persons():
    return pd.DataFrame({"ID": ["1", "1", "2"], "AGE": ["30", "5", "40"]})


def _sorted(df):
    return df.sort_values(list(df.columns)).reset_index(drop=True)


# ---------- parquet ----------

def test_parquet_roundtrip_dask_multiple_partitions(tmp_path):
    ddf = dd.from_pandas(_households(), npartitions=2)
    out = s4h_export(ddf, tmp_path, names=["households"])
    assert out == {"households": str(tmp_path / "households")}
    back = pd.read_parquet(tmp_path / "households")
    pd.testing.assert_frame_equal(_sorted(back.astype(str)), _sorted(_households()))


def test_parquet_list_gets_default_names(tmp_path):
    out = s4h_export([_households(), dd.from_pandas(_persons(), npartitions=1)], tmp_path)
    assert set(out) == {"table_0", "table_1"}
    assert len(pd.read_parquet(out["table_1"])) == 3


def test_parquet_partition_on(tmp_path):
    s4h_export(_households(), tmp_path, names="households", partition_on="YEAR")
    assert sorted(p.name for p in (tmp_path / "households").iterdir() if p.is_dir()) == ["YEAR=2023", "YEAR=2024"]


def test_single_dataframe_default_name(tmp_path):
    assert list(s4h_export(_households(), tmp_path)) == ["data"]


# ---------- csv ----------

def test_csv_single_file(tmp_path):
    out = s4h_export(dd.from_pandas(_households(), npartitions=2), tmp_path, format="csv", names=["hh"])
    assert out == {"hh": str(tmp_path / "hh.csv")}
    back = pd.read_csv(tmp_path / "hh.csv", dtype=str)
    pd.testing.assert_frame_equal(_sorted(back), _sorted(_households()))


def test_csv_one_file_per_partition(tmp_path):
    s4h_export(dd.from_pandas(_households(), npartitions=2), tmp_path, format="csv",
               names=["hh"], single_file=False)
    assert len(list((tmp_path / "hh").glob("*.csv"))) == 2


def test_csv_passes_kwargs(tmp_path):
    s4h_export(_households(), tmp_path, format="csv", names=["hh"], sep=";")
    assert (tmp_path / "hh.csv").read_text().splitlines()[0] == "ID;YEAR;INCOME"


# ---------- duckdb ----------

def test_duckdb_dict_by_country(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    db = tmp_path / "db" / "surveys.duckdb"
    out = s4h_export({"COL": [dd.from_pandas(_households(), npartitions=2)],
                      "BRA": [_households(), _persons()]}, db, format="duckdb")
    assert set(out) == {"COL", "BRA_0", "BRA_1"}
    with duckdb.connect(str(db)) as con:
        assert con.execute('SELECT COUNT(*) FROM "COL"').fetchone()[0] == 4
        assert con.execute('SELECT COUNT(*) FROM "BRA_1"').fetchone()[0] == 3


def test_duckdb_keeps_other_tables_and_respects_overwrite(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    db = tmp_path / "surveys.duckdb"
    s4h_export(_persons(), db, format="duckdb", names=["persons"])
    s4h_export(_households(), db, format="duckdb", names=["households"])
    with pytest.raises(FileExistsError):
        s4h_export(_households(), db, format="duckdb", names=["households"])
    s4h_export(_households().head(1), db, format="duckdb", names=["households"], overwrite=True)
    with duckdb.connect(str(db)) as con:
        assert con.execute("SELECT COUNT(*) FROM households").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM persons").fetchone()[0] == 3


def test_duckdb_rejects_kwargs(tmp_path):
    with pytest.raises(TypeError):
        s4h_export(_households(), tmp_path / "x.duckdb", format="duckdb", sep=";")


# ---------- overwrite and validation ----------

def test_existing_output_raises_without_overwrite(tmp_path):
    s4h_export(_households(), tmp_path, names=["hh"])
    with pytest.raises(FileExistsError):
        s4h_export(_persons(), tmp_path, names=["hh"])
    s4h_export(_persons(), tmp_path, names=["hh"], overwrite=True)
    assert list(pd.read_parquet(tmp_path / "hh").columns) == ["ID", "AGE"]


def test_overwrite_only_touches_exported_tables(tmp_path):
    (tmp_path / "keep.txt").write_text("x")
    s4h_export(_households(), tmp_path, names=["hh"])
    s4h_export(_households(), tmp_path, names=["hh"], overwrite=True)
    assert (tmp_path / "keep.txt").exists()


def test_nothing_written_when_one_table_exists(tmp_path):
    s4h_export(_households(), tmp_path, names=["b"])
    with pytest.raises(FileExistsError):
        s4h_export([_households(), _persons()], tmp_path, names=["a", "b"])
    assert not (tmp_path / "a").exists()


@pytest.mark.parametrize("kwargs, error", [
    ({"format": "xlsx"}, ValueError),
    ({"names": ["only_one"]}, ValueError),
    ({"names": ["x y", "x-y"]}, ValueError),     # both clean to "x_y"
    ({"format": "csv", "partition_on": "YEAR"}, ValueError),
])
def test_invalid_arguments(tmp_path, kwargs, error):
    with pytest.raises(error):
        s4h_export([_households(), _persons()], tmp_path, **kwargs)


def test_empty_input(tmp_path):
    with pytest.raises(ValueError):
        s4h_export([], tmp_path)


def test_names_are_sanitized(tmp_path):
    out = s4h_export(_households(), tmp_path, names=["PNADC 2024/Q1"])
    assert list(out) == ["PNADC_2024_Q1"]


# ---------- geometry ----------

def test_geometry_is_written_as_wkt(tmp_path):
    gpd = pytest.importorskip("geopandas")
    from shapely.geometry import Point
    gdf = gpd.GeoDataFrame({"MPIO": ["73001", "11001"]},
                           geometry=[Point(0, 1), Point(2, 3)], crs="EPSG:4326")

    s4h_export(gdf, tmp_path, format="csv", names=["mpios"])
    assert pd.read_csv(tmp_path / "mpios.csv")["geometry"].tolist() == ["POINT (0 1)", "POINT (2 3)"]

    s4h_export(gdf, tmp_path, names=["mpios"])   # GeoParquet keeps the geometry
    back = gpd.read_parquet(tmp_path / "mpios" / "part.0.parquet")
    assert back.crs.to_epsg() == 4326 and back.geometry.iloc[1].equals(Point(2, 3))

    # Dask frame holding shapely objects in a plain object column
    ddf = dd.from_pandas(pd.DataFrame({"MPIO": ["1"], "GEOMETRY": pd.Series([Point(5, 6)], dtype=object)}),
                         npartitions=1)
    s4h_export(ddf, tmp_path, names=["geo_dask"])
    assert pd.read_parquet(tmp_path / "geo_dask")["GEOMETRY"].tolist() == ["POINT (5 6)"]
