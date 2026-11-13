"""
Load step of the socio4health pipeline: write extracted or harmonized DataFrames to disk.

:func:`s4h_export` takes the output of :meth:`Extractor.s4h_extract`, of any :class:`Harmonizer` method or a
plain ``pandas`` DataFrame, and stores it as Parquet, CSV or a DuckDB database, one table per DataFrame.
"""

import re
import shutil
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple, Union

import dask.dataframe as dd
import pandas as pd

from socio4health.utils.deps import import_optional
from socio4health.utils.log import logger, progress_bar

FrameLike = Union[dd.DataFrame, pd.DataFrame]
FramesInput = Union[FrameLike, Sequence[FrameLike], Mapping[str, Union[FrameLike, Sequence[FrameLike]]]]

FORMATS = ("parquet", "csv", "duckdb")


def s4h_export(
        dfs: FramesInput,
        path: Union[str, Path],
        format: str = "parquet",
        names: Optional[Sequence[str]] = None,
        overwrite: bool = False,
        single_file: bool = True,
        partition_on: Optional[Union[str, List[str]]] = None,
        **kwargs,
) -> Dict[str, str]:
    """
    Write one or more DataFrames to Parquet, CSV or a DuckDB database (the *load* step of the pipeline).

    Each DataFrame becomes one table. `Dask <https://docs.dask.org>`_ DataFrames are written partition by
    partition, so data larger than memory can be exported.

    Parameters
    ----------
    dfs : DataFrame, list of DataFrames or dict
        What to export. Accepts a single `Dask <https://docs.dask.org>`_ or ``pandas`` DataFrame, a list of them
        (e.g. the output of :meth:`Extractor.s4h_extract` or :meth:`Harmonizer.s4h_vertical_merge`), or a dict
        mapping a name to a DataFrame or to a list of DataFrames (e.g. the output of
        :meth:`Harmonizer.s4h_harmonize_dataframes`, keyed by country).
    path : str or Path
        For ``"parquet"`` and ``"csv"``, the output directory (created if needed). For ``"duckdb"``, the database
        file (e.g. ``"data/surveys.duckdb"``), which is created if it does not exist and can already hold other
        tables.
    format : {"parquet", "csv", "duckdb"}
        Output format. Defaults to ``"parquet"``.
    names : list of str, optional
        Table names, one per DataFrame, when ``dfs`` is a list. By default tables are named ``table_0``,
        ``table_1``... (or ``data`` for a single DataFrame). With a dict, the keys are used, followed by ``_0``,
        ``_1``... when a key holds several DataFrames. Names are reduced to letters, digits and ``_``.
    overwrite : bool
        Replace tables that already exist. Defaults to ``False``, which raises an error before anything is
        written. Only the files or tables being exported are replaced, never the rest of ``path``.
    single_file : bool
        CSV only. If ``True`` (default), each table is one ``<name>.csv`` file. If ``False``, each table is a
        ``<name>/`` folder with one CSV per partition, which is faster for large data.
    partition_on : str or list of str, optional
        Parquet only. Columns used to split each table into sub-folders (e.g. ``"YEAR"``), so readers can load
        only the partitions they need.
    **kwargs
        Extra arguments passed to `dask.dataframe.to_parquet
        <https://docs.dask.org/en/stable/generated/dask.dataframe.to_parquet.html>`_ or `dask.dataframe.to_csv
        <https://docs.dask.org/en/stable/generated/dask.dataframe.to_csv.html>`_ (e.g. ``compression="gzip"``,
        ``sep=";"``, ``encoding="latin1"``). Not accepted for ``"duckdb"``.

    Returns
    -------
    dict of str to str
        Maps each table name to where it was written: the Parquet folder, the CSV file (or folder) or, for
        DuckDB, the database file.

    Raises
    ------
    ValueError
        If ``format`` is not supported, ``names`` does not match ``dfs``, two tables get the same name, or there
        is nothing to export.
    FileExistsError
        If a table already exists and ``overwrite`` is ``False``.
    ImportError
        If ``format="duckdb"`` and ``duckdb`` is not installed (``pip install "socio4health-core[duckdb]"``).

    Note
    -----
    - Parquet tables are written as folders (``<path>/<name>/part.0.parquet``...). ``pandas.read_parquet``,
      ``dask.dataframe.read_parquet`` and DuckDB (``read_parquet('<path>/<name>/*.parquet')``) read them
      directly.
    - Geometry columns (from geospatial files) are stored as WKT text in CSV and DuckDB. A single ``pandas``
      GeoDataFrame exported to Parquet is written as GeoParquet and keeps its geometry and CRS.
    - The DataFrame index is not written.

    Examples
    --------
    >>> from socio4health import Extractor, Harmonizer, s4h_export
    >>> dfs = Extractor(input_path="data/raw").s4h_extract()
    >>> merged = Harmonizer().s4h_vertical_merge(dfs)
    >>> s4h_export(merged, "data/clean", names=["households"])   # data/clean/households/*.parquet
    >>> s4h_export({"COL": col_dfs, "BRA": bra_dfs}, "data/surveys.duckdb", format="duckdb")
    """
    format = str(format).lower().strip()
    if format not in FORMATS:
        raise ValueError(f"format must be one of {FORMATS}, got {format!r}.")
    if format == "duckdb" and kwargs:
        raise TypeError(f"Extra arguments are not supported for format='duckdb': {sorted(kwargs)}")
    if partition_on is not None and format != "parquet":
        raise ValueError("partition_on is only supported for format='parquet'.")

    tables = _named_tables(dfs, names)
    path = Path(path)

    if format == "duckdb":
        return _export_duckdb(tables, path, overwrite)

    targets = {
        name: path / name if format == "parquet" or not single_file else path / f"{name}.csv"
        for name, _ in tables
    }
    existing = [str(t) for t in targets.values() if t.exists()]
    if existing and not overwrite:
        raise FileExistsError(f"Output already exists (use overwrite=True to replace it): {existing}")

    path.mkdir(parents=True, exist_ok=True)
    written = {}
    with progress_bar(total=len(tables), desc=f"Exporting to {format}", unit="table") as bar:
        for name, df in tables:
            target = targets[name]
            _remove(target)
            if format == "parquet":
                _write_parquet(df, target, partition_on, kwargs)
            else:
                _write_csv(df, target, single_file, kwargs)
            written[name] = str(target)
            logger.debug(f"Wrote table '{name}' to {target}")
            bar.update(1)

    logger.info(f"Exported {len(written)} table(s) as {format} to {path}")
    return written


def _named_tables(dfs: FramesInput, names: Optional[Sequence[str]]) -> List[Tuple[str, FrameLike]]:
    """Turn every accepted input shape into a list of ``(table_name, dataframe)`` pairs."""
    if isinstance(dfs, Mapping):
        if names is not None:
            raise ValueError("names cannot be used when dfs is a dict: the keys are used as names.")
        pairs = []
        for key, value in dfs.items():
            if _is_frame(value):
                pairs.append((str(key), value))
            else:
                frames = list(value)
                if len(frames) == 1:
                    pairs.append((str(key), frames[0]))
                else:
                    pairs.extend((f"{key}_{i}", frame) for i, frame in enumerate(frames))
    else:
        frames = [dfs] if _is_frame(dfs) else list(dfs)
        if names is None:
            names = ["data"] if _is_frame(dfs) else [f"table_{i}" for i in range(len(frames))]
        elif isinstance(names, str):
            names = [names]
        if len(names) != len(frames):
            raise ValueError(f"Got {len(names)} name(s) for {len(frames)} DataFrame(s).")
        pairs = list(zip(names, frames))

    if not pairs:
        raise ValueError("There are no DataFrames to export.")

    result, seen = [], set()
    for name, frame in pairs:
        if not _is_frame(frame):
            raise TypeError(f"Table '{name}' is a {type(frame).__name__}, expected a Dask or pandas DataFrame.")
        clean = _clean_name(name)
        if clean in seen:
            raise ValueError(f"Two tables would be named '{clean}'. Pass distinct names.")
        seen.add(clean)
        result.append((clean, frame))
    return result


def _is_frame(obj) -> bool:
    return isinstance(obj, (dd.DataFrame, pd.DataFrame))


def _clean_name(name) -> str:
    clean = re.sub(r"\W+", "_", str(name)).strip("_")
    if not clean:
        raise ValueError(f"Invalid table name: {name!r}")
    return clean


def _is_geodataframe(df) -> bool:
    return isinstance(df, pd.DataFrame) and type(df).__name__ == "GeoDataFrame"


def _geometry_to_wkt(pdf: pd.DataFrame) -> pd.DataFrame:
    """Replace geometry columns (shapely objects) with their WKT text so any writer can store them."""
    pdf = pd.DataFrame(pdf)  # a GeoDataFrame becomes a plain DataFrame
    for col in pdf.columns:
        series = pdf[col]
        if str(series.dtype) != "geometry":
            if series.dtype != object:
                continue
            sample = series.dropna()
            if not (len(sample) and hasattr(sample.iloc[0], "wkt") and hasattr(sample.iloc[0], "geom_type")):
                continue
        pdf[col] = pd.Series([g.wkt if hasattr(g, "wkt") else None for g in series],
                             index=series.index, dtype=object)
    return pdf


def _to_dask(df: FrameLike) -> dd.DataFrame:
    if isinstance(df, dd.DataFrame):
        meta = _geometry_to_wkt(df._meta)
        return df.map_partitions(_geometry_to_wkt, meta=meta)
    return dd.from_pandas(_geometry_to_wkt(df).reset_index(drop=True), npartitions=1)


def _remove(target: Path) -> None:
    if target.is_dir():
        shutil.rmtree(target)
    elif target.exists():
        target.unlink()


def _write_parquet(df: FrameLike, target: Path, partition_on, kwargs) -> None:
    if _is_geodataframe(df) and partition_on is None:
        target.mkdir(parents=True)
        df.to_parquet(target / "part.0.parquet", index=False, **kwargs)
        return
    try:
        _to_dask(df).to_parquet(str(target), write_index=False, partition_on=partition_on, **kwargs)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Could not write '{target.name}' as Parquet: {exc}. This usually means a column mixes types "
            "(e.g. numbers and text); convert it first, e.g. df.astype({'COL': str}), or export as CSV."
        ) from exc


def _write_csv(df: FrameLike, target: Path, single_file: bool, kwargs) -> None:
    ddf = _to_dask(df)
    options = {"index": False, **kwargs}
    if single_file:
        ddf.to_csv(str(target), single_file=True, **options)
    else:
        target.mkdir(parents=True)
        ddf.to_csv(str(target / "part-*.csv"), **options)


def _export_duckdb(tables: List[Tuple[str, FrameLike]], db_path: Path, overwrite: bool) -> Dict[str, str]:
    duckdb = import_optional("duckdb", extra="duckdb")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        existing = {row[0] for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()}
        clashes = [name for name, _ in tables if name in existing]
        if clashes and not overwrite:
            raise FileExistsError(
                f"Table(s) {clashes} already exist in {db_path} (use overwrite=True to replace them).")

        with progress_bar(total=len(tables), desc="Exporting to duckdb", unit="table") as bar:
            for name, df in tables:
                _write_duckdb_table(con, name, df)
                logger.debug(f"Wrote table '{name}' to {db_path}")
                bar.update(1)
    finally:
        con.close()

    logger.info(f"Exported {len(tables)} table(s) to DuckDB database {db_path}")
    return {name: str(db_path) for name, _ in tables}


def _write_duckdb_table(con, name: str, df: FrameLike) -> None:
    """Create (or replace) a table from a DataFrame, loading a Dask DataFrame one partition at a time."""
    parts = _to_dask(df).to_delayed()
    con.execute("BEGIN TRANSACTION")
    try:
        for i, part in enumerate(parts):
            con.register("_s4h_part", part.compute())
            if i == 0:
                con.execute(f'CREATE OR REPLACE TABLE "{name}" AS SELECT * FROM _s4h_part')
            else:
                con.execute(f'INSERT INTO "{name}" BY NAME SELECT * FROM _s4h_part')
            con.unregister("_s4h_part")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
