from __future__ import annotations

import os
import duckdb
import pandas as pd


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_parquet_partitioned(df: pd.DataFrame, base_dir: str, partition_cols: list[str]) -> None:
    _ensure_dir(base_dir)
    # pandas -> parquet partitioned requires pyarrow engine
    df.to_parquet(base_dir, index=False, partition_cols=partition_cols, engine="pyarrow")


def load_duckdb(df: pd.DataFrame, db_path: str, table: str) -> None:
    _ensure_dir(os.path.dirname(db_path) or ".")
    con = duckdb.connect(db_path)
    try:
        con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df LIMIT 0;")
        # Replace strategy (idempotent for this lab). For incremental, implement MERGE by keys.
        con.execute(f"DELETE FROM {table};")
        con.register("df_view", df)
        con.execute(f"INSERT INTO {table} SELECT * FROM df_view;")
    finally:
        con.close()
