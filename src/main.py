from __future__ import annotations

import json
import os
import pandas as pd

from extract import (
    extract_bcb_sgs_series,
    extract_ibge_uf_dim,
    extract_anp_from_local_csv,
    save_bronze,
)
from transform import to_silver_bcb, to_silver_anp, build_gold_metrics
from load import write_parquet_partitioned, load_duckdb
from summary import build_summary_text


def read_run_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    cfg = read_run_config("inputs/run_config.json")

    start_date = cfg["start_date"]
    end_date = cfg["end_date"]
    anp_file = cfg["anp_bronze_file"]
    duckdb_path = cfg["duckdb_path"]

    # ---------- EXTRACT ----------
    series_cfg = pd.read_csv("inputs/bcb_series.csv")
    series_cfg = series_cfg[series_cfg["enabled"].astype(str).str.lower().isin(["true", "1", "yes"])]

    bcb_frames = []
    for _, row in series_cfg.iterrows():
        series_id = int(row["series_id"])
        series_name = str(row["series_name"])
        df_bcb = extract_bcb_sgs_series(series_id, start_date, end_date)
        save_bronze(df_bcb, f"data/bronze/bcb_sgs_{series_id}.parquet")
        bcb_frames.append(to_silver_bcb(df_bcb, series_name))

    bcb_silver = pd.concat(bcb_frames, ignore_index=True) if bcb_frames else pd.DataFrame()

    uf_dim = extract_ibge_uf_dim()
    save_bronze(uf_dim, "data/bronze/ibge_uf_dim.parquet")

    df_anp_raw = extract_anp_from_local_csv(anp_file)
    # keep the raw too (optional)
    df_anp_raw.to_parquet("data/bronze/anp_raw.parquet", index=False)

    # ---------- TRANSFORM ----------
    anp_silver = to_silver_anp(df_anp_raw)

    # Join UF dim (optional but recommended)
    anp_silver = anp_silver.merge(uf_dim[["uf_sigla", "uf_nome", "regiao_nome"]], on="uf_sigla", how="left")

    # ---------- GOLD ----------
    gold = build_gold_metrics(bcb_silver, anp_silver)

    # ---------- LOAD ----------
    # Silver outputs
    bcb_silver.to_parquet("data/silver/bcb_sgs.parquet", index=False)
    anp_silver.to_parquet("data/silver/anp_prices.parquet", index=False)
    uf_dim.to_parquet("data/silver/dim_uf.parquet", index=False)

    # Gold outputs (Parquet partitioned)
    write_parquet_partitioned(gold["bcb_monthly"], "data/gold/bcb_monthly", partition_cols=["series_id"])
    write_parquet_partitioned(gold["anp_monthly"], "data/gold/anp_monthly", partition_cols=["uf_sigla"])

    # DuckDB loads (simple replace for lab)
    load_duckdb(bcb_silver, duckdb_path, "silver_bcb_sgs")
    load_duckdb(anp_silver, duckdb_path, "silver_anp_prices")
    load_duckdb(uf_dim, duckdb_path, "dim_uf")
    load_duckdb(gold["bcb_monthly"], duckdb_path, "gold_bcb_monthly")
    load_duckdb(gold["anp_monthly"], duckdb_path, "gold_anp_monthly")

    # ---------- SUMMARY ----------
    summary_text = build_summary_text(bcb_silver, anp_silver)
    os.makedirs("data/gold", exist_ok=True)
    with open("data/gold/summary.md", "w", encoding="utf-8") as f:
        f.write(summary_text)

    print("ETL concluído com sucesso.")
    print("\n--- RESUMO ---")
    print(summary_text)


if __name__ == "__main__":
    main()
