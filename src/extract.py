from __future__ import annotations

import os
import json
import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def extract_bcb_sgs_series(series_id: int, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Extract BCB/SGS series as a DataFrame with columns:
    - series_id (int)
    - date (datetime64[ns])
    - value (float)
    """
    # SGS expects dd/mm/YYYY in query params
    start = datetime.fromisoformat(start_date).strftime("%d/%m/%Y")
    end = datetime.fromisoformat(end_date).strftime("%d/%m/%Y")

    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados"
    params = {"formato": "json", "dataInicial": start, "dataFinal": end}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()

    data = resp.json()  # list of {"data": "dd/mm/YYYY", "valor": "x,y"}
    df = pd.DataFrame(data)
    if df.empty:
        return pd.DataFrame(columns=["series_id", "date", "value"])

    df["series_id"] = series_id
    df["date"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
    # valor may come as "13,15"
    df["value"] = (
        df["valor"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )
    df = df[["series_id", "date", "value"]].dropna(subset=["date"])
    return df


def extract_ibge_uf_dim() -> pd.DataFrame:
    """
    Minimal IBGE dimension for UFs.
    Columns: uf_sigla, uf_nome, uf_id, regiao_nome
    """
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    df = pd.json_normalize(data)
    # Normalize columns
    df_out = pd.DataFrame({
        "uf_id": df["id"].astype(int),
        "uf_sigla": df["sigla"].astype(str),
        "uf_nome": df["nome"].astype(str),
        "regiao_nome": df["regiao.nome"].astype(str),
    })
    return df_out


def extract_anp_from_local_csv(csv_path: str) -> pd.DataFrame:
    """
    Reads ANP price file placed locally by you.
    Because ANP files vary by layout, we keep this function flexible.
    You will map/rename columns in transform.py.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"ANP file not found: {csv_path}. "
            f"Download the CSV from ANP (series histórica) and place it at this path."
        )

    # Try common separators; adjust if needed
    try:
        df = pd.read_csv(csv_path, sep=";", encoding="utf-8", low_memory=False)
    except Exception:
        df = pd.read_csv(csv_path, sep=",", encoding="utf-8", low_memory=False)
    return df


def save_bronze(df: pd.DataFrame, out_path: str) -> None:
    _ensure_dir(os.path.dirname(out_path))
    df.to_parquet(out_path, index=False)
