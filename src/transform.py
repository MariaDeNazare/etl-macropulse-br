from __future__ import annotations

import re
import unicodedata
import pandas as pd


def to_silver_bcb(df_bcb: pd.DataFrame, series_name: str) -> pd.DataFrame:
    df = df_bcb.copy()
    df["series_name"] = series_name
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values(["series_id", "date"]).drop_duplicates(["series_id", "date"])
    df = df.dropna(subset=["date"])
    return df[["series_id", "series_name", "date", "value"]]


def to_silver_anp(df_anp_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize ANP raw CSV into a clean schema:
    - date_ref (datetime)
    - uf_sigla (str)
    - product (str)
    - price (float)
    """
    df = df_anp_raw.copy()

    def norm(s: str) -> str:
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = s.lower().strip()
        s = re.sub(r"[^a-z0-9]+", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    cols = {norm(c): c for c in df.columns}

    def pick_exact(*normalized_names: str) -> str | None:
        for n in normalized_names:
            if n in cols:
                return cols[n]
        return None

    def pick_contains(all_tokens: list[str]) -> str | None:
        # return first col that contains all tokens
        for nrm, original in cols.items():
            if all(t in nrm for t in all_tokens):
                return original
        return None

    # --- Map columns (based on your header) ---
    # Example header: "Estado - Sigla", "Produto", "Data da Coleta" (may vary slightly)
    col_uf = pick_exact("estado sigla") or pick_contains(["estado", "sigla"])
    col_prod = pick_exact("produto") or pick_contains(["produto"])
    col_date = pick_exact("data da coleta") or pick_contains(["data", "coleta"]) or pick_contains(["data"])

    # Price column varies (depends on ANP file type)
    col_price = (
        pick_exact("valor de venda")
        or pick_contains(["valor", "venda"])
        or pick_exact("preco medio revenda")
        or pick_contains(["preco", "medio"])
        or pick_contains(["preco"])
    )

    missing = [("uf", col_uf), ("product", col_prod), ("date", col_date), ("price", col_price)]
    missing = [k for k, v in missing if v is None]
    if missing:
        raise ValueError(
            "Não consegui mapear automaticamente as colunas da ANP. Faltando: "
            + ", ".join(missing)
            + ". Ajuste o mapeamento no to_silver_anp() conforme o cabeçalho do CSV."
        )

    df_out = pd.DataFrame(
        {
            "uf_sigla": df[col_uf].astype(str).str.strip().str.upper(),
            "product": df[col_prod].astype(str).str.strip(),
            "date_ref": df[col_date].astype(str).str.strip(),
            "price": df[col_price].astype(str).str.strip(),
        }
    )

    # Normalize date (dayfirst for BR)
    df_out["date_ref"] = pd.to_datetime(df_out["date_ref"], errors="coerce", dayfirst=True)

    # Normalize price safely:
    # - if contains comma -> pt-BR: "1.234,56" -> 1234.56
    # - else -> parse directly (e.g. "6.59" or "6")
    price = df_out["price"].astype(str).str.strip()
    has_comma = price.str.contains(",", na=False)

    price_pt = (
        price[has_comma]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    price_en = price[~has_comma]

    df_out.loc[has_comma, "price"] = pd.to_numeric(price_pt, errors="coerce")
    df_out.loc[~has_comma, "price"] = pd.to_numeric(price_en, errors="coerce")

    # Basic quality filters
    df_out = df_out.dropna(subset=["date_ref", "uf_sigla", "product", "price"])
    df_out = df_out[df_out["price"] > 0]

    # Deduplicate at natural key level
    df_out = df_out.drop_duplicates(["date_ref", "uf_sigla", "product"])

    return df_out

def build_gold_metrics(bcb_silver: pd.DataFrame, anp_silver: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Build gold tables for analysis.
    Returns dict of named DataFrames.
    """
    gold: dict[str, pd.DataFrame] = {}

    # BCB: monthly stats (avg and last value per month)
    bcb = bcb_silver.copy()
    bcb["month"] = bcb["date"].dt.to_period("M").dt.to_timestamp()
    gold["bcb_monthly"] = (
        bcb.groupby(["series_id", "series_name", "month"], as_index=False)
           .agg(avg_value=("value", "mean"), last_value=("value", "last"))
    )

    # ANP: monthly average price by UF/product
    anp = anp_silver.copy()
    anp["month"] = anp["date_ref"].dt.to_period("M").dt.to_timestamp()
    gold["anp_monthly"] = (
        anp.groupby(["uf_sigla", "product", "month"], as_index=False)
           .agg(avg_price=("price", "mean"))
    )

    return gold

