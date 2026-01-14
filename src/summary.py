from __future__ import annotations

import pandas as pd


def build_summary_text(bcb_silver: pd.DataFrame, anp_silver: pd.DataFrame) -> str:
    lines: list[str] = []

    # -------------------------------
    # BCB/SGS: latest value + MoM
    # -------------------------------
    target_name = "selic_sgs_11"  # deve bater com inputs/bcb_series.csv (case-insensitive)

    if bcb_silver is not None and not bcb_silver.empty:
        serie = (
            bcb_silver[bcb_silver["series_name"].astype(str).str.lower().eq(target_name)]
            .sort_values("date")
        )

        if not serie.empty:
            latest = serie.iloc[-1]
            series_id = int(latest["series_id"])
            series_name = str(latest["series_name"])

            lines.append(
                f"BCB/SGS (série {series_id}) - {series_name}: último valor em {latest['date'].date()} = {latest['value']:.2f}."
            )

            # month-over-month usando o último valor de cada mês
            serie_m = serie.copy()
            serie_m["month"] = serie_m["date"].dt.to_period("M").dt.to_timestamp()

            last_by_month = (
                serie_m.sort_values(["month", "date"])
                .groupby("month", as_index=False)
                .tail(1)
                .sort_values("month")
            )

            if len(last_by_month) >= 2:
                v1 = float(last_by_month.iloc[-2]["value"])
                v2 = float(last_by_month.iloc[-1]["value"])
                delta = v2 - v1
                lines.append(f"Variação vs mês anterior: {delta:+.2f} (variação absoluta).")
        else:
            lines.append(f"BCB/SGS - série '{target_name}' não encontrada no período.")
    else:
        lines.append("BCB/SGS - sem dados para o período.")

    # -------------------------------
    # ANP: top MoM increases by UF/product
    # -------------------------------
    if anp_silver is not None and not anp_silver.empty:
        anp = anp_silver.copy()

        # Garante datetime
        anp["date_ref"] = pd.to_datetime(anp["date_ref"], errors="coerce")
        anp = anp.dropna(subset=["date_ref"])

        if not anp.empty:
            anp["month"] = anp["date_ref"].dt.to_period("M").dt.to_timestamp()

            # média mensal por UF/produto
            m = (
                anp.groupby(["uf_sigla", "product", "month"], as_index=False)
                .agg(avg_price=("price", "mean"))
            )

            # MoM por UF/produto
            m = m.sort_values(["uf_sigla", "product", "month"])
            m["mom_change"] = m.groupby(["uf_sigla", "product"])["avg_price"].diff()

            latest_month = m["month"].max()
            m_latest = m[m["month"] == latest_month].dropna(subset=["mom_change"])

            if not m_latest.empty:
                top = m_latest.sort_values("mom_change", ascending=False).head(3)
                lines.append(f"ANP - Destaques de {latest_month.date()}:")
                for _, r in top.iterrows():
                    lines.append(
                        f"- {r['uf_sigla']} / {r['product']}: variação média {r['mom_change']:+.2f} (vs mês anterior)."
                    )
            else:
                lines.append("ANP - Sem variação mensal suficiente para destacar no período.")
        else:
            lines.append("ANP - Sem dados válidos para o período.")
    else:
        lines.append("ANP - sem dados para o período.")

    # -------------------------------
    # Final: sempre retorna string
    # -------------------------------
    text = "\n".join(lines).strip()
    return text if text else "Resumo indisponível: não houve dados suficientes após o ETL."
