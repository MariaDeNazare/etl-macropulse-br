from __future__ import annotations

import pandas as pd


def build_summary_text(bcb_silver: pd.DataFrame, anp_silver: pd.DataFrame) -> str:
    lines = []

    # BCB: latest value and MoM change (if possible)
    selic = bcb_silver[bcb_silver["series_name"].str.lower().eq("selic")].sort_values("date")
    if not selic.empty:
        latest = selic.iloc[-1]
        lines.append(f"BCB/SGS - Selic: último valor em {latest['date'].date()} = {latest['value']:.2f}.")

        # month-over-month using monthly last values
        selic_m = selic.copy()
        selic_m["month"] = selic_m["date"].dt.to_period("M").dt.to_timestamp()
        last_by_month = selic_m.groupby("month", as_index=False).tail(1).sort_values("month")
        if len(last_by_month) >= 2:
            v1 = float(last_by_month.iloc[-2]["value"])
            v2 = float(last_by_month.iloc[-1]["value"])
            delta = v2 - v1
            lines.append(f"Variação vs mês anterior: {delta:+.2f} p.p.")

    # ANP: top increases by UF/product (simple insight)
    if not anp_silver.empty:
        anp = anp_silver.copy()
        anp["month"] = anp["date_ref"].dt.to_period("M").dt.to_timestamp()
        m = anp.groupby(["uf_sigla", "product", "month"], as_index=False).agg(avg_price=("price", "mean"))
        # compute MoM by UF/product
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

    if not lines:
        return "Resumo indisponível: não houve dados suficientes após o ETL."
    return "\n".join(lines)
