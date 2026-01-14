import duckdb

DB_PATH = "data/macropulse.duckdb" 

def main():
    con = duckdb.connect(DB_PATH)

    print("Tabelas disponíveis:")
    print(con.execute("SHOW TABLES;").df())

    print("\nAmostra - silver_bcb_sgs (últimas 10 linhas):")
    print(con.execute("""
        SELECT series_id, series_name, date, value
        FROM silver_bcb_sgs
        ORDER BY date DESC
        LIMIT 10;
    """).df())

    print("\nAmostra - gold_anp_monthly (últimos 10 registros):")
    print(con.execute("""
        SELECT uf_sigla, product, month, avg_price
        FROM gold_anp_monthly
        ORDER BY month DESC
        LIMIT 10;
    """).df())

    con.close()

if __name__ == "__main__":
    main()
