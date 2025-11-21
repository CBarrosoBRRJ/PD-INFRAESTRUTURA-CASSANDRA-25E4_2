import sys
from pathlib import Path

import pandas as pd
from cassandra.cluster import Cluster


def fetch_all_sales(session):
    """
    Lê todos os registros da tabela sales_transactions do Cassandra
    e retorna uma lista de dicionários, adequada para criar um DataFrame.
    """
    query = """
        SELECT
            state,
            category,
            transaction_id,
            product_id,
            price,
            quantity,
            total_value,
            purchase_date,
            rating
        FROM sales_transactions;
    """

    # fetch_size controla quantas linhas vêm por página
    session.default_fetch_size = 10_000

    print("[ANALYTICS] Executando SELECT completo em sales_transactions ...")
    rows = session.execute(query)

    data = []
    for i, r in enumerate(rows, start=1):
        data.append(
            {
                "state": r.state,
                "category": r.category,
                "transaction_id": r.transaction_id,
                "product_id": r.product_id,
                "price": float(r.price) if r.price is not None else None,
                "quantity": int(r.quantity) if r.quantity is not None else None,
                "total_value": float(r.total_value) if r.total_value is not None else None,
                "purchase_date": r.purchase_date,  # já vem como datetime
                "rating": float(r.rating) if r.rating is not None else None,
            }
        )
        if i % 100_000 == 0:
            print(f"[ANALYTICS] Linhas lidas: {i:,}".replace(",", "."))

    print(f"[ANALYTICS] Total de linhas lidas do Cassandra: {len(data):,}".replace(",", "."))
    return data


def main():
    # Descobre pasta raiz do projeto
    base_dir = Path(__file__).resolve().parents[1]

    processed_dir = base_dir / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------------------------
    # 1) Conectar ao Cassandra
    # -------------------------------------------------------------------------
    print("=" * 80)
    print("[ANALYTICS] Conectando ao Cassandra (localhost:9042, keyspace marketplace_ks) ...")

    cluster = Cluster(["127.0.0.1"], port=9042)
    try:
        session = cluster.connect("marketplace_ks")
    except Exception as e:
        print("[ERRO] Não foi possível conectar ao keyspace marketplace_ks:")
        print(e)
        cluster.shutdown()
        sys.exit(1)

    print("[ANALYTICS] Conexão estabelecida.")

    # -------------------------------------------------------------------------
    # 2) Ler todos os dados da tabela sales_transactions
    # -------------------------------------------------------------------------
    raw_data = fetch_all_sales(session)

    if not raw_data:
        print("[ERRO] Nenhum dado foi retornado da tabela sales_transactions.")
        cluster.shutdown()
        sys.exit(1)

    df = pd.DataFrame(raw_data)
    print("[ANALYTICS] DataFrame principal criado.")
    print("[ANALYTICS] Dimensão do DataFrame:", df.shape)
    print("[ANALYTICS] Colunas:", list(df.columns))

    # Garantir tipo datetime
    df["purchase_date"] = pd.to_datetime(df["purchase_date"])

    # -------------------------------------------------------------------------
    # 3) Agregação 1: Receita por estado e categoria
    # -------------------------------------------------------------------------
    print("-" * 80)
    print("[ANALYTICS] Calculando receita total por estado e categoria...")

    df_receita = (
        df.groupby(["state", "category"], as_index=False)["total_value"]
        .sum()
        .rename(columns={"total_value": "total_revenue"})
    )

    # Variável nova: participação da categoria no total do estado (share)
    df_receita["state_total_revenue"] = df_receita.groupby("state")["total_revenue"].transform("sum")
    df_receita["share_in_state"] = df_receita["total_revenue"] / df_receita["state_total_revenue"]

    # Ordenar para facilitar análise (por estado, depois receita desc)
    df_receita = df_receita.sort_values(["state", "total_revenue"], ascending=[True, False])

    receita_path = processed_dir / "receita_estado_categoria.csv"
    df_receita.to_csv(receita_path, index=False)

    print("[ANALYTICS] Arquivo gerado:", receita_path)
    print("[ANALYTICS] Dimensão df_receita:", df_receita.shape)
    print("[ANALYTICS] Amostra df_receita:")
    print(df_receita.head(10))

    # -------------------------------------------------------------------------
    # 4) Agregação 2: Preço médio x rating médio por produto
    # -------------------------------------------------------------------------
    print("-" * 80)
    print("[ANALYTICS] Calculando preço médio e rating médio por produto...")

    df_preco_rating = (
        df.groupby("product_id", as_index=False)
        .agg(
            avg_price=("price", "mean"),
            avg_rating=("rating", "mean"),
            num_transactions=("transaction_id", "count"),
        )
    )

    preco_rating_path = processed_dir / "preco_rating_por_produto.csv"
    df_preco_rating.to_csv(preco_rating_path, index=False)

    print("[ANALYTICS] Arquivo gerado:", preco_rating_path)
    print("[ANALYTICS] Dimensão df_preco_rating:", df_preco_rating.shape)
    print("[ANALYTICS] Amostra df_preco_rating:")
    print(df_preco_rating.head(10))

    # -------------------------------------------------------------------------
    # 5) Agregação 3: Evolução temporal das vendas (por mês)
    # -------------------------------------------------------------------------
    print("-" * 80)
    print("[ANALYTICS] Calculando evolução de vendas por mês (2019-2024)...")

    df["year_month"] = df["purchase_date"].dt.to_period("M").astype(str)

    df_vendas_mes = (
        df.groupby("year_month", as_index=False)
        .agg(
            total_revenue=("total_value", "sum"),
            num_transactions=("transaction_id", "count"),
        )
        .sort_values("year_month")
    )

    vendas_mes_path = processed_dir / "vendas_por_mes.csv"
    df_vendas_mes.to_csv(vendas_mes_path, index=False)

    print("[ANALYTICS] Arquivo gerado:", vendas_mes_path)
    print("[ANALYTICS] Dimensão df_vendas_mes:", df_vendas_mes.shape)
    print("[ANALYTICS] Amostra df_vendas_mes:")
    print(df_vendas_mes.head(12))

    # -------------------------------------------------------------------------
    # 6) Finalização
    # -------------------------------------------------------------------------
    cluster.shutdown()
    print("=" * 80)
    print("[ANALYTICS] ETL analítico concluído com sucesso.")
    print("=" * 80)


if __name__ == "__main__":
    main()
