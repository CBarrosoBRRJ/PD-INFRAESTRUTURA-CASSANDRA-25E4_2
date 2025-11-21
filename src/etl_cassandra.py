import sys
from pathlib import Path
from itertools import islice

import pandas as pd
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args


def batched(iterable, n):
    """
    Quebra um iterável em blocos de tamanho n.
    Ex.: batched(range(10), 3) -> [0,1,2], [3,4,5], [6,7,8], [9]
    """
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch


def main():
    # Descobre a pasta raiz do projeto a partir deste arquivo
    base_dir = Path(__file__).resolve().parents[1]

    # Caminho do arquivo Parquet com 1M de registros
    data_path = base_dir / "data" / "raw" / "marketplace_bigdata_1M.parquet"

    print("=" * 80)
    print(f"[ETL] Iniciando carga para o Cassandra")
    print(f"[ETL] Arquivo de entrada: {data_path}")

    if not data_path.exists():
        print(f"[ERRO] Arquivo não encontrado: {data_path}")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # 1) Leitura do arquivo Parquet
    # -------------------------------------------------------------------------
    print("[ETL] Lendo arquivo Parquet...")
    df = pd.read_parquet(data_path)

    print(f"[ETL] Registros carregados do arquivo: {len(df):,}".replace(",", "."))
    print("[ETL] Colunas disponíveis:", list(df.columns))

    # Garantir tipos corretos
    df["purchase_date"] = pd.to_datetime(df["purchase_date"])

    # -------------------------------------------------------------------------
    # 2) Conexão com Cassandra
    # -------------------------------------------------------------------------
    print("[ETL] Conectando ao cluster Cassandra em localhost:9042 ...")
    cluster = Cluster(["127.0.0.1"], port=9042)

    try:
        session = cluster.connect("marketplace_ks")
    except Exception as e:
        print("[ERRO] Não foi possível conectar ao keyspace marketplace_ks:")
        print(e)
        cluster.shutdown()
        sys.exit(1)

    print("[ETL] Conexão estabelecida com sucesso.")

    # -------------------------------------------------------------------------
    # 3) Preparar statement de INSERT
    # -------------------------------------------------------------------------
    insert_cql = """
        INSERT INTO sales_transactions (
            state,
            category,
            transaction_id,
            customer_id,
            product_id,
            price,
            quantity,
            total_value,
            purchase_date,
            city,
            payment_method,
            device_type,
            rating
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    prepared = session.prepare(insert_cql)
    print("[ETL] Statement de INSERT preparado.")

    # -------------------------------------------------------------------------
    # 4) Montar os dados como lista de tuplas
    # -------------------------------------------------------------------------
    print("[ETL] Montando tuplas para inserção...")

    def row_generator():
        for row in df.itertuples(index=False):
            yield (
                str(row.state),
                str(row.category),
                str(row.transaction_id),
                str(row.customer_id),
                str(row.product_id),
                float(row.price),
                int(row.quantity),
                float(row.total_value),
                row.purchase_date.to_pydatetime(),  # datetime nativo
                str(row.city),
                str(row.payment_method),
                str(row.device_type),
                float(row.rating),
            )

    total_inseridos = 0
    batch_size = 10_000     # quantidade de linhas por lote
    concurrency = 100       # número de requisições concorrentes por lote

    print(f"[ETL] Iniciando inserção em lotes de {batch_size} registros "
          f"(concorrência={concurrency})...")

    for batch_num, batch in enumerate(batched(row_generator(), batch_size), start=1):
        # execute_concurrent_with_args retorna lista de (success, result/exception)
        results = execute_concurrent_with_args(
            session,
            prepared,
            batch,
            concurrency=concurrency,
            raise_on_first_error=False,
        )

        # Checar rapidamente se houve algum erro grave no lote
        erros = [r for r in results if not r[0]]
        if erros:
            print(f"[ALERTA] Foram encontrados {len(erros)} erros no lote {batch_num}. "
                  f"Exemplo do primeiro erro: {erros[0][1]}")
            # Para um TCC, logar o erro já é suficiente; não vamos abortar tudo.

        total_inseridos += len(batch)
        print(f"[ETL] Lote {batch_num} inserido. Total acumulado: {total_inseridos:,}".replace(",", "."))

    print("=" * 80)
    print(f"[ETL] Carga concluída. Total final inserido (estimado): {total_inseridos:,}".replace(",", "."))

    # -------------------------------------------------------------------------
    # 5) Validação: SELECT LIMIT 5
    # -------------------------------------------------------------------------
    print("[ETL] Executando SELECT de validação (LIMIT 5)...")

    rows = session.execute(
        """
        SELECT state, category, transaction_id, total_value, purchase_date
        FROM sales_transactions
        LIMIT 5;
        """
    )

    print("[ETL] Exemplos de registros gravados:")
    for r in rows:
        print(f"  state={r.state}, category={r.category}, "
              f"transaction_id={r.transaction_id}, total_value={r.total_value}, "
              f"purchase_date={r.purchase_date}")

    print("[ETL] ETL finalizado com sucesso.")
    print("=" * 80)

    cluster.shutdown()


if __name__ == "__main__":
    main()
