-- Criação do keyspace com replicação em 2 nós
CREATE KEYSPACE IF NOT EXISTS marketplace_ks
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 2
}
AND durable_writes = true;

-- Usar o keyspace
USE marketplace_ks;

-- Criação da tabela de transações do marketplace
CREATE TABLE IF NOT EXISTS sales_transactions (
    state text,
    category text,
    transaction_id text,
    customer_id text,
    product_id text,
    price double,
    quantity int,
    total_value double,
    purchase_date timestamp,
    city text,
    payment_method text,
    device_type text,
    rating double,
    PRIMARY KEY ((state), category, purchase_date, transaction_id)
) WITH CLUSTERING ORDER BY (category ASC, purchase_date DESC, transaction_id ASC);
