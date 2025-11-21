# Infraestrutura de Dados com Apache Cassandra — Marketplace de Varejo

Projeto de Disciplina da matéria **Banco de Dados Não Relacionais — Apache Cassandra** (INFNET).

Implementação de uma **infraestrutura de dados em Apache Cassandra**, com **ETL em Python**, **Docker**, modelagem orientada a consultas e visualizações de dados para responder perguntas de negócio de um marketplace de varejo.

---

## 1. Objetivo

Construir uma infraestrutura Cassandra completa para:

- armazenar 1 milhão de transações simuladas de um marketplace;
- realizar ETL (extração, transformação e carga) em Python;
- suportar consultas analíticas;
- gerar visualizações que respondem a perguntas de negócio.

---

## 2. Tecnologias

- **Apache Cassandra 4.x**
- **Docker & Docker Compose**
- **Python 3.10+**
- Bibliotecas:  
  `pandas`, `pyarrow`, `matplotlib`, `cassandra-driver`
- Relatório final em **HTML/CSS**

---

## 3. Estrutura do Projeto

```text
assets/
  img/
    Infnet-Logo.png
  evidencias/
    dataset_sintetico.png
    etl_analysis.png

data/
  raw/
    marketplace_sample_30.csv
    marketplace_bigdata_1M.parquet
  processed/
    vendas_por_mes.csv
    receita_estado_categoria.csv
    preco_rating_por_produto.csv

docker/
  docker-compose.yml
  marketplace_schema.cql

img/
  grafico_linha_vendas_tempo.png
  grafico_barras_categoria_estado.png
  grafico_dispersao_preco_rating.png

src/
  dataset_sintetico.py
  etl_analysis.py
  etl_cassandra.py
  plots_marketplace.py

index.html          # Relatório final
README.md           # Este arquivo
```

---

## 4. Como Rodar

### 4.1 Criar ambiente virtual

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 4.2 Instalar dependências

```powershell
pip install pandas pyarrow matplotlib cassandra-driver
```

### 4.3 Subir o cluster Cassandra

```powershell
cd docker
docker compose up -d
docker ps
```

### 4.4 Rodar ETL de análise local

```powershell
python .\src\etl_analysis.py
```

### 4.5 Carregar dados no Cassandra

```powershell
python .\src\etl_cassandra.py
```

### 4.6 Gerar gráficos

```powershell
python .\src\plots_marketplace.py
```

---

## 5. Consultas de Validação

### Via CQL

```sql
USE marketplace_ks;
SELECT COUNT(*) FROM sales_transactions;
```

### Via Python

```python
from cassandra.cluster import Cluster
cluster = Cluster(['localhost'])
session = cluster.connect('marketplace_ks')
rows = session.execute("SELECT * FROM sales_by_month LIMIT 10")
for r in rows:
    print(r)
```

---

## 6. Relatório

O relatório oficial do trabalho está em:

```
index.html
```

Abra o arquivo no navegador e use **Imprimir → Salvar como PDF** para gerar a entrega.

---

## 7. Autor

**Aluno:** Caio Barroso  
**Disciplina:** Infraestrutura Cassandra (25E4_2)

---

