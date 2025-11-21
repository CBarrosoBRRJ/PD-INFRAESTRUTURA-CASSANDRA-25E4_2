import numpy as np
import pandas as pd

# -----------------------------
# Configurações
# -----------------------------
np.random.seed(42)
N_ROWS = 1_000_000 

# -----------------------------
# Valores possíveis
# -----------------------------
categories = [
    "Eletrônicos", "Casa & Cozinha", "Moda", "Livros", "Esportes",
    "Beleza", "Brinquedos", "Pet Shop", "Mercado", "Informática",
    "Móveis", "Automotivo"
]

states = [
    "SP", "RJ", "MG", "ES", "PR", "SC", "RS", "BA", "PE", "CE",
    "DF", "GO", "MT", "MS", "AM", "PA"
]

cities = [
    "São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba", "Porto Alegre",
    "Salvador", "Fortaleza", "Recife", "Brasília", "Campinas",
    "Niterói", "Santos", "Florianópolis", "Vitória", "Goiania", "Manaus"
]

payment_methods = ["cartao_credito", "pix", "boleto", "carteira_digital"]
device_types = ["desktop", "mobile", "tablet"]

# Datas entre 2019-01-01 e 2024-12-31
start_date = np.datetime64("2019-01-01")
end_date = np.datetime64("2024-12-31")
date_range_days = int((end_date - start_date).astype(int))

# -----------------------------
# Geração dos dados
# -----------------------------
print("Gerando dados sintéticos...")

transaction_id = [f"T{i:09d}" for i in range(1, N_ROWS + 1)]
customer_id = [f"C{np.random.randint(1, 300_000):06d}" for _ in range(N_ROWS)]
product_id = [f"P{np.random.randint(1, 50_000):07d}" for _ in range(N_ROWS)]

category = np.random.choice(
    categories,
    size=N_ROWS,
    p=[0.13, 0.08, 0.12, 0.07, 0.08, 0.07, 0.07, 0.06, 0.12, 0.08, 0.06, 0.06]
)

state = np.random.choice(states, size=N_ROWS)
city = np.random.choice(cities, size=N_ROWS)

payment_method = np.random.choice(
    payment_methods,
    size=N_ROWS,
    p=[0.45, 0.30, 0.15, 0.10]
)

device_type = np.random.choice(
    device_types,
    size=N_ROWS,
    p=[0.30, 0.60, 0.10]
)

# Regras de preço por categoria (mais realista)
base_price = {
    "Eletrônicos": (100, 4000),
    "Casa & Cozinha": (30, 800),
    "Moda": (20, 600),
    "Livros": (15, 200),
    "Esportes": (40, 1500),
    "Beleza": (10, 400),
    "Brinquedos": (20, 500),
    "Pet Shop": (10, 600),
    "Mercado": (5, 300),
    "Informática": (80, 3000),
    "Móveis": (150, 5000),
    "Automotivo": (50, 2500),
}

price = np.empty(N_ROWS)

for cat, (low, high) in base_price.items():
    mask = (category == cat)
    price[mask] = np.round(
        np.random.uniform(low, high, size=mask.sum()),
        2
    )

quantity = np.random.randint(1, 6, size=N_ROWS)
total_value = np.round(price * quantity, 2)

# Datas aleatórias (com hora/segundo)
random_days = np.random.randint(0, date_range_days + 1, size=N_ROWS)
random_seconds = np.random.randint(0, 24 * 60 * 60, size=N_ROWS)

purchase_date = (
    start_date
    + random_days.astype("timedelta64[D]")
    + random_seconds.astype("timedelta64[s]")
).astype("datetime64[ns]")

# Ratings (avaliação) com leve viés para cima
rating = np.clip(
    np.random.normal(loc=4.1, scale=0.6, size=N_ROWS),
    1.0,
    5.0
)
rating = np.round(rating, 1)

# -----------------------------
# Monta DataFrame
# -----------------------------
df = pd.DataFrame({
    "transaction_id": transaction_id,
    "customer_id": customer_id,
    "product_id": product_id,
    "category": category,
    "price": price,
    "quantity": quantity,
    "total_value": total_value,
    "purchase_date": purchase_date,
    "city": city,
    "state": state,
    "payment_method": payment_method,
    "device_type": device_type,
    "rating": rating,
})

print("Exemplo de linhas geradas:")
print(df.head())

# -----------------------------
# Salvar arquivos
# -----------------------------
print("\nSalvando marketplace_bigdata_1M.parquet ...")
try:
    df.to_parquet("marketplace_bigdata_1M.parquet", index=False)
except Exception as e:
    print("Erro ao salvar Parquet. Verifique se o 'pyarrow' está instalado.")
    print("Instale com: pip install pyarrow")
    raise e

print("Salvando marketplace_sample_30.csv ...")
sample_df = df.sample(30, random_state=42)
sample_df.to_csv("marketplace_sample_30.csv", index=False)

print("\nConcluído!")
print("Arquivos gerados:")
print(" - marketplace_bigdata_1M.parquet")
print(" - marketplace_sample_30.csv")
