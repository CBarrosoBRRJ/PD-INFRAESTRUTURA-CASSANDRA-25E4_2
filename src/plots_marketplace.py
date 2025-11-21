import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.image as mpimg


def add_logo_header(fig, axis_position, logo_path):
    """
    Adiciona o logo no topo à esquerda, ocupando uma faixa horizontal acima do gráfico.
    """
    try:
        logo = mpimg.imread(logo_path)
        ax_logo = fig.add_axes(axis_position)  # left, bottom, width, height
        ax_logo.imshow(logo)
        ax_logo.axis("off")
    except Exception as e:
        print(f"[PLOTS] Erro ao carregar logo: {e}")


def add_footer(fig, text):
    """
    Insere o rodapé na parte inferior do gráfico.
    """
    fig.text(
        0.5, -0.08,
        text,
        ha='center',
        fontsize=10,
        color='#444444'
    )


def main():
    base_dir = Path(__file__).resolve().parents[1]

    processed_dir = base_dir / "data" / "processed"
    img_dir = base_dir / "img"
    img_dir.mkdir(parents=True, exist_ok=True)

    logo_path = base_dir / "assets" / "img" / "Infnet-Logo.png"

    # -------------------------------------------------------------------------
    # 1) Gráfico de Barras – Top 10 categorias do estado SP
    # -------------------------------------------------------------------------
    print("[PLOTS] Gráfico de barras...")

    df_receita = pd.read_csv(processed_dir / "receita_estado_categoria.csv")
    df_sp = df_receita[df_receita["state"] == "SP"].copy()
    df_sp_top10 = df_sp.sort_values("total_revenue", ascending=False).head(10)

    fig = plt.figure(figsize=(14, 8))

    # Logo no topo ESQUERDO — ocupa 15% da altura do canvas
    add_logo_header(
        fig,
        axis_position=[0.02, 0.82, 0.18, 0.16],  # left, bottom, width, height
        logo_path=logo_path
    )

    # Área do gráfico (abaixo do logo)
    ax = fig.add_axes([0.07, 0.12, 0.90, 0.68])
    ax.bar(
        df_sp_top10["category"],
        df_sp_top10["total_revenue"],
        color="#003366"
    )
    ax.set_title("Top 10 Categorias por Receita — Estado de SP", fontsize=14)
    ax.set_ylabel("Receita Total (R$)")
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    add_footer(
        fig,
        "Este gráfico apresenta as 10 categorias com maior receita no estado de SP.\n"
        "Projeto de Pós-Graduação — Engenharia de Dados — INFNET"
    )

    out1 = img_dir / "grafico_barras_categoria_estado.png"
    fig.savefig(out1, dpi=300, bbox_inches="tight")
    plt.close()
    print("[PLOTS] →", out1)

    # -------------------------------------------------------------------------
    # 2) Gráfico de Dispersão – Preço médio × Rating médio
    # -------------------------------------------------------------------------
    print("[PLOTS] Gráfico de dispersão...")

    df_preco_rating = pd.read_csv(processed_dir / "preco_rating_por_produto.csv")

    fig = plt.figure(figsize=(14, 8))

    add_logo_header(
        fig,
        axis_position=[0.02, 0.82, 0.18, 0.16],
        logo_path=logo_path
    )

    ax = fig.add_axes([0.07, 0.12, 0.90, 0.68])
    ax.scatter(
        df_preco_rating["avg_price"],
        df_preco_rating["avg_rating"],
        color="#003366",
        s=16,
        alpha=0.5
    )
    ax.set_xlabel("Preço Médio (R$)")
    ax.set_ylabel("Rating Médio")
    ax.set_title("Dispersão: Preço Médio × Rating Médio por Produto")

    add_footer(
        fig,
        "Cada ponto representa um produto. Preço médio no eixo X e rating médio no eixo Y.\n"
        "Projeto de Pós-Graduação — Engenharia de Dados — INFNET"
    )

    out2 = img_dir / "grafico_dispersao_preco_rating.png"
    fig.savefig(out2, dpi=300, bbox_inches="tight")
    plt.close()
    print("[PLOTS] →", out2)

    # -------------------------------------------------------------------------
    # 3) Gráfico de Linha – Evolução temporal
    # -------------------------------------------------------------------------
    print("[PLOTS] Gráfico de linha...")

    df_vendas = pd.read_csv(processed_dir / "vendas_por_mes.csv")

    fig = plt.figure(figsize=(15, 8))

    add_logo_header(
        fig,
        axis_position=[0.02, 0.82, 0.18, 0.16],
        logo_path=logo_path
    )

    ax = fig.add_axes([0.07, 0.12, 0.90, 0.68])
    ax.plot(
        df_vendas["year_month"],
        df_vendas["total_revenue"],
        color="#003366",
        linewidth=2
    )
    plt.setp(ax.get_xticklabels(), rotation=90)
    ax.set_title("Evolução da Receita Mensal — 2019 a 2024", fontsize=14)
    ax.set_xlabel("Ano-Mês")
    ax.set_ylabel("Receita Total (R$)")

    add_footer(
        fig,
        "Receita agregada mensalmente no período de 2019 a 2024.\n"
        "Projeto de Pós-Graduação — Engenharia de Dados — INFNET"
    )

    out3 = img_dir / "grafico_linha_vendas_tempo.png"
    fig.savefig(out3, dpi=300, bbox_inches="tight")
    plt.close()
    print("[PLOTS] →", out3)

    print("\n[PLOTS] Todos os gráficos foram atualizados com layout profissional.")


if __name__ == "__main__":
    main()
