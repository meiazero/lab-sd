import os
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Configuração de estilo para gráficos bonitos
sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 8)
plt.rcParams["font.size"] = 12


def load_data(filepath):
    """
    Carrega os dados do arquivo CSV.
    """
    if not os.path.exists(filepath):
        print(f"Erro: Arquivo '{filepath}' não encontrado.")
        print(
            "Certifique-se de executar 'make run' no diretório pai para gerar o arquivo 'resultados.csv'."
        )
        sys.exit(1)

    try:
        df = pd.read_csv(filepath)
        return df
    except Exception as e:
        print(f"Erro ao ler o arquivo CSV: {e}")
        sys.exit(1)


def plot_activity_distribution(df, output_dir):
    """
    Plota a distribuição do tempo médio de atividade diária.
    Isso ajuda a entender o perfil das máquinas (ex: servidores 24/7 vs estações de trabalho).
    """
    plt.figure(figsize=(10, 6))
    sns.histplot(
        data=df,
        x="avg_hours_per_day",
        bins=30,
        kde=True,
        color="#3498db",
        edgecolor="white",
    )
    plt.title(
        "Distribuição do Tempo Médio de Atividade Diária",
        fontsize=16,
        fontweight="bold",
    )
    plt.xlabel("Média de Horas Ativas por Dia", fontsize=12)
    plt.ylabel("Contagem de Máquinas", fontsize=12)

    mean_val = df["avg_hours_per_day"].mean()
    plt.axvline(
        mean_val,
        color="red",
        linestyle="--",
        label=f"Média: {mean_val:.2f}h",
    )
    plt.legend()
    plt.tight_layout()

    output_path = os.path.join(output_dir, "distribuicao_atividade.png")
    plt.savefig(output_path, dpi=300)
    plt.close()


def plot_lifespan_vs_activity(df, output_dir):
    """
    Plota a relação entre o tempo de vida da máquina no sistema e sua atividade média.
    Ajuda a identificar se máquinas mais antigas tendem a ser mais ou menos ativas.
    """
    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        data=df,
        x="active_span_days",
        y="avg_hours_per_day",
        alpha=0.6,
        color="#2ecc71",
        s=80,
    )

    # Adiciona uma linha de tendência se houver dados suficientes
    if len(df) > 1:
        sns.regplot(
            data=df,
            x="active_span_days",
            y="avg_hours_per_day",
            scatter=False,
            color="#27ae60",
            line_kws={"linestyle": "--"},
        )

    plt.title(
        "Relação: Tempo de Vida vs. Atividade Média", fontsize=16, fontweight="bold"
    )
    plt.xlabel("Tempo de Vida no Sistema (Dias)", fontsize=12)
    plt.ylabel("Média de Horas Ativas por Dia", fontsize=12)
    plt.tight_layout()

    output_path = os.path.join(output_dir, "vida_vs_atividade.png")
    plt.savefig(output_path, dpi=300)
    plt.close()


def plot_timeline(df, output_dir):
    """
    Plota uma linha do tempo para uma amostra das máquinas, mostrando quando começaram e terminaram.
    Útil para visualizar a entrada e saída de nós no cluster.
    """
    # Ordena por data de início
    df_sorted = df.sort_values("start_epoch")

    # Limita a 50 máquinas para o gráfico não ficar ilegível
    if len(df_sorted) > 50:
        sample = df_sorted.sample(50, random_state=42).sort_values("start_epoch")
        subtitle = "(Amostra aleatória de 50 máquinas)"
    else:
        sample = df_sorted
        subtitle = ""

    plt.figure(figsize=(12, 8))

    # Converte epoch para datetime para o eixo X
    dates_start = pd.to_datetime(sample["start_epoch"], unit="s")
    dates_end = pd.to_datetime(sample["end_epoch"], unit="s")

    # Plota linhas horizontais
    plt.hlines(
        y=range(len(sample)),
        xmin=dates_start,
        xmax=dates_end,
        color="#9b59b6",
        alpha=0.8,
        linewidth=3,
    )
    plt.scatter(
        dates_start,
        range(len(sample)),
        color="#8e44ad",
        s=30,
        marker="|",
        label="Início",
    )
    plt.scatter(
        dates_end, range(len(sample)), color="#e74c3c", s=30, marker="|", label="Fim"
    )

    plt.title(
        f"Linha do Tempo de Vida das Máquinas {subtitle}",
        fontsize=16,
        fontweight="bold",
    )
    plt.xlabel("Data", fontsize=12)
    plt.ylabel("Máquinas (Ordenadas por Início)", fontsize=12)
    plt.yticks([])  # Remove labels do eixo Y pois são apenas índices
    plt.grid(axis="x", linestyle="--", alpha=0.7)
    plt.tight_layout()

    output_path = os.path.join(output_dir, "timeline_maquinas.png")
    plt.savefig(output_path, dpi=300)
    plt.close()


def main():
    # Tenta localizar o arquivo CSV em locais prováveis
    possible_paths = [
        os.path.join("..", "resultados.csv"),  # No diretório pai (padrão do Makefile)
        "resultados.csv",  # No diretório atual
        os.path.join("lab1", "resultados.csv"),  # Caso executado da raiz do projeto
    ]

    csv_path = None
    for path in possible_paths:
        if os.path.exists(path):
            csv_path = path
            break

    if csv_path is None:
        # Se não encontrar, usa o padrão para exibir a mensagem de erro correta
        csv_path = os.path.join("..", "resultados.csv")

    print(f"Lendo dados de: {csv_path}")
    df = load_data(csv_path)

    if df.empty:
        print("O arquivo CSV está vazio.")
        return

    print(f"Dados carregados: {len(df)} registros.")

    # Exibe uma prévia dos dados mais ativos
    print("\nTop 5 máquinas com maior tempo médio de atividade:")
    print(
        df.sort_values(by="avg_hours_per_day", ascending=False).head()[
            ["node_id", "avg_hours_per_day", "active_span_days"]
        ]
    )
    print("-" * 30)

    # Define diretório de saída (diretório atual onde o script está)
    output_dir = os.path.dirname(os.path.abspath(__file__))

    # Gera os gráficos
    plot_activity_distribution(df, output_dir)
    plot_lifespan_vs_activity(df, output_dir)
    plot_timeline(df, output_dir)


if __name__ == "__main__":
    main()
