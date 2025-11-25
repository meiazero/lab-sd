import os
import sys

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans

# Configuração de estilo para gráficos bonitos e profissionais
sns.set_theme(
    style="whitegrid", context="talk"
)  # Contexto 'talk' aumenta fontes para apresentações
plt.rcParams["figure.figsize"] = (12, 8)


def load_and_process_data(filepath):
    """
    Carrega os dados e adiciona métricas derivadas para análise estatística.
    """
    if not os.path.exists(filepath):
        print(f"Erro: Arquivo '{filepath}' não encontrado.")
        print(
            "Certifique-se de executar 'make run' no diretório pai para gerar o arquivo 'resultados.csv'."
        )
        sys.exit(1)

    try:
        df = pd.read_csv(filepath)

        # 1. Normalização: Taxa de Utilização (0-100%)
        # Transforma horas/dia em uma porcentagem de uso da capacidade total (24h)
        df["utilization_pct"] = (df["avg_hours_per_day"] / 24.0) * 100

        # 2. Categorização: Quartis de Tempo de Vida
        # Agrupa as máquinas em 4 categorias baseadas no tempo de vida (quartis)
        # Isso permite comparar o comportamento de máquinas "jovens" vs "velhas"
        if len(df) >= 4:
            df["lifespan_quartile"] = pd.qcut(
                df["active_span_days"],
                q=4,
                labels=["Q1 (Curto)", "Q2 (Médio)", "Q3 (Longo)", "Q4 (Muito Longo)"],
                retbins=False,
            )
        else:
            df["lifespan_quartile"] = "Geral"

        return df
    except Exception as e:
        print(f"Erro ao processar dados: {e}")
        sys.exit(1)


def plot_utilization_distribution(df, output_dir):
    """
    Plota a distribuição da Taxa de Utilização (%) com KDE e ECDF.
    A ECDF (Função de Distribuição Acumulada Empírica) é excelente para responder:
    "Qual porcentagem das máquinas tem utilização abaixo de X%?"
    """
    fig, ax = plt.subplots(1, 2, figsize=(16, 6))

    # Histograma + KDE
    sns.histplot(
        data=df, x="utilization_pct", bins=30, kde=True, color="#3498db", ax=ax[0]
    )
    ax[0].set_title("Distribuição da Taxa de Utilização")
    ax[0].set_xlabel("Utilização (%)")
    ax[0].set_ylabel("Frequência")

    # ECDF
    sns.ecdfplot(data=df, x="utilization_pct", color="#e74c3c", linewidth=3, ax=ax[1])
    ax[1].set_title("Probabilidade Acumulada (ECDF)")
    ax[1].set_xlabel("Utilização (%)")
    ax[1].set_ylabel("Proporção de Máquinas")
    ax[1].grid(True, which="both", linestyle="--", alpha=0.5)

    # Linhas de referência (ex: mediana)
    median_util = df["utilization_pct"].median()
    ax[1].axvline(
        median_util, color="black", linestyle=":", label=f"Mediana: {median_util:.1f}%"
    )
    ax[1].legend()

    plt.tight_layout()
    save_plot(output_dir, "distribuicao_utilizacao_ecdf.png")


def plot_utilization_by_lifespan_boxplot(df, output_dir):
    """
    Boxplot comparando a utilização entre diferentes grupos de tempo de vida.
    Isso agrega valor estatístico ao mostrar a variância e outliers dentro de cada grupo.
    """
    plt.figure(figsize=(12, 8))

    sns.boxplot(data=df, x="lifespan_quartile", y="utilization_pct", palette="viridis")
    sns.stripplot(
        data=df,
        x="lifespan_quartile",
        y="utilization_pct",
        color=".3",
        alpha=0.4,
        size=3,
    )

    plt.title("Utilização da Máquina por Categoria de Tempo de Vida", fontweight="bold")
    plt.xlabel("Quartil de Tempo de Vida (Dias)")
    plt.ylabel("Taxa de Utilização (%)")

    save_plot(output_dir, "boxplot_utilizacao_por_vida.png")


def plot_joint_analysis(df, output_dir):
    """
    Joint Plot (Scatter + Histogramas Marginais) para correlacionar Vida vs Utilização.
    Usa hexbin ou kde para lidar melhor com sobreposição de pontos se houver muitos dados.
    """
    # Usando 'reg' para adicionar uma linha de regressão linear e ver a tendência
    g = sns.jointplot(
        data=df,
        x="active_span_days",
        y="utilization_pct",
        kind="reg",
        truncate=False,
        color="#2ecc71",
        height=10,
        scatter_kws={"alpha": 0.5, "s": 30},
    )

    g.fig.suptitle(
        "Correlação: Tempo de Vida vs. Taxa de Utilização", y=1.02, fontweight="bold"
    )
    g.set_axis_labels("Tempo de Vida (Dias)", "Taxa de Utilização (%)")

    save_plot(output_dir, "jointplot_correlacao.png")


def analyze_pca_and_clusters(df, output_dir):
    """
    Realiza análise multivariada avançada: PCA + K-Means + Autovalores/Autovetores.
    """
    print("\n--- Iniciando Análise Multivariada (PCA + Clustering) ---")

    # 1. Preparação das Features
    min_start = df["start_epoch"].min()
    df["start_day_offset"] = (df["start_epoch"] - min_start) / 86400.0

    features = ["active_span_days", "utilization_pct", "start_day_offset"]
    feature_names = ["Tempo de Vida", "Utilização (%)", "Início (Dia)"]

    X = df[features].dropna()

    if len(X) < 5:
        print("Dados insuficientes para PCA e Clustering.")
        return

    # 2. Padronização
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # 3. PCA
    pca = PCA(n_components=2)
    X_pca = pca.fit_transform(X_scaled)

    explained_variance = pca.explained_variance_ratio_
    eigenvalues = pca.explained_variance_
    components = pca.components_

    print(
        f"Variância Explicada: PC1={explained_variance[0]*100:.1f}%, PC2={explained_variance[1]*100:.1f}%"
    )
    print(f"Autovalores: {eigenvalues}")

    # 4. Clustering (K-Means)
    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(X_scaled)

    # 5. Plotagem do Biplot
    plt.figure(figsize=(14, 10))

    # Scatter plot
    sns.scatterplot(
        x=X_pca[:, 0],
        y=X_pca[:, 1],
        hue=clusters,
        palette="viridis",
        s=100,
        alpha=0.7,
        edgecolor="k",
        legend="full",
    )

    # Vetores (Loadings)
    scale_factor = np.max(np.abs(X_pca)) * 0.8
    for i, (feat, x, y) in enumerate(zip(feature_names, components[0], components[1])):
        plt.arrow(
            0,
            0,
            x * scale_factor,
            y * scale_factor,
            color="red",
            alpha=0.8,
            head_width=0.05 * scale_factor,
            linewidth=2,
        )
        plt.text(
            x * scale_factor * 1.15,
            y * scale_factor * 1.15,
            feat,
            color="darkred",
            ha="center",
            va="center",
            fontweight="bold",
            fontsize=12,
        )

    plt.title(
        f"PCA Biplot + K-Means Clustering\n(Variância Total: {sum(explained_variance)*100:.1f}%)",
        fontweight="bold",
    )
    plt.xlabel(f"PC1 ({explained_variance[0]*100:.1f}%)")
    plt.ylabel(f"PC2 ({explained_variance[1]*100:.1f}%)")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.legend(title="Cluster", loc="upper right")

    save_plot(output_dir, "pca_clustering_analysis.png")

    # Salva estatísticas
    with open(os.path.join(output_dir, "pca_stats.txt"), "w") as f:
        f.write("--- Estatísticas PCA ---\n")
        f.write(f"Autovalores: {eigenvalues}\n")
        f.write(f"Variância Explicada: {explained_variance}\n")
        f.write("\nAutovetores (Cargas):\n")
        for i, pc in enumerate(["PC1", "PC2"]):
            f.write(f"{pc}: {dict(zip(feature_names, components[i]))}\n")


def save_plot(output_dir, filename):
    path = os.path.join(output_dir, filename)
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()


def main():
    # Tenta localizar o arquivo CSV em locais prováveis
    possible_paths = [
        "results-2.csv",  # No diretório atual
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
    df = load_and_process_data(csv_path)

    if df.empty:
        print("O arquivo CSV está vazio.")
        return

    print(f"Dados carregados: {len(df)} registros.")

    # Exibe uma prévia dos dados mais ativos
    print("\nTop 5 máquinas com maior taxa de utilização:")
    print(
        df.sort_values(by="utilization_pct", ascending=False).head()[
            ["node_id", "utilization_pct", "active_span_days"]
        ]
    )

    # Define diretório de saída (diretório atual onde o script está)
    output_dir = os.path.dirname(os.path.abspath(__file__))

    # Gera os gráficos
    plot_utilization_distribution(df, output_dir)
    plot_utilization_by_lifespan_boxplot(df, output_dir)
    plot_joint_analysis(df, output_dir)
    analyze_pca_and_clusters(df, output_dir)


if __name__ == "__main__":
    main()
