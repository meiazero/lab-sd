import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

results = pd.read_csv(Path("./resultados.csv"), sep=",", index_col=False)

print(results.columns)
# Index(
#     ["node_id", "avg_hours_per_day", "active_span_days", "start_epoch", "end_epoch"],
#     dtype="object",
# )

# Organiza os dados do node_id com maior tempo de atividade em primeiro
df = results.sort_values(by="avg_hours_per_day", ascending=False)
print(df.head())
