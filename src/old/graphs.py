"""
Sensitivity analysis estesa per Parallel FP-Growth + pruning (alpha extraction).
Legge un CSV con colonne:
indice,support,heap_size,num_groups,fraction,sample_size,
patterns_count,reduced_patterns_count,pfp_time_sec,alpha_extraction_time_sec
"""

import os
from pathlib import Path
from matplotlib.colors import LogNorm
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

INPUT_PATH = Path(__file__).parent.parent / "input.csv"
OUT_DIR = Path(__file__).parent.parent / "out"
os.makedirs(OUT_DIR, exist_ok=True)

df = pd.read_csv(INPUT_PATH)

# ----------------------------------------------------------------------
# Colonne derivate
# ----------------------------------------------------------------------
df["total_time_sec"] = df["pfp_time_sec"] + df["alpha_extraction_time_sec"]
df["pruning_ratio_pct"] = (
    (df["patterns_count"] - df["reduced_patterns_count"]) / df["patterns_count"] * 100
)
df["alpha_time_share_pct"] = (
    df["alpha_extraction_time_sec"] / df["total_time_sec"] * 100
)

print(f"Righe lette: {len(df)}")
print(df.head())
print("\nValori unici per parametro:")
for col in ["support", "heap_size", "num_groups", "fraction", "sample_size"]:
    if col in df.columns:
        print(f"{col}: {sorted(df[col].unique())}")

# Valori di default per gli slice OFAT: il valore più basso/centrale disponibile
SUP_DEFAULT = sorted(df.support.unique())[0]
HEAP_DEFAULT = sorted(df.heap_size.unique())[len(df.heap_size.unique()) // 2]
GROUPS_DEFAULT = sorted(df.num_groups.unique())[len(df.num_groups.unique()) // 2]
FRACTION_DEFAULT = sorted(df.fraction.unique())[
    -1
]  # frazione più alta = dataset quasi completo


def slice_default(data: pd.DataFrame, exclude=()) -> pd.Series:
    """Filtra il dataframe sui valori di default per tutti i parametri tranne quelli in exclude."""
    defaults = {
        "support": SUP_DEFAULT,
        "heap_size": HEAP_DEFAULT,
        "num_groups": GROUPS_DEFAULT,
        "fraction": FRACTION_DEFAULT,
    }
    mask = True
    for k, v in defaults.items():
        if k in exclude or k not in data.columns:
            continue
        mask = mask & (data[k] == v)
    return data[mask]


# ----------------------------------------------------------------------
# 1. Effetto di FRACTION / SAMPLE_SIZE sul tempo di mining
# ----------------------------------------------------------------------
# fig, ax1 = plt.subplots(figsize=(7, 5))

# sub = slice_default(df, exclude=("fraction",)).sort_values("fraction")

# ax1.plot(
#     sub.sample_size,
#     sub.pfp_time_sec,
#     marker="o",
#     color="tab:blue",
#     label="tempo mining (s)",
# )
# ax1.set_xlabel("Sample size (n. record)")
# ax1.set_ylabel("Tempo mining (s)", color="tab:blue")
# ax1.tick_params(axis="y", labelcolor="tab:blue")


# # Linear line
# # ax2 = ax1.twinx()
# # LINEAR_COSTANT = (sub.pfp_time_sec / sub.sample_size).mean()
# sample_size = sub.sample_size.astype(float)
# pfp_time = sub.pfp_time_sec.astype(float)


# LINEAR_COSTANT = np.exp(np.mean(np.log(pfp_time) - 1 * np.log(sample_size)))
# ax1.plot(
#     sub.sample_size,
#     sub.sample_size * LINEAR_COSTANT,
#     marker="s",
#     color="tab:orange",
#     label="sample size",
# )
# # ax2.set_ylabel("Sample size (n. record)", color="tab:orange")
# # ax2.tick_params(axis="y", labelcolor="tab:orange")

# # Quadratic line
# # ax3 = ax1.twinx()
# # QUADRATIC_COSTANT = (sub.pfp_time_sec / sub.sample_size**2).mean()
# QUADRATIC_COSTANT = np.exp(np.mean(np.log(pfp_time) - 2 * np.log(sample_size)))
# ax1.plot(
#     sub.sample_size,
#     sub.sample_size**2 * QUADRATIC_COSTANT,
#     marker="^",
#     color="tab:green",
#     label="sample size^2",
# )

# ax1.set_title(
#     f"Effetto di fraction sul tempo (support={SUP_DEFAULT}, heap={HEAP_DEFAULT}, groups={GROUPS_DEFAULT})"
# )
# fig.tight_layout()
# fig.savefig(f"{OUT_DIR}/1_effetto_fraction.png", dpi=150)
# plt.close(fig)


# # ----------------------------------------------------------------------
# # 2. Trade-off accuratezza-velocità: fraction vs pattern trovati
# #    (verifica se campionare fa perdere pattern reali)
# # ----------------------------------------------------------------------
# fig, ax1 = plt.subplots(figsize=(7, 5))

# ax1.plot(
#     sub.fraction,
#     sub.patterns_count,
#     marker="o",
#     color="tab:green",
#     label="pattern trovati (prima del pruning)",
# )
# ax1.plot(
#     sub.fraction,
#     sub.reduced_patterns_count,
#     marker="s",
#     color="tab:red",
#     label="pattern dopo pruning",
# )

# ax1.fill_between(
#     sub.fraction,
#     sub.patterns_count,
#     sub.reduced_patterns_count,
#     color="tab:gray",
#     alpha=0.3,
#     label="pattern rimossi dal pruning",
# )
# ax1.set_yscale("log")

# # ax2 = ax1.twinx()
# # # pruning_ratio_pct è già calcolata all'inizio dello script
# # ax2.plot(
# #     sub.fraction,
# #     sub.pruning_ratio_pct,
# #     marker="^",
# #     color="tab:gray",
# # )
# # ax2.set_xlabel("Fraction (campionamento dataset)")
# # ax2.set_ylabel("% pattern rimossi dal pruning")
# # ax2.tick_params(axis="y", labelcolor="tab:gray")

# ax1.set_xlabel("Fraction (campionamento dataset)")
# ax1.set_ylabel("N. pattern")
# ax1.legend()
# ax1.set_title("Pattern trovati al variare del campionamento")
# fig.tight_layout()
# fig.savefig(f"{OUT_DIR}/2_fraction_vs_patterns.png", dpi=150)
# plt.close(fig)


# # ----------------------------------------------------------------------
# # 3. Pruning ratio in funzione di SUPPORT
# # ----------------------------------------------------------------------
# fig, ax1 = plt.subplots(figsize=(7, 5))

# sub = slice_default(df, exclude=("support",)).sort_values("support")

# ax1.plot(sub.support, sub.patterns_count, marker="o", label="prima del pruning")
# ax1.plot(sub.support, sub.reduced_patterns_count, marker="s", label="dopo il pruning")
# ax1.set_xlabel("Support")
# ax1.set_ylabel("N. pattern")
# ax1.legend(loc="upper right")

# ax2 = ax1.twinx()
# ax2.plot(
#     sub.support,
#     sub.pruning_ratio_pct,
#     color="gray",
#     linestyle="--",
#     marker="^",
#     label="% rimossa",
# )
# ax2.set_ylabel("% pattern rimossi dal pruning")

# ax1.set_title(
#     f"Efficacia del pruning al variare di support (heap={HEAP_DEFAULT}, groups={GROUPS_DEFAULT}, fraction={FRACTION_DEFAULT})"
# )
# fig.tight_layout()
# fig.savefig(f"{OUT_DIR}/3_pruning_vs_support.png", dpi=150)
# plt.close(fig)


# # ----------------------------------------------------------------------
# # 4. Pruning ratio in funzione di NUM_GROUPS
# #    (verifica se il parallelismo frammenta i pattern e riduce l'efficacia)
# # ----------------------------------------------------------------------
# fig, ax = plt.subplots(figsize=(7, 5))

# sub = slice_default(df, exclude=("num_groups",)).sort_values("num_groups")
# ax.plot(sub.num_groups, sub.pruning_ratio_pct, marker="o", color="tab:purple")
# ax.set_xlabel("Num Groups")
# ax.set_ylabel("% pattern rimossi dal pruning")
# ax.set_title("Efficacia del pruning al variare del parallelismo")
# fig.tight_layout()
# fig.savefig(f"{OUT_DIR}/4_pruning_vs_num_groups.png", dpi=150)
# plt.close(fig)


# # ----------------------------------------------------------------------
# # 5. Overhead del pruning: quota % di tempo speso in alpha extraction
# # ----------------------------------------------------------------------
# fig, ax = plt.subplots(figsize=(8, 5))

# sub = df.groupby("support", as_index=False)["alpha_time_share_pct"].mean()
# ax.bar(sub.support.astype(str), sub.alpha_time_share_pct, color="tab:orange")
# ax.set_xlabel("Support")
# ax.set_ylabel("% tempo totale speso nel pruning (media)")
# ax.set_title("Overhead della fase di pruning (alpha extraction)")
# fig.tight_layout()
# fig.savefig(f"{OUT_DIR}/5_pruning_overhead.png", dpi=150)
# plt.close(fig)


# ----------------------------------------------------------------------
# 6. Scalabilità del pruning: alpha_extraction_time_sec vs patterns_count
#    (log-log: una pendenza ~2 indica costo quadratico)
# ----------------------------------------------------------------------
# fig, ax = plt.subplots(figsize=(7, 5))

# ax.scatter(
#     df.patterns_count,
#     df.alpha_extraction_time_sec,
#     alpha=0.6,
#     edgecolor="k",
#     linewidth=0.3,
# )

# ax2 = ax.twinx()
# ax3 = ax.twinx()
# Fit line in log-log space
# log_x = np.log(df.patterns_count)
# log_y = np.log(df.alpha_extraction_time_sec)
# ax2.plot(
#     df.patterns_count,
#     log_x,
#     color="tab:red",
#     linestyle="--",
# )
# ax3.plot(
#     df.alpha_extraction_time_sec,
#     log_y,
#     color="tab:green",
# )
# ax2.scatter(
#     df.patterns_count,
#     df.patterns_count.astype(float) ** 3,
#     color="tab:green",
#     linestyle="--",
#     label="costo teorico O(n^3)",
# )

# coeffs = np.polyfit(log_x, log_y, 1)
# slope = coeffs[0]
# intercept = coeffs[1]
# x_fit = np.linspace(df.patterns_count.min(), df.patterns_count.max(), 100)
# y_fit = np.exp(intercept) * x_fit**slope
# ax2.plot(
#     x_fit, y_fit, color="tab:red", linestyle="--", label=f"fit line (slope={slope:.2f})"
# )
# ax2.set_ylabel("Fit line (tempo previsto dal fit, s)", color="tab:red")
# ax2.tick_params(axis="y", labelcolor="tab:red")

# ax.set_xlabel("N. pattern prima del pruning")
# ax.set_ylabel("Tempo pruning (s)")
# ax.set_xscale("log")
# ax.set_yscale("log")
# ax.set_title("Scalabilità del pruning rispetto al numero di pattern")
# fig.tight_layout()
# fig.savefig(f"{OUT_DIR}/6_scalabilita_pruning.png", dpi=150)
# plt.close(fig)

# print((df.alpha_extraction_time_sec / df.patterns_count**3).describe())

# ----------------------------------------------------------------------
# 7. Heatmap tempo totale: NUM_GROUPS x HEAP_SIZE (come nello script precedente)
# ----------------------------------------------------------------------
supports = (10, 40, 65, 95)
data = df[df.fraction == FRACTION_DEFAULT]

# griglia: 4 righe x 5 colonne per 20 pannelli
ncols = 4
nrows = int(np.ceil(len(supports) / ncols))

# scala colore condivisa (LogNorm se il range è ampio, altrimenti Normalize)
vmin, vmax = data.total_time_sec.min(), data.total_time_sec.max()
norm = LogNorm(vmin, vmax)  # oppure: Normalize(vmin, vmax)
cmap = plt.get_cmap("viridis")

ANNOT = True  # True solo se i pannelli sono abbastanza grandi

fig, axes = plt.subplots(
    nrows,
    ncols,
    figsize=(3.4 * ncols, 3.2 * nrows),
    sharex=True,
    sharey=True,
    constrained_layout=True,
)
axes = np.atleast_2d(axes)

for ax, sup in zip(axes.flat, supports):
    pivot = (
        data[data.support == sup]
        .pivot_table(
            index="heap_size",
            columns="num_groups",
            values="total_time_sec",
            aggfunc="mean",
        )
        .sort_index(ascending=False)
    )

    im = ax.imshow(pivot.values, cmap=cmap, norm=norm, aspect="auto")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, fontsize=6, rotation=45)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontsize=6)
    ax.set_title(f"support={sup}", fontsize=9)

    if ANNOT:
        for i in range(pivot.shape[0]):
            for j in range(pivot.shape[1]):
                val = pivot.values[i, j]
                # colore testo in base alla scala GLOBALE
                bright = cmap(norm(val))[:3]
                lum = 0.299 * bright[0] + 0.587 * bright[1] + 0.114 * bright[2]
                ax.text(
                    j,
                    i,
                    f"{val:.1f}",
                    ha="center",
                    va="center",
                    color="black" if lum > 0.5 else "white",
                    fontsize=5,
                )

# nascondi i pannelli inutilizzati
for ax in axes.flat[len(supports) :]:
    ax.axis("off")

fig.supxlabel("Num Groups")
fig.supylabel("Heap Size (Number of items)")
fig.suptitle(f"Tempo totale (mining+pruning, s) — fraction={FRACTION_DEFAULT}")
fig.colorbar(im, ax=axes, shrink=0.6, label="Total Time (s)")

fig.savefig(f"{OUT_DIR}/7_heatmap_groups_heap.png", dpi=150)
plt.close(fig)

print("\nGrafici salvati in", OUT_DIR)
