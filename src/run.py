import csv
import itertools
import json
import os
import random
import time
from pathlib import Path
from time import perf_counter

import psutil
import pyspark as ps
from pyspark.sql import SparkSession

from alpha import extract_alpha_groups
from fpgrowth import PatternWithSupport

NUM_CORES = 16

# ============================================================
### MODIFICA QUI ### Range dei parametri da testare (8000 combinazioni)
# ============================================================
# Adatta questi range in modo che il prodotto dia circa 8000 combinazioni.
# Esempio: 20 support * 10 heap_size * 5 num_groups * 8 fractions = 8000
SUPPORTS = [i for i in range(10, 101, 5)]  # 20 valori
# SUPPORTS = [10]  # 20 valori
# HEAP_SIZES = [200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000]  # 10 valori
HEAP_SIZES = [2**i for i in range(4, 11)]  # 10 valori
NUM_GROUPS_LIST = [2**i for i in range(0, 9)]  # 5 valori
FRACTIONS = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.8, 1.0]  # 8 valori
# FRACTIONS = [1.0]  # 8 valori


def genera_parametri():
    combinazioni = []
    for support, heap_size, num_groups, fraction in itertools.product(
        SUPPORTS, HEAP_SIZES, NUM_GROUPS_LIST, FRACTIONS
    ):
        combinazioni.append(
            {
                "support": support,
                "heap_size": heap_size,
                "num_groups": num_groups,
                "fraction": fraction,
            }
        )
    return combinazioni


# ============================================================
### MODIFICA QUI ### Configurazione throttling termico
# ============================================================
# CHECK_INTERVAL_SEC = 20  # ogni quanti secondi di tempo reale controllare la temp
# TEMP_LIMITE = 85.0  # C: sopra questa soglia, pausa
# TEMP_RIPRESA = 70.0  # C: sotto questa soglia, riprendi
# POLL_INTERVAL_SEC = 10  # ogni quanti secondi ricontrollare la temp mentre sei in pausa
# SENSOR_LABEL = "Tctl"  # es. "Tctl" - None = prende il max di tutti i sensori

RISULTATI_PATH = Path("risultati_pfp.csv")
CHECKPOINT_PATH = Path("checkpoint.json")


def apply_aplha_groups(conversion_map: ps.Broadcast[dict], _patterns: ps.RDD):

    def convert_to_quantified_pattern(
        pattern: "PatternWithSupport", c_map: dict | ps.Broadcast[dict]
    ) -> tuple:
        if isinstance(c_map, ps.Broadcast):
            c_map = c_map.value
        return (pattern[0], tuple(c_map[item] for item in pattern[1]))

    inverse_conversion_map = sc.broadcast(
        {v: k for k, v in conversion_map.value.items()}
    )

    flattened_patterns = _patterns.flatMap(lambda x: x[1]).distinct().persist()

    quantified_patterns = flattened_patterns.map(
        lambda pws: convert_to_quantified_pattern(pws, inverse_conversion_map)
    )

    alpha_groups = extract_alpha_groups(quantified_patterns).persist()
    return alpha_groups


def leggi_temperatura():
    # temps = psutil.sensors_temperatures()
    # if not temps:
    #     raise RuntimeError(
    #         "Nessun sensore trovato. Installa lm-sensors ed esegui 'sudo sensors-detect'."
    #     )
    # letture = []
    # for _, entries in temps.items():
    #     for entry in entries:
    #         if SENSOR_LABEL is None or entry.label == SENSOR_LABEL:
    #             letture.append(entry.current)
    # if not letture:
    #     raise RuntimeError(f"Nessuna lettura trovata per label={SENSOR_LABEL}")
    # return max(letture)
    return 0


def attendi_raffreddamento():
    # temp = leggi_temperatura()
    # if temp < TEMP_LIMITE:
    #     return
    # print(f"  Temperatura {temp:.1f}C >= {TEMP_LIMITE}C, pausa...")
    # while temp >= TEMP_RIPRESA:
    #     time.sleep(POLL_INTERVAL_SEC)
    #     temp = leggi_temperatura()
    #     print(f"    ...{temp:.1f}C")
    # print(f"  Temperatura scesa a {temp:.1f}C, riprendo.")
    pass


def carica_checkpoint():
    if CHECKPOINT_PATH.exists():
        with open(CHECKPOINT_PATH) as f:
            return json.load(f)["ultimo_indice_completato"]
    return -1


def salva_checkpoint(indice):
    with open(CHECKPOINT_PATH, "w") as f:
        json.dump({"ultimo_indice_completato": indice}, f)


if __name__ == "__main__":
    SRC_DIR = Path(__file__).parent
    DATASETS_DIR = Path(__file__).parent.parent / "dataset"

    event_log_dir = "/tmp/spark-events"
    if not os.path.exists(event_log_dir):
        os.makedirs(event_log_dir)
    elif not os.access(event_log_dir, os.W_OK):
        raise PermissionError(
            f"Directory {event_log_dir} is not writable. Please check permissions."
        )

    spark = (
        SparkSession.builder.appName("test_pfp")
        .master(f"local[{NUM_CORES}]")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", event_log_dir)
        .config("spark.eventLog.compress", "true")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sdf = spark.read.csv(
        os.path.join(DATASETS_DIR, "dunnhumby/transaction_data.csv"),
        header=True,
        inferSchema=True,
    )

    sc.addPyFile(os.path.join(SRC_DIR, "alpha.py"))
    sc.addPyFile(os.path.join(SRC_DIR, "pfp.py"))
    sc.addPyFile(os.path.join(SRC_DIR, "fpgrowth.py"))

    transactions = (
        sdf.select("BASKET_ID", "PRODUCT_ID", "QUANTITY")
        .rdd.map(
            lambda row: (row["BASKET_ID"], (row["PRODUCT_ID"], int(row["QUANTITY"])))
        )
        .groupByKey()
        .mapValues(list)
        .persist(ps.StorageLevel.MEMORY_AND_DISK)
    )

    total = transactions.count()

    from algorithm import apply_pfp

    def warm_up():
        for it in range(10):
            data_len = random.randint(1000, total)
            sample = transactions.sample(False, data_len).persist(
                ps.StorageLevel.MEMORY_AND_DISK
            )
            patterns = apply_pfp(
                sample, support=100, max_heap_size=1000, num_groups=500
            )
            patterns.count()
            extract_alpha_groups(patterns).count()
            sample.unpersist()
            print(f"Warm-up[{it}]: with {data_len} transactions completed")

    warm_up()

    # ============================================================
    # Ciclo principale: 8000 combinazioni, con checkpoint e pause termiche
    # ============================================================
    parametri = genera_parametri()
    n_totale = len(parametri)
    print(f"Totale combinazioni: {n_totale}")

    start_idx = carica_checkpoint() + 1
    if start_idx > 0:
        print(f"Riprendo dal checkpoint: indice {start_idx}")

    file_esiste = RISULTATI_PATH.exists()
    csv_file = open(RISULTATI_PATH, "a", newline="")
    fieldnames = [
        "indice",
        "support",
        "heap_size",
        "num_groups",
        "fraction",
        "sample_size",
        "patterns_count",
        "reduced_patterns_count",
        "pfp_time_sec",
        "alpha_extraction_time_sec",
    ]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    if not file_esiste:
        writer.writeheader()

    ultimo_controllo = time.monotonic()

    try:
        for i in range(start_idx, n_totale):
            p = parametri[i]
            data_len = int(total * p["fraction"])
            print(
                f"[{i + 1}/{n_totale}] support={p['support']} heap_size={p['heap_size']} "
                f"num_groups={p['num_groups']} fraction={p['fraction']} (~{data_len} transazioni)"
            )

            sample = (
                transactions.zipWithIndex()
                .filter(lambda x: x[1] < data_len)
                .keys()
                .persist(ps.StorageLevel.MEMORY_AND_DISK)
            )
            sample_size = sample.count()

            try:
                start_time = perf_counter()
                patterns = apply_pfp(
                    sample,
                    support=p["support"],
                    max_heap_size=p["heap_size"],
                    num_groups=p["num_groups"],
                )
                patterns_count = patterns.count()
                pfp_time = perf_counter() - start_time

                start_time = perf_counter()
                reduce_patterns_count = extract_alpha_groups(patterns).count()
                alpha_time = perf_counter() - start_time

                writer.writerow(
                    {
                        "indice": i,
                        "support": p["support"],
                        "heap_size": p["heap_size"],
                        "num_groups": p["num_groups"],
                        "fraction": p["fraction"],
                        "sample_size": sample_size,
                        "patterns_count": patterns_count,
                        "reduced_patterns_count": reduce_patterns_count,
                        "pfp_time_sec": pfp_time,
                        "alpha_extraction_time_sec": alpha_time,
                    }
                )
                csv_file.flush()

            except Exception as e:
                print(f"  ERRORE su indice {i}: {e}")
            finally:
                sample.unpersist()

            salva_checkpoint(i)

            # Controllo termico basato sul tempo reale trascorso
            # ora = time.monotonic()
            # if ora - ultimo_controllo >= CHECK_INTERVAL_SEC:
            #     attendi_raffreddamento()
            #     ultimo_controllo = time.monotonic()

        print("Completato tutte le combinazioni.")

    finally:
        csv_file.close()
        spark.stop()

    # ============================================================
    # Grafico finale: tempo di esecuzione vs numero di transazioni
    # (una linea per ogni combinazione support/heap_size/num_groups,
    #  utile solo se hai poche combinazioni "fisse" da confrontare;
    #  con 8000 punti conviene analizzare il CSV con pandas dopo)
    # ============================================================
    print(
        f"Dati grezzi salvati in {RISULTATI_PATH}. Usa pandas per costruire i grafici che ti servono."
    )
