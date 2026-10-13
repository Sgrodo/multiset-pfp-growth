import json
import os
from pathlib import Path
import random
import time

from pyspark.sql import SparkSession
import requests

from pfp import parallel_fp_growth


SRC_DIR = Path(__file__).parent
DATASETS_DIR = Path(__file__).parent.parent / "dataset"
EVENT_LOG_DIR = "/tmp/spark-events"


def init_event_logs():
    # Check /tmp/spark-events exists and is writable, otherwise Spark will fail to start.
    if not os.path.exists(EVENT_LOG_DIR):
        os.makedirs(EVENT_LOG_DIR)
    elif not os.access(EVENT_LOG_DIR, os.W_OK):
        raise PermissionError(
            f"Directory {EVENT_LOG_DIR} is not writable. Please check permissions."
        )


_NUM_CORES = 8
_MINIMUM_SUPPORT = 10
_HEAP_SIZE = 1000
_NUM_GROUPS = 500


def run(
    num_cores: int = _NUM_CORES,
    minimum_support: int = _MINIMUM_SUPPORT,
    heap_size: int = _HEAP_SIZE,
    num_groups: int = _NUM_GROUPS,
    warm_up: bool = False,
):
    spark = (
        SparkSession.builder.appName("statistics_pfp")
        .master(f"local[{num_cores}]")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", EVENT_LOG_DIR)
        .config("spark.eventLog.compress", "true")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sdf = spark.read.parquet(os.path.join(DATASETS_DIR, "online_retail.parquet"))

    sc.addPyFile(os.path.join(SRC_DIR, "alpha.py"))
    sc.addPyFile(os.path.join(SRC_DIR, "pfp.py"))
    sc.addPyFile(os.path.join(SRC_DIR, "fpgrowth.py"))

    app_id = sc.applicationId
    print(f"Spark application ID: {app_id}")
    num_repartitions = sc.defaultParallelism

    start = time.time()
    transactions = (
        sdf.select("InvoiceNo", "StockCode", "Quantity")
        .rdd.map(
            lambda row: (row["InvoiceNo"], (row["StockCode"], int(row["Quantity"])))
        )
        .groupByKey()
        .mapValues(list)
    )

    print(f"[{app_id}] Transactions loaded and grouped by InvoiceNo.")
    if warm_up:
        import pyspark as ps

        data_len = random.randint(1000, transactions.count())
        transactions = transactions.sample(False, data_len).persist(
            ps.StorageLevel.MEMORY_AND_DISK
        )
        print(f"[{app_id}] Warm-up sample created with {data_len} transactions.")

    conversion_map = sc.broadcast(
        transactions.flatMap(lambda x: x[1])  # prendo tutte le tuple
        .distinct()  # tuple uniche
        .sortBy(lambda pair: (pair[0], pair[1]))  # ordine stabile
        .zipWithIndex()  # assegna indice 0,1,2...
        .collectAsMap()
    )
    print(
        f"[{app_id}] Conversion map created with {len(conversion_map.value)} unique items."
    )

    adapted_transactions = transactions.map(
        lambda t: [conversion_map.value[item] for item in t[1]]
    ).repartition(num_repartitions)

    patterns = parallel_fp_growth(
        adapted_transactions, minimum_support, heap_size, num_groups=num_groups
    )
    n_itemsets = patterns.count()
    elapsed = time.time() - start

    print(f"[{app_id}] Analysis completed in {elapsed} seconds.")

    # raccogli le metriche PRIMA di fare stop
    stages = requests.get(
        f"http://localhost:4040/api/v1/applications/{app_id}/stages"
    ).json()
    total_spill_disk = sum(s.get("diskBytesSpilled", 0) for s in stages)
    total_shuffle_read = sum(s.get("shuffleReadBytes", 0) for s in stages)

    spark.stop()
    print(f"[{app_id}] Spark session stopped.")

    return {
        "min_support": minimum_support,
        "heap_size": heap_size,
        "num_groups": num_groups,
        "time_sec": elapsed,
        "n_itemsets": n_itemsets,
        "spill_disk_bytes": total_spill_disk,
        "shuffle_read_bytes": total_shuffle_read,
    }

    # flat_patterns = set()
    # for group in patterns.values():
    #     flat_patterns.update(group)

    # def convert_to_quantified_pattern(
    #     pattern: "PatternWithSupport", c_map: dict | pyspark.Broadcast[dict]
    # ) -> tuple:
    #     if isinstance(c_map, pyspark.Broadcast):
    #         c_map = c_map.value
    #     return (pattern[0], tuple(c_map[item] for item in pattern[1]))

    # inverse_conversion_map = sc.broadcast(
    #     {v: k for k, v in conversion_map.value.items()}
    # )

    # flattened_patterns = patterns.flatMap(lambda x: x[1]).distinct().persist()
    # prev_count = flattened_patterns.count()

    # quantified_patterns = flattened_patterns.map(
    #     lambda pws: convert_to_quantified_pattern(pws, inverse_conversion_map)
    # )

    # alpha_groups = extract_alpha_groups(quantified_patterns).persist()
    # pattern_count = alpha_groups.count()

    # print(
    #     f"Total patterns: {pattern_count}\tPruned patterns: {prev_count - pattern_count}"
    # )


init_event_logs()


def pick_random_parameters() -> tuple[int, int, int]:
    support_range = [10, 20, 50, 100]
    heap_sizes = [500, 1000, 1500, 2000]
    num_groups_list = [250, 500, 750, 1000]

    support = random.choice(support_range)
    heap_size = random.choice(heap_sizes)
    num_groups = random.choice(num_groups_list)

    return support, heap_size, num_groups


# Warm-up phase to ensure JIT compilation and caching effects are minimized
WARM_UP_CYCLES = 5
for i in range(WARM_UP_CYCLES):
    print(f"Warm-up cycle {i + 1}/{WARM_UP_CYCLES}")
    ms, hs, ng = pick_random_parameters()
    run(
        num_cores=_NUM_CORES,
        minimum_support=ms,
        heap_size=hs,
        num_groups=ng,
        warm_up=True,
    )

support_range = [10, 20, 50, 100]
heap_sizes = [500, 1000, 1500, 2000]
num_groups_list = [250, 500, 750, 1000]

with open("statistics_data.json", "w") as f:
    for min_support in support_range:
        for heap_size in heap_sizes:
            for num_groups in num_groups_list:
                print(
                    f"Running with min_support={min_support}, heap_size={heap_size}, num_groups={num_groups}"
                )
                metrics = run(
                    num_cores=_NUM_CORES,
                    minimum_support=min_support,
                    heap_size=heap_size,
                    num_groups=num_groups,
                )
                f.write(json.dumps(metrics) + "\n")
