from ast import operator
import json
import operator
import os
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

from alpha import extract_alpha_groups
from fpgrowth import Pattern, PatternWithSupport
from pfp import parallel_fp_growth

SRC_DIR = Path(__file__).parent
DATASETS_DIR = Path(__file__).parent.parent / "dataset"

# Check /tmp/spark-events exists and is writable, otherwise Spark will fail to start.
event_log_dir = "/tmp/spark-events"
if not os.path.exists(event_log_dir):
    os.makedirs(event_log_dir)
elif not os.access(event_log_dir, os.W_OK):
    raise PermissionError(
        f"Directory {event_log_dir} is not writable. Please check permissions."
    )

NUM_CORES = "*"

spark = (
    SparkSession.builder.appName("test_pfp")
    .master(f"local[{NUM_CORES}]")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", event_log_dir)
    .config("spark.eventLog.compress", "true")
    .getOrCreate()
)

sc = spark.sparkContext
sdf = spark.read.parquet(os.path.join(DATASETS_DIR, "online_retail_II.parquet"))

sc.addPyFile(os.path.join(SRC_DIR, "alpha.py"))
sc.addPyFile(os.path.join(SRC_DIR, "pfp.py"))
sc.addPyFile(os.path.join(SRC_DIR, "fpgrowth.py"))

num_repartitions = sc.defaultParallelism

transactions = (
    sdf.select("Invoice", "StockCode", "Quantity")
    .rdd.map(lambda row: (row["Invoice"], (row["StockCode"], int(row["Quantity"]))))
    .groupByKey()
    .mapValues(list)
)
conversion_map = sc.broadcast(
    transactions.flatMap(lambda x: x[1])  # prendo tutte le tuple
    .distinct()  # tuple uniche
    .sortBy(lambda pair: (pair[0], pair[1]))  # ordine stabile
    .zipWithIndex()  # assegna indice 0,1,2...
    .collectAsMap()
)
adapted_transactions = transactions.map(
    lambda t: [conversion_map.value[item] for item in t[1]]
).repartition(num_repartitions)

minimum_support = 5
heap_size = 100000000
patterns = parallel_fp_growth(adapted_transactions, minimum_support, heap_size)
# flat_patterns = set()
# for group in patterns.values():
#     flat_patterns.update(group)


def convert_to_quantified_pattern(
    pattern: "PatternWithSupport", c_map: dict | pyspark.Broadcast[dict]
) -> tuple:
    if isinstance(c_map, pyspark.Broadcast):
        c_map = c_map.value
    return (pattern[0], tuple(c_map[item] for item in pattern[1]))


inverse_conversion_map = sc.broadcast({v: k for k, v in conversion_map.value.items()})

flattened_patterns = patterns.flatMap(lambda x: x[1]).distinct().persist()
prev_count = flattened_patterns.count()

quantified_patterns = flattened_patterns.map(
    lambda pws: convert_to_quantified_pattern(pws, inverse_conversion_map)
)

alpha_groups = extract_alpha_groups(quantified_patterns).persist()
pattern_count = alpha_groups.aggregate(
    0, lambda acc, group: acc + group["pattern_count"], operator.add
)
print(f"Total patterns: {pattern_count}\tPruned patterns: {prev_count - pattern_count}")
# with open(SRC_DIR / "alpha_groups.json", "w") as f:
#     json.dump(alpha_groups, f, indent=4)
