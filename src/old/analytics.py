import os
from pathlib import Path
import random
from time import perf_counter

import pyspark as ps
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

from alpha import extract_alpha_groups

STEP = 10000
SUPPORT = 100
HEAP_SIZE = 1000
NUM_GROUPS = 500

NUM_CORES = 8


def save_chart(data, file_name: str):
    x = [d[0] for d in data]
    y = [d[1] for d in data]
    plt.plot(x, y)
    plt.xlabel("Number of Transactions")
    plt.ylabel("Execution Time (seconds)")
    plt.title(
        f"Performance of PFP-Growth Algorithm (Dunnhumby Dataset)\nSupport={SUPPORT}, Heap Size={HEAP_SIZE}, Groups={NUM_GROUPS}"
    )
    plt.grid()
    plt.savefig(file_name)
    plt.close()


if __name__ == "__main__":
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

    num_repartitions = sc.defaultParallelism

    transactions = (
        sdf.select("BASKET_ID", "PRODUCT_ID", "QUANTITY")
        .rdd.map(
            lambda row: (row["BASKET_ID"], (row["PRODUCT_ID"], int(row["QUANTITY"])))
        )
        .groupByKey()
        .mapValues(list)
        .persist(ps.StorageLevel.MEMORY_AND_DISK)
    )

    perf_data_pfp = []
    perf_data_props = []
    from algorithm import apply_pfp

    def warm_up():
        for it in range(10):
            data_len = random.randint(1000, transactions.count())
            sample = transactions.sample(False, data_len).persist(
                ps.StorageLevel.MEMORY_AND_DISK
            )
            patterns = apply_pfp(
                sample,
                support=SUPPORT,
                max_heap_size=HEAP_SIZE,
                num_groups=NUM_GROUPS,
            )
            patterns.count()  # Force evaluation

            # Test alpha groups extraction performance on the same sample
            extract_alpha_groups(patterns).count()  # Force evaluation
            print(f"Warm-up[{it}]: with {data_len} transactions completed")

    warm_up()

    total = transactions.count()
    fractions = [0.1, 0.25, 0.5, 0.75, 1.0]
    for fraction in fractions:
        data_len = int(total * fraction)
        sample = (
            transactions.zipWithIndex()
            .filter(lambda x: x[1] < data_len)
            .keys()
            .persist(ps.StorageLevel.MEMORY_AND_DISK)
        )
        sample_size = sample.count()  # Force evaluation to load data into memory
        print(f"Processing sample with {sample_size} transactions...")

        # Test PFP-Growth performance on the sample
        start_time = perf_counter()
        patterns = apply_pfp(
            sample, support=SUPPORT, max_heap_size=HEAP_SIZE, num_groups=NUM_GROUPS
        )
        patterns.count()  # Force evaluation
        end_time = perf_counter()
        perf_data_pfp.append((sample_size, end_time - start_time))

        # Test alpha groups extraction performance on the same sample
        start_time = perf_counter()
        extract_alpha_groups(patterns).count()  # Force evaluation
        end_time = perf_counter()
        perf_data_props.append((sample_size, end_time - start_time))
        print(
            f"Processed {sample_size} transactions in {end_time - start_time:.2f} seconds"
        )
        sample.unpersist()

    save_chart(perf_data_pfp, "performance_chart_dunnhumby.png")
    save_chart(perf_data_props, "properties_extraction_chart_dunnhumby.png")
