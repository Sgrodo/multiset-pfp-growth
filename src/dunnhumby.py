from pathlib import Path

import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DATASET_PATH = (
    Path(__file__).parent.parent / "dataset" / "dunnhumby/transaction_data.csv"
)


def load_transactions_rdd(
    session: SparkSession,
) -> "ps.RDD[tuple[str, list[tuple[str, int]]]]":
    return (
        session.read.csv(
            DATASET_PATH.as_posix(),
            header=True,
            inferSchema=True,
        )
        .groupBy("BASKET_ID", "PRODUCT_ID")
        .agg(F.sum(F.abs("QUANTITY")).alias("QUANTITY"))
        .rdd.map(
            lambda row: (
                str(row["BASKET_ID"]),
                (str(row["PRODUCT_ID"]), int(row["QUANTITY"])),
            )
        )
        .groupByKey()
        .mapValues(list)
        .persist(ps.StorageLevel.MEMORY_AND_DISK)
    )
