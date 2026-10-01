import pyspark.sql as spark


def read_dataset(ss: spark.SparkSession, csv_file: str) -> spark.DataFrame:
    df = ss.read.csv(csv_file, header=True, inferSchema=True, sep=";").dropna(
        subset=["BillNo", "Itemname", "Quantity"]
    )
    all_items = df.select("Itemname").distinct().collect()
    items_ids = {row["Itemname"]: idx for idx, row in enumerate(sorted(all_items))}
    df = df.rdd.map(
        lambda row: (row["BillNo"], items_ids[row["Itemname"]], row["Quantity"])
    ).toDF(["InvoiceNo", "StockCode", "Quantity"])
    return df
