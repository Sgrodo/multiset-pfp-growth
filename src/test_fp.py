from pathlib import Path

import pandas as pd

from fpgrowth import fptree_from_transactions, top_k_fp_growth

PROJECT_DIR = Path(__file__).parent.parent

transactions = (
    pd.read_parquet(PROJECT_DIR / "dataset/online_retail.parquet")
    .groupby("InvoiceNo")
    .apply(
        lambda x: [[desc, int(qty)] for desc, qty in zip(x["StockCode"], x["Quantity"])]
    )
    .tolist()
)
min_support = 100
print(transactions[:5])
# create a map from item, quantity to a unique integer, and convert transactions to use these integers
item_quantity_pairs = set(
    (item, quantity) for transaction in transactions for item, quantity in transaction
)
conversion_map = {pair: index for index, pair in enumerate(sorted(item_quantity_pairs))}
adapted_transactions = [
    [(conversion_map[(item, quantity)], quantity) for item, quantity in transaction]
    for transaction in transactions
]

conversion_map_inv = {v: k for k, v in conversion_map.items()}

fp_tree = fptree_from_transactions(adapted_transactions, min_support)

# # write all fpgrowth patterns to a file
# with open("patterns.txt", "w") as f:
# cnt = 0
# for support, pattern in top_k_fp_growth(fp_tree, min_support, heap_size=100):
# convert pattern back to item, quantity pairs
# original_pattern = tuple(
#     (conversion_map_inv[item], quantity) for item, quantity in pattern
# )
# cnt += 1

print(
    f"Number of patterns: {len(top_k_fp_growth(fp_tree, min_support, heap_size=100))}"
)
