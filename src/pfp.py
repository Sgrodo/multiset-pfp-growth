from typing import Any, TypeAlias

import pyspark

from fpgrowth import (
    FPTree,
    PatternWithSupport,
    add_to_heap,
    merge_heaps,
    top_k_fp_growth,
    construct_conditional_tree,
)

Transaction: TypeAlias = list[Any]


def parallel_fp_growth(
    transactions: pyspark.RDD[list[Transaction]],
    min_support: int,
    heap_size: int,
    num_groups: int = 100,
) -> pyspark.RDD[tuple[Any, list[PatternWithSupport]]]:
    sc = transactions.context
    # Collect the map, then sort it by support descending
    raw_f_map = (
        count_items(transactions).filter(lambda x: x[1] >= min_support).collect()
    )
    sorted_f_items = sorted(raw_f_map, key=lambda x: x[1], reverse=True)

    # Rebuild f_map and g_list based on the sorted order
    f_map = sc.broadcast(dict(sorted_f_items))
    g_list = sc.broadcast(
        {item: index % num_groups for index, (item, _) in enumerate(sorted_f_items)}
    )
    inverse_g_list = {gid: [] for gid in range(num_groups)}
    for item, gid in g_list.value.items():
        inverse_g_list[gid].append(item)

    inverse_g_list = sc.broadcast(inverse_g_list)
    return (
        transactions.map(lambda transaction: reorder_transaction(transaction, f_map))
        .flatMap(lambda t: generate_group_dependent_transactions(t, g_list))
        .groupByKey()
        .flatMap(
            lambda x: find_patterns(
                x[0], list(x[1]), inverse_g_list, heap_size, min_support
            )
        )
        .flatMap(lambda x: [(_item, x) for _item in x[1]])
        .combineByKey(
            lambda p: [p],
            lambda acc, p: add_to_heap(acc, p, heap_size),
            lambda h1, h2: merge_heaps(h1, h2, heap_size),
        )
        .persist()
    )


def create_tree_combiner(t: Transaction) -> FPTree:
    tree = FPTree()
    tree.insert([(item, 1) for item in t])
    return tree


def merge_values(tree: FPTree, t: Transaction) -> FPTree:
    tree.insert([(item, 1) for item in t])
    return tree


def reorder_transaction(
    transaction: Transaction, f_map: pyspark.Broadcast[dict[Any, int]] | dict[Any, int]
) -> Transaction:
    if isinstance(f_map, pyspark.Broadcast):
        f_map = f_map.value
    filtered_transaction = filter(lambda item: item in f_map, transaction)
    sorted_transaction = sorted(
        filtered_transaction, key=lambda item: (f_map[item], item), reverse=True
    )
    return list(sorted_transaction)


def find_patterns(
    gid: int,
    transactions: list[Transaction],
    inverse_g_list: pyspark.Broadcast[dict[int, list[Any]]],
    heap_size: int,
    min_support: int,
) -> list[PatternWithSupport]:
    tree = FPTree()
    for transaction in transactions:
        tree.insert([(item, 1) for item in transaction])

    now_group = inverse_g_list.value[gid]
    patterns = []
    for i in now_group:
        conditional_tree = construct_conditional_tree(tree, i, min_support)
        for support, pattern in top_k_fp_growth(
            conditional_tree, min_support, (i,), heap_size
        ):
            patterns.append((support, pattern))

    return patterns


def generate_group_dependent_transactions(
    transaction: Transaction, g_list: pyspark.Broadcast[dict[Any, int]]
) -> list[tuple[int, Transaction]]:
    seen = {}
    transactions = []
    for i in range(len(transaction) - 1, -1, -1):
        gid = g_list.value[transaction[i]]
        if seen.get(gid, False):
            continue
        seen[gid] = True
        transactions.append((gid, transaction[: i + 1]))

    return transactions


def count_items(
    transactions: pyspark.RDD[list[Transaction]],
) -> pyspark.RDD[tuple[Any, int]]:
    return transactions.flatMap(lambda t: [(item, 1) for item in t]).reduceByKey(
        lambda a, b: a + b
    )
