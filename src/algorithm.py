import pyspark as ps

from alpha import extract_alpha_groups
from fpgrowth import PatternWithSupport
from pfp import parallel_fp_growth


def apply_pfp(
    transactions: ps.RDD,
    support: int,
    max_heap_size: int,
    num_groups: int,
    num_partitions: int | None = None,
) -> ps.RDD[PatternWithSupport]:
    sc = transactions.context
    num_repartitions = num_partitions or sc.defaultParallelism

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

    patterns = parallel_fp_growth(
        adapted_transactions, support, max_heap_size, num_groups
    )

    def convert_to_quantified_pattern(
        pattern: "PatternWithSupport", c_map: dict | ps.Broadcast[dict]
    ) -> tuple:
        if isinstance(c_map, ps.Broadcast):
            c_map = c_map.value
        return (pattern[0], tuple(c_map[item] for item in pattern[1]))

    inverse_conversion_map = sc.broadcast(
        {v: k for k, v in conversion_map.value.items()}
    )

    flattened_patterns = patterns.flatMap(lambda x: x[1]).distinct().persist()

    return flattened_patterns.map(
        lambda pws: convert_to_quantified_pattern(pws, inverse_conversion_map)
    )
