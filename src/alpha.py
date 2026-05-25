from functools import reduce
from math import gcd
from typing import TypeAlias

from pyspark import RDD

from fpgrowth import Pattern, PatternWithSupport

CanonicalPatternForm: TypeAlias = tuple[Pattern, int, Pattern]
ShapeGroupedCanonicalPattern: TypeAlias = tuple[Pattern, tuple[int, Pattern, int]]
SupportCanonicalForm: TypeAlias = tuple[int, Pattern, int]


def pattern_canonical_form(pattern: Pattern) -> CanonicalPatternForm | None:

    canonical_pattern = tuple(sorted(pattern, key=lambda x: x[0]))

    quantities = [abs(int(qty)) for item, qty in canonical_pattern]

    if not quantities:
        return None

    alpha = reduce(gcd, quantities)

    if alpha == 0:
        return None

    shape = tuple((item, abs(int(qty)) // alpha) for item, qty in canonical_pattern)

    return shape, alpha, canonical_pattern


def alpha_decomposition(
    pattern: PatternWithSupport,
) -> list[ShapeGroupedCanonicalPattern]:
    support, pattern_items = pattern
    if (r := pattern_canonical_form(pattern_items)) is None:
        return []
    return [(r[0], (r[1], r[2], support))]


def reduce_alpha_edges(alphas: list[int]) -> list[tuple[int, int, int]]:
    sorted_alphas = sorted(set(alphas))
    edges = []

    for parent in sorted_alphas:
        for child in sorted_alphas:
            if parent >= child:
                continue

            if child % parent != 0:
                continue

            redundant = False

            for middle in sorted_alphas:
                if parent < middle < child:
                    if middle % parent == 0 and child % middle == 0:
                        redundant = True
                        break

            if not redundant:
                edges.append((parent, child, child // parent))

    return edges


def summarize_alpha_group(group: tuple[Pattern, list[SupportCanonicalForm]]) -> dict:
    shape, values = group

    by_alpha = {}

    for alpha, pattern, support in values:
        if alpha not in by_alpha:
            by_alpha[alpha] = []
        by_alpha[alpha].append((pattern, support))

    alphas = sorted(by_alpha.keys())
    edges = reduce_alpha_edges(alphas)

    return {
        "shape": shape,
        "shape_size": len(shape),  # numero di oggetti nella shape
        "alpha_count": len(alphas),
        "pattern_count": sum(len(v) for v in by_alpha.values()),
        "alphas": alphas,
        "patterns_by_alpha": by_alpha,
        "edges": edges,
        "edge_count": len(edges),
        "is_proportional": len(alphas) > 1,
    }


def extract_alpha_groups(patterns: RDD[tuple]) -> RDD[dict]:
    return (
        patterns.flatMap(alpha_decomposition)
        .groupByKey()  # group by shape
        .map(summarize_alpha_group)  # summarize each shape group
    )
