"""Run parallel FP-Growth and alpha analysis, then create a compact report.

Example:
    spark-submit test_pfp_alpha_report.py \
        --dataset ../dataset/online_retail.parquet \
        --output ../results/online_retail

The script expects a Parquet dataset with InvoiceNo, StockCode and Quantity.
Returns are intentionally retained: this matches the current project semantics.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pyspark
from pyspark.sql import SparkSession

from alpha import extract_alpha_groups
from fpgrowth import Pattern, PatternWithSupport
from pfp import parallel_fp_growth


DEFAULT_MIN_SUPPORT = 10
DEFAULT_HEAP_SIZE = 1000
DEFAULT_NUM_GROUPS = 500
DEFAULT_NUM_CORES = 10
DEFAULT_TOP_N = 20


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FP-Growth + alpha analysis with compact tables and plots."
    )
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--min-support", type=int, default=DEFAULT_MIN_SUPPORT)
    parser.add_argument("--heap-size", type=int, default=DEFAULT_HEAP_SIZE)
    parser.add_argument("--num-groups", type=int, default=DEFAULT_NUM_GROUPS)
    parser.add_argument("--num-cores", type=int, default=DEFAULT_NUM_CORES)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    return parser.parse_args()


def convert_to_quantified_pattern(
    pattern: PatternWithSupport,
    conversion_map: dict[int, tuple[Any, int]] | pyspark.Broadcast,
) -> PatternWithSupport:
    if isinstance(conversion_map, pyspark.Broadcast):
        conversion_map = conversion_map.value
    support, items = pattern
    return support, tuple(conversion_map[item] for item in items)


def product_label(item: Any, descriptions: dict[Any, str] | None = None) -> str:
    description = descriptions.get(item) if descriptions else None
    if description:
        return f"{description} [{item}]"
    return str(item)


def pattern_text(
    pattern: Pattern,
    descriptions: dict[Any, str] | None = None,
    max_items: int | None = None,
) -> str:
    items = list(pattern)
    shown = items if max_items is None else items[:max_items]
    text = ", ".join(
        f"{product_label(item, descriptions)} x{quantity}"
        for item, quantity in shown
    )
    if max_items is not None and len(items) > max_items:
        text += f", ... (+{len(items) - max_items})"
    return text


def markdown_escape(text: str) -> str:
    """Escape table separators without backslashes inside f-string expressions."""
    return text.replace("|", "\\|")


def alpha_support_text(group: dict) -> str:
    """Compact mapping from each alpha level to its observed support(s)."""
    parts = []
    for alpha in group["alphas"]:
        supports = sorted(
            {support for _, support in group["patterns_by_alpha"][alpha]},
            reverse=True,
        )
        value = str(supports[0]) if len(supports) == 1 else str(supports)
        parts.append(f"{alpha}: {value}")
    return "; ".join(parts)


def group_peak_support(group: dict) -> int:
    return max(
        support
        for entries in group["patterns_by_alpha"].values()
        for _, support in entries
    )


def support_bucket(support: int) -> str:
    """Logarithmic bucket that stays readable for datasets of different sizes."""
    if support <= 1:
        return "1"
    lower = 2 ** int(math.log2(support))
    upper = 2 * lower - 1
    return f"{lower}-{upper}"


def bucket_lower_bound(label: str) -> int:
    return int(label.split("-", 1)[0])


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def save_bar_plot(
    labels: list[str],
    values: list[int],
    title: str,
    xlabel: str,
    path: Path,
    horizontal: bool = False,
) -> None:
    fig_height = max(4.5, len(labels) * 0.34) if horizontal else 5.0
    fig, ax = plt.subplots(figsize=(11, fig_height))
    if horizontal:
        positions = list(range(len(labels)))
        ax.barh(positions, values, color="#4472C4")
        ax.set_yticks(positions, labels)
        ax.invert_yaxis()
        ax.set_xlabel(xlabel)
    else:
        ax.bar(labels, values, color="#4472C4")
        ax.set_ylabel(xlabel)
        ax.tick_params(axis="x", rotation=45)
    ax.set_title(title)
    ax.grid(axis="x" if horizontal else "y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    args.output.mkdir(parents=True, exist_ok=True)

    event_log_dir = Path("/tmp/spark-events")
    event_log_dir.mkdir(parents=True, exist_ok=True)
    if not os.access(event_log_dir, os.W_OK):
        raise PermissionError(f"{event_log_dir} is not writable")

    spark = (
        SparkSession.builder.appName("pfp_alpha_report")
        .master(f"local[{args.num_cores}]")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", str(event_log_dir))
        .config("spark.eventLog.compress", "true")
        .getOrCreate()
    )
    sc = spark.sparkContext

    source_dir = Path(__file__).resolve().parent
    for module in ("alpha.py", "pfp.py", "fpgrowth.py"):
        sc.addPyFile(str(source_dir / module))

    try:
        sdf = spark.read.parquet(str(args.dataset))
        required_columns = {"InvoiceNo", "StockCode", "Quantity"}
        missing_columns = required_columns.difference(sdf.columns)
        if missing_columns:
            raise ValueError(
                "Dataset columns missing: " + ", ".join(sorted(missing_columns))
            )

        # Deliberately retain negative quantities/returns. Only null records are removed.
        selected_columns = ["InvoiceNo", "StockCode", "Quantity"]
        has_descriptions = "Description" in sdf.columns
        if has_descriptions:
            selected_columns.append("Description")
        rows = (
            sdf.select(*selected_columns)
            .rdd.filter(
                lambda row: row["InvoiceNo"] is not None
                and row["StockCode"] is not None
                and row["Quantity"] is not None
            )
        )

        row_count = rows.count()
        if has_descriptions:
            description_map = (
                rows.filter(
                    lambda row: row["Description"] is not None
                    and str(row["Description"]).strip() != ""
                )
                .map(
                    lambda row: (
                        (row["StockCode"], str(row["Description"]).strip()),
                        1,
                    )
                )
                .reduceByKey(lambda a, b: a + b)
                .map(lambda entry: (entry[0][0], (entry[1], entry[0][1])))
                .reduceByKey(lambda a, b: a if a[0] >= b[0] else b)
                .mapValues(lambda value: value[1])
                .collectAsMap()
            )
        else:
            description_map = {}
        transactions = (
            rows.map(
                lambda row: (
                    row["InvoiceNo"],
                    (row["StockCode"], int(row["Quantity"])),
                )
            )
            .groupByKey()
            # FP-Growth mines itemsets: an identical item may occur at most once
            # in a transaction. Preserve the first occurrence for stable output.
            .mapValues(lambda items: list(dict.fromkeys(items)))
            .persist()
        )
        transaction_count = transactions.count()

        unique_items = transactions.flatMap(lambda entry: entry[1]).distinct().persist()
        unique_item_count = unique_items.count()
        unique_product_count = rows.map(lambda row: row["StockCode"]).distinct().count()

        conversion_map = sc.broadcast(
            unique_items.sortBy(lambda pair: (str(pair[0]), pair[1]))
            .zipWithIndex()
            .collectAsMap()
        )
        inverse_conversion_map = sc.broadcast(
            {index: item for item, index in conversion_map.value.items()}
        )

        adapted_transactions = transactions.map(
            lambda entry: [conversion_map.value[item] for item in entry[1]]
        ).repartition(sc.defaultParallelism)

        patterns_by_item = parallel_fp_growth(
            adapted_transactions,
            args.min_support,
            args.heap_size,
            num_groups=args.num_groups,
        )
        flattened_patterns = (
            patterns_by_item.flatMap(lambda entry: entry[1]).distinct().persist()
        )
        frequent_pattern_count = flattened_patterns.count()

        quantified_patterns = flattened_patterns.map(
            lambda pattern: convert_to_quantified_pattern(
                pattern, inverse_conversion_map
            )
        ).persist()

        alpha_groups = extract_alpha_groups(quantified_patterns).persist()
        canonical_shape_count = alpha_groups.count()
        proportional_groups = alpha_groups.filter(
            lambda group: group["is_proportional"]
        ).persist()
        proportional_shape_count = proportional_groups.count()
        patterns_in_proportional_shapes = proportional_groups.map(
            lambda group: group["pattern_count"]
        ).sum()
        # Alpha reduction concerns distinct scale levels, not multiple patterns
        # that happen to have the same alpha.
        alpha_levels_before_reduction = alpha_groups.map(
            lambda group: group["alpha_count"]
        ).sum()
        alpha_levels_collapsed = proportional_groups.map(
            lambda group: group["alpha_count"] - 1
        ).sum()

        top_patterns = quantified_patterns.takeOrdered(
            args.top_n,
            key=lambda value: (-value[0], -len(value[1]), repr(value[1])),
        )
        top_groups = proportional_groups.takeOrdered(
            args.top_n,
            key=lambda group: (
                -group["alpha_count"],
                -group["pattern_count"],
                -group["shape_size"],
                repr(group["shape"]),
            ),
        )
        top_supported_groups = proportional_groups.takeOrdered(
            args.top_n,
            key=lambda group: (
                -group_peak_support(group),
                -group["alpha_count"],
                repr(group["shape"]),
            ),
        )

        pattern_length_distribution = dict(
            quantified_patterns.map(lambda value: (len(value[1]), 1))
            .reduceByKey(lambda a, b: a + b)
            .sortByKey()
            .collect()
        )
        support_distribution = dict(
            quantified_patterns.map(lambda value: (support_bucket(value[0]), 1))
            .reduceByKey(lambda a, b: a + b)
            .collect()
        )
        support_distribution = dict(
            sorted(support_distribution.items(), key=lambda item: bucket_lower_bound(item[0]))
        )
        alpha_count_distribution = dict(
            proportional_groups.map(lambda group: (group["alpha_count"], 1))
            .reduceByKey(lambda a, b: a + b)
            .sortByKey()
            .collect()
        )

        alpha_reduction_percentage = (
            100.0 * alpha_levels_collapsed / alpha_levels_before_reduction
            if alpha_levels_before_reduction
            else 0.0
        )
        proportional_shape_percentage = (
            100.0 * proportional_shape_count / canonical_shape_count
            if canonical_shape_count
            else 0.0
        )
        proportional_pattern_percentage = (
            100.0 * patterns_in_proportional_shapes / frequent_pattern_count
            if frequent_pattern_count
            else 0.0
        )
        report_n = min(10, args.top_n)

        pattern_rows = [
            {
                "rank": rank,
                "support": support,
                "support_percentage": 100.0 * support / transaction_count,
                "length": len(pattern),
                "pattern": pattern_text(pattern, description_map),
            }
            for rank, (support, pattern) in enumerate(top_patterns, start=1)
        ]
        group_rows = [
            {
                "rank": rank,
                "pattern_count": group["pattern_count"],
                "alpha_count": group["alpha_count"],
                "alpha_levels_collapsed": group["alpha_count"] - 1,
                "alphas": json.dumps(group["alphas"]),
                "supports_by_alpha": alpha_support_text(group),
                "peak_support": group_peak_support(group),
                "shape_size": group["shape_size"],
                "shape": pattern_text(group["shape"], description_map),
            }
            for rank, group in enumerate(top_groups, start=1)
        ]
        supported_group_rows = [
            {
                "rank": rank,
                "peak_support": group_peak_support(group),
                "alpha_count": group["alpha_count"],
                "alphas": json.dumps(group["alphas"]),
                "supports_by_alpha": alpha_support_text(group),
                "shape": pattern_text(group["shape"], description_map),
            }
            for rank, group in enumerate(top_supported_groups, start=1)
        ]

        write_csv(
            args.output / "top_patterns.csv",
            ["rank", "support", "support_percentage", "length", "pattern"],
            pattern_rows,
        )
        write_csv(
            args.output / "top_alpha_groups.csv",
            [
                "rank",
                "pattern_count",
                "alpha_count",
                "alpha_levels_collapsed",
                "alphas",
                "supports_by_alpha",
                "peak_support",
                "shape_size",
                "shape",
            ],
            group_rows,
        )
        write_csv(
            args.output / "top_supported_alpha_groups.csv",
            [
                "rank",
                "peak_support",
                "alpha_count",
                "alphas",
                "supports_by_alpha",
                "shape",
            ],
            supported_group_rows,
        )

        save_bar_plot(
            [f"{row['rank']}. {row['pattern'][:65]}" for row in pattern_rows],
            [row["support"] for row in pattern_rows],
            f"Top {len(pattern_rows)} frequent patterns",
            "Transactions",
            args.output / "top_bundles.png",
            horizontal=True,
        )
        save_bar_plot(
            [f"{row['rank']}. {row['shape'][:65]}" for row in group_rows],
            [row["alpha_levels_collapsed"] for row in group_rows],
            f"Top {len(group_rows)} proportional shapes",
            "Distinct alpha levels collapsed",
            args.output / "top_alpha_families.png",
            horizontal=True,
        )

        report_lines = [
            "# FP-Growth and alpha analysis",
            "",
            "## Configuration",
            "",
            f"- Dataset: `{args.dataset}`",
            f"- Minimum support: {args.min_support:,}",
            f"- Heap size: {args.heap_size:,}",
            f"- PFP groups: {args.num_groups:,}",
            f"- Local Spark cores: {args.num_cores}",
            "- Returns/negative quantities: retained",
            f"- Product descriptions: {'available' if has_descriptions else 'not available'}",
            "",
            "## Dataset summary",
            "",
            f"- Rows: {row_count:,}",
            f"- Transactions: {transaction_count:,}",
            f"- Distinct products: {unique_product_count:,}",
            f"- Distinct `(product, quantity)` items: {unique_item_count:,}",
            "",
            "## Main results",
            "",
            f"- Frequent patterns: {frequent_pattern_count:,}",
            f"- Canonical shapes: {canonical_shape_count:,}",
            f"- Proportional shapes (more than one alpha): {proportional_shape_count:,} "
            f"({proportional_shape_percentage:.2f}% of shapes)",
            f"- Patterns belonging to proportional shapes: "
            f"{int(patterns_in_proportional_shapes):,} "
            f"({proportional_pattern_percentage:.2f}% of patterns)",
            f"- Distinct alpha levels before reduction: {int(alpha_levels_before_reduction):,}",
            f"- Alpha levels collapsed: {int(alpha_levels_collapsed):,}",
            f"- Reduction of distinct alpha levels: {alpha_reduction_percentage:.2f}%",
            "",
            "## Interpretation",
            "",
            f"Alpha is present in {proportional_shape_percentage:.2f}% of canonical "
            f"shapes and covers {proportional_pattern_percentage:.2f}% of returned "
            "patterns. It is therefore a minority structure in this dataset, but "
            f"it identifies {proportional_shape_count:,} distinct proportional families.",
            "",
            f"## {report_n} most frequent quantitative bundles",
            "",
            "Support is the number and percentage of transactions containing the "
            "exact products with the exact quantities shown.",
            "",
            "| # | Transactions | Share | Bundle |",
            "|---:|---:|---:|---|",
        ]
        report_lines.extend(
            f"| {row['rank']} | {row['support']:,} | "
            f"{row['support_percentage']:.2f}% | "
            f"{markdown_escape(row['pattern'])} |"
            for row in pattern_rows[:report_n]
        )
        report_lines.extend(
            [
                "",
                f"## {report_n} richest proportional families",
                "",
                "Each shape is a quantity ratio. Multiplying it by an alpha produces "
                "one of the observed quantitative bundles. `Supports by alpha` uses "
                "the form `alpha: transactions`.",
                "",
                "| # | Shape/ratio | Alpha levels | Supports by alpha |",
                "|---:|---|---|---|",
            ]
        )
        report_lines.extend(
            f"| {row['rank']} | {markdown_escape(row['shape'])} | "
            f"{row['alphas']} | {row['supports_by_alpha']} |"
            for row in group_rows[:report_n]
        )
        report_lines.extend(
            [
                "",
                f"## {report_n} strongest proportional families by support",
                "",
                "These are the proportional families containing at least one of the "
                "most frequently observed scaled bundles.",
                "",
                "| # | Peak transactions | Shape/ratio | Alpha levels | Supports by alpha |",
                "|---:|---:|---|---|---|",
            ]
        )
        report_lines.extend(
            f"| {row['rank']} | {row['peak_support']:,} | "
            f"{markdown_escape(row['shape'])} | {row['alphas']} | "
            f"{row['supports_by_alpha']} |"
            for row in supported_group_rows[:report_n]
        )
        report_lines.extend(
            [
                "",
                "## Technical appendix",
                "",
                f"- Returned-pattern lengths (`length: count`): "
                f"`{json.dumps(pattern_length_distribution)}`",
                f"- Returned-pattern support buckets (`range: count`): "
                f"`{json.dumps(support_distribution)}`",
                f"- Alpha counts among proportional shapes (`alphas: shapes`): "
                f"`{json.dumps(alpha_count_distribution)}`",
                "- These distributions describe the top-K-limited patterns returned "
                "by the configured PFP run; they are not a complete enumeration of "
                "every possible frequent pattern.",
                "",
                "## Visual summary",
                "",
                "- `top_bundles.png`",
                "- `top_alpha_families.png`",
                "",
                "Full lists are available in `top_patterns.csv`, "
                "`top_alpha_groups.csv`, and `top_supported_alpha_groups.csv`.",
            ]
        )
        (args.output / "report.md").write_text(
            "\n".join(report_lines) + "\n", encoding="utf-8"
        )

        print("\n".join(report_lines[:29]))
        print(f"\nReport written to: {args.output / 'report.md'}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
