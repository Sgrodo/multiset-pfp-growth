from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, TypeAlias

Transaction: TypeAlias = list[tuple[Any, int]]
Pattern: TypeAlias = tuple
PatternWithSupport: TypeAlias = tuple[int, Pattern]


@dataclass
class FPNode:
    item: Any
    count: int = 1
    parent: "FPNode | None" = None
    children: dict[Any, "FPNode"] = field(default_factory=dict)


class FPTree:
    def __init__(self):
        self.__root = FPNode(None)
        self.__header_table: dict[Any, list[FPNode]] = {}
        self.__node_count = 0

    @property
    def root(self) -> FPNode:
        return self.__root

    @property
    def header_table(self) -> dict[Any, list[FPNode]]:
        # Fix: was cached_property, which went stale after inserts
        return self.__header_table

    @property
    def node_count(self) -> int:
        return self.__node_count

    def _add_node(self, item: Any, parent: FPNode, count: int = 1) -> FPNode:
        new_node = FPNode(item, count, parent)
        parent.children[item] = new_node
        if item not in self.__header_table:
            self.__header_table[item] = []
        self.__header_table[item].append(new_node)
        self.__node_count += 1
        return new_node

    def insert(self, transaction: Transaction) -> None:
        current_node = self.__root
        for item, count in transaction:
            if item in current_node.children:
                current_node.children[item].count += count
            else:
                self._add_node(item, current_node, count)
            current_node = current_node.children[item]

    def has_single_path(self) -> bool:
        return count_leaf(self.__root) == 1

    def get_single_path(self) -> list[FPNode] | None:
        if not self.has_single_path():
            return None
        return visit(self.__root)


def get_all_leaf_paths(tree: FPTree) -> list[Transaction]:
    paths = []

    def dfs(node: FPNode, path: Transaction) -> None:
        current_entry = (node.item, node.count)
        if not node.children:
            paths.append(path + [current_entry])
            return
        for child in node.children.values():
            dfs(child, path + [current_entry])

    # Fix: was calling dfs(tree.root, []) which included root (item=None)
    # in every path, causing None to be inserted as an item on merge.
    for child in tree.root.children.values():
        dfs(child, [])

    return paths


def merge_trees(tree1: FPTree, tree2: FPTree) -> FPTree:
    def merge_nodes(node1: FPNode, node2: FPNode):
        for item, child2 in node2.children.items():
            if item in node1.children:
                # 1. Branch exists in tree1: Add the count
                child1 = node1.children[item]
                child1.count += child2.count

                # Recursively merge their children
                merge_nodes(child1, child2)
            else:
                # 2. Branch does not exist: Create a new node in tree1
                new_node = FPNode(item=item, count=child2.count, parent=node1)
                node1.children[item] = new_node

                # Update tree1's header table
                if item not in tree1.header_table:
                    tree1.header_table[item] = []
                tree1.header_table[item].append(new_node)

                # Recursively build out the rest of the branch
                merge_nodes(new_node, child2)

    # Start the recursive merge from the roots
    merge_nodes(tree1.root, tree2.root)

    return tree1


def visit(root: FPNode) -> list[FPNode]:
    if not root.children:
        return []
    nodes = []
    for child in root.children.values():
        nodes.append(child)
        nodes.extend(visit(child))
    return nodes


def count_leaf(root: FPNode) -> int:
    if not root.children:
        return 1
    return sum(count_leaf(child) for child in root.children.values())


def prefix_of(node: FPNode) -> list[FPNode]:
    """Returns the path from just below root up to (but not including) node."""
    path = []
    current = node.parent
    while current is not None and current.item is not None:
        path.append(current)
        current = current.parent
    return list(reversed(path))


def count_items(transactions: list[Transaction]) -> dict[Any, int]:
    map_count: dict[Any, int] = {}
    for trans in transactions:
        for item, count in trans:
            map_count[item] = map_count.get(item, 0) + count
    return map_count


def extract_frequent_itemsets(
    transactions: list[Transaction], min_support: int
) -> list[Transaction]:
    f_list = count_items(transactions)
    f_list_order = sorted(
        [item for item, count in f_list.items() if count >= min_support],
        key=lambda x: f_list[x],
        reverse=True,
    )
    item_order = {item: index for index, item in enumerate(f_list_order)}
    filtered_transactions = []
    for trans in transactions:
        filtered_trans = [(item, count) for item, count in trans if item in item_order]
        new_trans = sorted(filtered_trans, key=lambda x: item_order[x[0]])
        if new_trans:
            filtered_transactions.append(new_trans)
    return filtered_transactions


def combinations(items: list[Any]) -> Iterable[tuple[Any, ...]]:
    from itertools import combinations as it_combinations

    for r in range(1, len(items) + 1):
        yield from it_combinations(items, r)


def conditional_pattern_base(
    tree: FPTree, item: Any, min_support: int
) -> list[Transaction]:
    transactions = []
    for node in tree.header_table[item]:
        # Fix 1: filter on node.count (the item's support), not path[-1].count
        if node.count < min_support:
            continue
        path = prefix_of(node)
        if not path:
            continue
        # Fix 2: all prefix nodes get count = node.count, not their own counts.
        # The support of any pattern in this conditional database is bounded
        # by how many times the conditioned item itself appears.
        transaction = [(n.item, node.count) for n in path]
        transactions.append(transaction)
    return transactions


def construct_conditional_tree(tree: FPTree, item: Any, min_support: int) -> FPTree:
    pattern_base = conditional_pattern_base(tree, item, min_support=min_support)
    conditional_tree = FPTree()
    for transaction in pattern_base:
        conditional_tree.insert(transaction)
    return conditional_tree


def fptree_from_transactions(
    transactions: list[Transaction], min_support: int
) -> FPTree:
    tree = FPTree()
    frequent_itemsets = extract_frequent_itemsets(transactions, min_support)
    for transaction in frequent_itemsets:
        tree.insert(transaction)
    return tree


def fp_growth(
    tree: FPTree, min_support: int, suffix: tuple = ()
) -> Iterator[PatternWithSupport]:
    # Fix: single-path case now checks support per combination and returns it.
    # Previously it emitted all combinations without support tracking.
    if path_nodes := tree.get_single_path():
        path_nodes = [n for n in path_nodes if n.item is not None]
        patterns: set[PatternWithSupport] = set()
        for comb in combinations(path_nodes):
            support = min(n.count for n in comb)
            if support >= min_support:
                pattern = tuple(n.item for n in comb) + suffix
                yield (support, pattern)
        return

    patterns = set()
    for item, nodes in tree.header_table.items():
        item_support = sum(n.count for n in nodes)
        if item_support < min_support:
            continue
        pattern = (item,) + suffix
        # Fix: now returns (pattern, support) pairs instead of bare patterns
        patterns.add((item_support, pattern))
        conditional_tree = construct_conditional_tree(tree, item, min_support)
        if conditional_tree.node_count == 0:
            continue
        yield from fp_growth(conditional_tree, min_support, pattern)


def write_dot(root: FPNode, path: str) -> None:
    with open(path, "w") as f:
        f.write(to_dot(root))


def to_dot(root: FPNode) -> str:
    lines = ["digraph FPTree {", "    node [shape=circle];"]
    node_id = [0]

    def assign_ids(node: FPNode) -> int:
        nid = node_id[0]
        node_id[0] += 1
        label = "null" if node.item is None else f"{node.item}:{node.count}"
        lines.append(f'    {nid} [label="{label}"];')
        for child in node.children.values():
            cid = assign_ids(child)
            lines.append(f"    {nid} -> {cid};")
        return nid

    assign_ids(root)
    lines.append("}")
    return "\n".join(lines)
