from copy import copy
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, Iterable, TypeAlias

Transaction: TypeAlias = list[tuple[Any, int]]

@dataclass
class FPNode:
    item: Any
    count: int = 1
    parent: 'FPNode' = None
    children: dict[Any, 'FPNode'] = field(default_factory=dict)

class FPTree:
    __root: FPNode
    __header_table: dict
    __leaf_count: int
    __node_count: int = 0

    def __init__(self):
        self.__root = FPNode(None)
        self.__header_table = {}
        self.__leaf_count = 1
        self.__node_count = 0

    @property
    def root(self):
        return copy(self.__root)

    @cached_property
    def header_table(self):
        return copy(self.__header_table)

    @property
    def node_count(self) -> int:
        return self.__node_count

    def __add_node(self, item: Any, parent: FPNode, count: int = 1) -> FPNode:
        if item in parent.children:
            return parent.children[item]

        new_node = FPNode(item, count, parent)
        parent.children[item] = new_node
        if item in self.__header_table:
            self.__header_table[item].append(new_node)
        else:
            self.__header_table[item] = [new_node]

        if len(parent.children) > 1:
            self.__leaf_count += 1

        self.__node_count += 1

        return new_node

    def insert(self, transaction: Transaction):
        current_node = self.__root
        for item, count in transaction:
            if item in current_node.children:
                current_node.children[item].count += count
            else:
                self.__add_node(item, current_node, count)
            current_node = current_node.children[item]

    def has_single_path(self) -> bool:
        return count_leaf(self.__root) == 1

    def get_single_path(self) -> list[FPNode] | None:
        if not self.has_single_path():
            return None

        return visit(self.__root)

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

def get_path_to(tree: FPTree, node: FPNode) -> list[FPNode]:
    if node is None:
        return []
    if node == tree.root:
        return [tree.root]
    return get_path_to(tree, node.parent) + [node]

def count_items(transactions: list[Transaction]) -> dict[Any, int]:
    map_count = {}
    for trans in transactions:
        for item, count in trans:
            map_count[item] = map_count.get(item, 0) + count
    return map_count

def extract_frequent_itemsets(transactions: list[Transaction], min_support: int) -> list[Transaction]:
    f_list = count_items(transactions)
    f_list_order = sorted([item for item, count in f_list.items() if count >= min_support], key=lambda x: f_list[x], reverse=True)
    item_order = {item: index for index, item in enumerate(f_list_order)}
    filtered_transactions = []
    for trans in transactions:
        filtered_trans = filter(lambda item: item[0] in item_order, trans)
        new_trans = sorted(filtered_trans, key=lambda x: item_order.get(x[0], -1))
        if new_trans:
            filtered_transactions.append(new_trans)

    return filtered_transactions


def combinations(items: list[Any]) -> Iterable[tuple[Any]]:
    from itertools import combinations as it_combinations
    for r in range(1, len(items) + 1):
        yield from it_combinations(items, r)


def conditional_pattern_base(tree: FPTree, item: Any, min_support: int) -> list[Transaction]:
    transactions = []
    for node in tree.header_table[item]:
        path = get_path_to(tree, node.parent)
        if not path:
            continue
        if path[-1].count < min_support:
            continue
        transaction = [(n.item, n.count) for n in path if n.item is not None]
        transactions.append(transaction)

    return transactions


def construct_conditional_tree(tree: FPTree, item: Any, min_support: int) -> FPTree:
    pattern_base = conditional_pattern_base(tree, item, min_support=min_support)
    conditional_tree = FPTree()
    for transaction in pattern_base:
        conditional_tree.insert(transaction)
    return conditional_tree

def fptree_from_transactions(transactions: list[Transaction], min_support: int) -> FPTree:
    tree = FPTree()
    frequent_itemsets = extract_frequent_itemsets(transactions, min_support)
    for transaction in frequent_itemsets:
        tree.insert(transaction)
    return tree

def fp_growth(tree: FPTree, min_support: int, suffix: tuple | None = None) -> set[list[Any]]:
    if suffix is None:
        suffix = tuple()

    if tree.has_single_path():
        path = tree.get_single_path()
        path_items = [node.item for node in path if node is not None and node.item is not None]
        patterns = set()
        for comb in combinations(path_items):
            pattern = comb + suffix
            patterns.add(pattern)
        return patterns

    patterns = set()
    for item, nodes in tree.header_table.items():
        patterns.add((item,) + suffix)
        conditional_tree = construct_conditional_tree(tree, item, min_support)
        if conditional_tree.node_count == 0:
            continue
        patterns |= fp_growth(conditional_tree, min_support, (item,) + suffix)

    return patterns

def write_dot(root: FPNode, path: str) -> None:
    with open(path, "w") as f:
        f.write(to_dot(root))

def to_dot(root: FPNode) -> str:
    lines = ["digraph FPTree {", '    node [shape=circle];']
    node_id = [0]

    def assign_ids(node: FPNode) -> int:
        nid = node_id[0]
        node_id[0] += 1
        label = f"null" if node.item is None else f"{node.item}:{node.count}"
        lines.append(f'    {nid} [label="{label}"];')
        for child in node.children.values():
            cid = assign_ids(child)
            lines.append(f"    {nid} -> {cid};")
        return nid

    assign_ids(root)
    lines.append("}")
    return "\n".join(lines)
