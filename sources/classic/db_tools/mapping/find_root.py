# Taked from Google Search AI

from collections import defaultdict


def find_root_in_dag(graph_adj):
    """
    Finds the root of a Directed Acyclic Graph (DAG).
    The root is defined as a node with an in-degree of 0, from which all other nodes are reachable.

    Args:
        graph_adj (dict): An adjacency list representation of the graph,
                          where keys are nodes and values are lists of their neighbors.
                          Example: {0: [1, 2], 1: [3], 2: [3], 3: []}

    Returns:
        int or None: The root node if a unique root exists, None otherwise.
    """

    in_degree = defaultdict(int)
    nodes = set()

    # Calculate in-degrees for all nodes
    for node, neighbors in graph_adj.items():
        nodes.add(node)
        for neighbor in neighbors:
            in_degree[neighbor] += 1
            nodes.add(neighbor)

    root_candidates = []
    for node in nodes:
        if in_degree[node] == 0:
            root_candidates.append(node)

    if len(root_candidates) == 1:
        return root_candidates[0]
    else:
        # No unique root (either multiple sources or no nodes with in-degree 0)
        return None


# # Example Usage:
# graph1 = {0: [1, 2], 1: [3], 2: [3], 3: []}  # Root is 0
# graph2 = {1: [2], 2: [3], 3: []}  # Root is 1
# graph3 = {0: [1], 2: [3]}  # No single root (disconnected)
# graph4 = {0: [1], 1: [0]}  # Cyclic graph, no root
#
# print(f"Root of graph1: {find_root_in_dag(graph1)}")
# print(f"Root of graph2: {find_root_in_dag(graph2)}")
# print(f"Root of graph3: {find_root_in_dag(graph3)}")
# print(f"Root of graph4: {find_root_in_dag(graph4)}")
