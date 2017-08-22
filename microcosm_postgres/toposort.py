"""
Topological sort.

"""
from collections import defaultdict


def toposorted(nodes, edges):
    """
    Perform a topological sort on the input resources.

    The topological sort uses Kahn's algorithm, which is a stable sort and will preserve this
    ordering; note that a DFS will produce a worst case ordering from the perspective of batching.

    """
    incoming = defaultdict(set)
    outgoing = defaultdict(set)
    for edge in edges:
        incoming[edge.to_id].add(edge.from_id)
        outgoing[edge.from_id].add(edge.to_id)

    working_set = list(nodes.values())
    results = []
    while working_set:
        remaining = []
        for node in working_set:
            if incoming[node.id]:
                # node still has incoming edges
                remaining.append(node)
                continue

            results.append(node)
            for child in outgoing[node.id]:
                incoming[child].remove(node.id)

        if len(working_set) == len(remaining):
            raise Exception("Cycle detected")
        working_set = remaining

    return results
