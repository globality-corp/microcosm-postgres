"""
Test topological sort.

"""
from hamcrest import assert_that, contains

from microcosm_postgres.dag import Edge
from microcosm_postgres.toposort import toposorted


class Node:
    def __init__(self, id):
        self.id = id


def test_toposort():
    nodes = dict(
        one=Node(id="one"),
        two=Node(id="two"),
        three=Node(id="three"),
        four=Node(id="four"),
    )
    edges = [
        Edge(from_id="one", to_id="two"),
        Edge(from_id="one", to_id="three"),
        Edge(from_id="two", to_id="three"),
        Edge(from_id="three", to_id="four"),
    ]

    assert_that(
        toposorted(nodes, edges),
        contains(
            nodes["one"],
            nodes["two"],
            nodes["three"],
            nodes["four"],
        ),
    )
