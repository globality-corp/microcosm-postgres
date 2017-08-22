"""
Directed Acyclic Graph of model objects.

"""
from collections import OrderedDict
from inspect import getmro

from inflection import underscore

from microcosm_postgres.cloning import clone, DEFAULT_IGNORE
from microcosm_postgres.toposort import toposorted


class Edge:
    def __init__(self, from_id, to_id):
        self.from_id = from_id
        self.to_id = to_id

    def _members(self):
        return

    def __eq__(self, other):
        return {self.from_id, self.to_id} == {other.from_id, other.to_id}

    def __ne__(self, other):
        return not self.__eq__(other)


class DAG:
    """
    A graph representation using a collection of nodes and edges.

    """
    def __init__(self, nodes, edges=None):
        """
        :param nodes: an iterable of `Model`; each must have a UUID `id`
        :param edges: an iterable of `Edge`

        """
        self.nodes = OrderedDict(
            (node.id, node)
            for node in nodes
        )
        self.edges = edges or []

    @classmethod
    def from_nodes(cls, *nodes):
        return cls(nodes=nodes).build_edges()

    @property
    def ordered_nodes(self):
        return toposorted(self.nodes, self.edges)

    @property
    def nodes_map(self):
        """
        Build a mapping from node type to a list of nodes.

        A typed mapping helps avoid polymorphism at non-persistent layers.

        """
        dct = dict()
        for node in self.nodes.values():
            cls = next(base for base in getmro(node.__class__) if "__tablename__" in base.__dict__)
            key = getattr(cls, "__alias__", underscore(cls.__name__))
            dct.setdefault(key, []).append(node)
        return dct

    def build_edges(self):
        """
        Build edges based on node `edges` property.

        Filters out any `Edge` not defined in the DAG.

        """
        self.edges = [
            edge
            for node in self.nodes.values()
            for edge in getattr(node, "edges", [])
            if edge.from_id in self.nodes and edge.to_id in self.nodes
        ]
        return self

    def clone(self, substitutions=None, ignore=DEFAULT_IGNORE):
        """
        Clone this dag using a set of substitutions.

        Traverse the dag in topological order.

        """
        substitutions = substitutions or {}
        nodes = [
            clone(node, substitutions, ignore)
            for node in toposorted(self.nodes, self.edges)
        ]
        return DAG.from_nodes(*nodes)
