"""
Directed Acyclic Graph of model objects.

"""
from abc import ABCMeta, abstractmethod
from collections import namedtuple, OrderedDict
from inspect import getmro

from inflection import underscore

from microcosm_postgres.cloning import clone
from microcosm_postgres.toposort import toposorted


Edge = namedtuple("Edge", ["from_id", "to_id"])


class DAG:
    """
    A graph representation using a collection of nodes and edges that supports cloning.

    """
    def __init__(self, nodes, edges=None, substitutions=None):
        """
        :param nodes: an iterable of `Model`; each must have a UUID `id`
        :param edges: an iterable of `Edge`
        :param substitutions: a dictionary of (UUID) value substitutions

        """
        self.nodes = OrderedDict(
            (node.id, node)
            for node in nodes
        )
        self.edges = edges or []
        self.substitutions = substitutions or {}

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
            edge if isinstance(edge, Edge) else Edge(*edge)
            for node in self.nodes.values()
            for edge in getattr(node, "edges", [])
            if edge[0] in self.nodes and edge[1] in self.nodes
        ]
        return self

    def clone(self, ignore=()):
        """
        Clone this dag using a set of substitutions.

        Traverse the dag in topological order.

        """
        nodes = [
            clone(node, self.substitutions, ignore)
            for node in toposorted(self.nodes, self.edges)
        ]
        return DAG(nodes=nodes, substitutions=self.substitutions).build_edges()


class DAGCloner(metaclass=ABCMeta):
    """
    A process for building and cloning a DAG.

    The expected usage is:
      - Define a root object
      - Define a generate for child objects
      - Define extra nodes as needed
      - Clone the resulting DAG
      - Update nodes as needed

    """
    def __init__(self, graph):
        pass

    def explain(self, **kwargs):
        """
        Generate a "dry run" DAG of that state that WILL be cloned.

        """
        root = self.retrieve_root(**kwargs)
        children = self.iter_children(root, **kwargs)
        dag = DAG.from_nodes(root, *children)
        return self.add_edges(dag)

    def clone(self, substitutions, **kwargs):
        """
        Clone a DAG.

        """
        dag = self.explain(**kwargs)
        dag.substitutions.update(substitutions)
        cloned_dag = dag.clone(ignore=self.ignore)
        return self.update_nodes(self.add_edges(cloned_dag))

    @abstractmethod
    def retrieve_root(self, **kwargs):
        """
        Retrieve the root node for the DAG.

        :raises Forbidden: if the arguments to define a valid root object

        """
        pass

    @property
    def ignore(self):
        """
        Fields to ignore.

        """
        return ("created_at", "updated_at")

    def iter_children(self, root, **kwargs):
        """
        Generate child nodes for the root.

        """
        return
        yield

    def add_edges(self, dag):
        """
        Add edges using non-local node state.

        Example: import a sequential ordering based on a timestamp by adding edges between
        consecutive pairs of nodes.

        """
        return dag

    def update_nodes(self, dag):
        """
        Update nodes using non-local node state.

        Example: if one kind of node needs to update a non-UUID value based on the timestamp
        of another node.

        """
        return dag
