import os
from copy import copy


def get_shard_names(graph):
    selected = os.environ.get("SHARD")
    return [selected] if selected else list(graph.shards.keys())


class subgraph:
    def __init__(self, graph, **overrides):
        self.graph = graph
        self.overrides = overrides

    def __getattr__(self, name):
        if name in self.overrides:
            return self.overrides[name]
        return getattr(self.graph, name)


def create_shard_specific_config(graph, shard_name):
    new_config = copy(graph.config)
    new_config.postgres = graph.config.shards[shard_name].postgres
    new_config.shards = {}
    return new_config


def create_shard_specific_graph(graph, shard_name):
    """
    Create a new graph with a specific shard.

    """
    return subgraph(
        graph,
        config=create_shard_specific_config(graph, shard_name),
        postgres=graph.shards[shard_name],
        sessionmaker=graph.sessionmakers[shard_name],
    )


def subgraphs(graph):
    for name in get_shard_names(graph):
        yield create_shard_specific_graph(graph, name)
