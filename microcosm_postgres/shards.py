import os
from copy import copy

from . import createall, migrate


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
    del new_config.shards
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


def migrate_command(graph, *args):
    """
    Run migrations for all shards.
    """
    selected = os.environ.get("SHARD")
    shards = [selected] if selected else graph.shards.keys()

    for name in shards:
        migrate.main(create_shard_specific_graph(graph, name), *args)


def createall_command(graph):
    """
    Create all databases.
    """
    selected = os.environ.get("SHARD")
    shards = [selected] if selected else graph.shards.keys()

    for name in shards:
        createall.main(create_shard_specific_graph(graph, name))
