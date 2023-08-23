import os
from copy import copy

from . import createall, migrate, operations


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


def get_shard_names(graph):
    selected = os.environ.get("SHARD")
    return [selected] if selected else list(graph.shards.keys())


def migrate_command(graph, *args):
    """
    Run migrations for all shards.
    """
    for name in get_shard_names(graph):
        migrate.main(create_shard_specific_graph(graph, name), *args)


def createall_command(graph):
    """
    Create all databases.
    """
    for name in get_shard_names(graph):
        createall.main(create_shard_specific_graph(graph, name))


def recreate_all(graph):
    """
    Drop all databases and recreate them.
    """
    for name in get_shard_names(graph):
        operations.recreate_all(create_shard_specific_graph(graph, name))


def check_alembic(graph):
    """
    Drop all databases and recreate them.
    """

    results = {}
    for name in get_shard_names(graph):
        with graph.sessionmakers[name]() as session:
            results[name] = session.execute(
                "SELECT version_num FROM alembic_version LIMIT 1;"
            ).scalar()

    return results


def check_health(graph):
    """
    Drop all databases and recreate them.
    """

    results = {}
    for name in get_shard_names(graph):
        with graph.sessionmakers[name]() as session:
            results[name] = session.execute("SELECT 1;").scalar()

    return results
