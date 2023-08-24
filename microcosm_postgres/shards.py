from . import createall, migrate, operations
from .sharded_subgraph import create_shard_specific_graph  # noqa: F401
from .sharded_subgraph import get_shard_names, subgraphs


def migrate_command(graph, *args):
    """
    Run migrations for all shards.

    Deprecated: Use `migrate.main` instead.
    """
    # Backwards compatibility
    return migrate.main(graph, *args)


def createall_command(graph):
    """
    Create all databases.
    """
    for subgraph in subgraphs(graph):
        createall.main(subgraph)


def recreate_all(graph):
    """
    Drop all databases and recreate them.
    """
    for subgraph in subgraphs(graph):
        operations.recreate_all(subgraph)


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
