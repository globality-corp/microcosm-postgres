"""
Common database operations.

"""
from microcosm_postgres.models import Model


def create_all(graph):
    """
    Create all database tables.

    """
    Model.metadata.create_all(graph.postgres)


def drop_all(graph):
    """
    Drop all database tables.

    """
    Model.metadata.drop_all(graph.postgres)


def new_session(graph, expire_on_commit=False):
    """
    Create a new session.

    """
    return graph.Session(expire_on_commit=expire_on_commit)
