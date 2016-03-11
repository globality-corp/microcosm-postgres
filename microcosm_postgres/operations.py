"""
Common database operations.

"""
from microcosm_postgres.migrate import main
from microcosm_postgres.models import Model


def stamp_head(graph):
    """
    Stamp the database with the current head revision.

    """
    main(graph, "stamp", "head")


def create_all(graph):
    """
    Create all database tables.

    """
    Model.metadata.create_all(graph.postgres)
    stamp_head(graph)


def drop_all(graph):
    """
    Drop all database tables.

    """
    Model.metadata.drop_all(graph.postgres)


def recreate_all(graph):
    """
    Drop and add back all database tables.

    """
    drop_all(graph)
    create_all(graph)


def new_session(graph, expire_on_commit=False):
    """
    Create a new session.

    """
    return graph.sessionmaker(expire_on_commit=expire_on_commit)
