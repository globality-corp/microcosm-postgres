"""
Common database operations.

"""
from sqlalchemy import MetaData
from sqlalchemy.exc import ProgrammingError

from microcosm_postgres.migrate import main
from microcosm_postgres.models import Model


def stamp_head(graph):
    """
    Stamp the database with the current head revision.

    """
    main(graph, "stamp", "head")


def get_current_head(graph):
    """
    Get the current database head revision, if any.

    """
    session = new_session(graph)
    try:
        result = session.execute("SELECT version_num FROM alembic_version")
    except ProgrammingError:
        return None
    else:
        return result.scalar()
    finally:
        session.close()


def create_all(graph):
    """
    Create all database tables.

    """
    head = get_current_head(graph)
    if head is None:
        Model.metadata.create_all(graph.postgres)
        stamp_head(graph)


def drop_all(graph):
    """
    Drop all database tables.

    """
    Model.metadata.drop_all(graph.postgres)
    drop_alembic_table(graph)


def drop_alembic_table(graph):
    """
    Drop the alembic version table.

    """
    try:
        graph.postgres.execute("DROP TABLE alembic_version;")
    except ProgrammingError:
        return False
    else:
        return True


# Cached database metadata instance
_metadata = None


def recreate_all(graph):
    """
    Drop and add back all database tables, or reset all data associated with a database.
    Intended mainly for testing, where a test database may either need to be re-initialized
    or cleared out between tests

    """

    global _metadata

    if _metadata is None:
        # First-run, the test database/metadata needs to be initialized
        drop_all(graph)
        create_all(graph)

        _metadata = MetaData(bind=graph.postgres)
        _metadata.reflect()
        return

    # Otherwise, truncate all existing tables
    connection = graph.postgres.connect()
    transaction = connection.begin()
    for table in reversed(_metadata.sorted_tables):
        connection.execute(table.delete())
    transaction.commit()


def new_session(graph, expire_on_commit=False):
    """
    Create a new session.

    """
    return graph.sessionmaker(expire_on_commit=expire_on_commit)
