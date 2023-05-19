"""
Common database operations.

"""
from sqlalchemy import MetaData, text
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
        result = session.execute(text("SELECT version_num FROM alembic_version"))
    except ProgrammingError:
        return None
    else:
        return result.scalar()
    finally:
        session.close()


def create_all(graph, model_cls=Model):
    """
    Create all database tables.

    """
    head = get_current_head(graph)
    if head is None:
        model_cls.metadata.create_all(graph.postgres)
        stamp_head(graph)


def drop_all(graph, model_cls=Model):
    """
    Drop all database tables.

    """
    model_cls.metadata.drop_all(graph.postgres)
    drop_alembic_table(graph)


def drop_alembic_table(graph):
    """
    Drop the alembic version table.

    """
    try:
        with graph.postgres.connect() as connection:
            connection.execute(text("DROP TABLE alembic_version"))
            connection.commit()

    except ProgrammingError:
        return False
    else:
        return True


# Cached database metadata instance
_metadata = None


def recreate_all(graph, model_cls=Model):
    """
    Drop and add back all database tables, or reset all data associated with a database.
    Intended mainly for testing, where a test database may either need to be re-initialized
    or cleared out between tests

    """

    global _metadata

    if _metadata is None:
        # First-run, the test database/metadata needs to be initialized
        drop_all(graph, model_cls)
        create_all(graph, model_cls)
        _metadata = MetaData()
        _metadata.reflect(graph.postgres)

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
