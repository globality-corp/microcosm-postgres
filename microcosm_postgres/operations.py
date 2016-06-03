"""
Common database operations.

"""
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
