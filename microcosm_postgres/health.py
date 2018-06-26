"""
Simple Postgres health check.

"""
from alembic.script import ScriptDirectory

from microcosm_postgres.context import SessionContext


def check_health(graph):
    """
    Basic database health check.

    """
    SessionContext.session.execute("SELECT 1;")


def check_alembic(graph):
    """
    Check connectivity to an alembic database.

    Returns the current migration.

    """
    return SessionContext.session.execute(
        "SELECT version_num FROM alembic_version LIMIT 1;",
    ).scalar()


def get_current_head_version(graph):
    """
    Returns the current head version.

    """
    script_dir = ScriptDirectory("/", version_locations=[graph.metadata.get_path("migrations")])
    return script_dir.get_current_head()
