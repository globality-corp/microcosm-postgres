"""
Factory that configures SQLAlchemy for PostgreSQL.

"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from microcosm.api import binding, defaults


@binding("postgres")
@defaults(
    host="localhost",
    port=5432,
    password="secret",
)
def configure_sqlalchemy_engine(graph):
    """
    Create the SQLAlchemy engine.

    """
    # use different database name for testing
    if graph.metadata.testing:
        database_name = "{}_test_db".format(graph.metadata.name)
    else:
        database_name = "{}_db".format(graph.metadata.name)

    # use the metadata name as the username
    username = graph.metadata.name
    password = graph.config.postgres.password or ""

    uri = "postgresql://{}:{}@{}:{}/{}".format(
        username,
        password,
        graph.config.postgres.host,
        graph.config.postgres.port,
        database_name,
    )

    return create_engine(uri)


def configure_sqlalchemy_sessionmaker(graph):
    """
    Create the SQLAlchemy session class.

    """
    return sessionmaker(bind=graph.postgres)
