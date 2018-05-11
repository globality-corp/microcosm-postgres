from sqlalchemy.orm import sessionmaker


def configure_sessionmaker(graph):
    """
    Create the SQLAlchemy session class.

    """
    return sessionmaker(bind=graph.postgres)
