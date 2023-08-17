from microcosm.api import binding
from sqlalchemy.orm import sessionmaker


@binding("sessionmakers")
def configure_sessionmakers(graph):
    """
    Create the SQLAlchemy session class.

    """

    return {name: sessionmaker(shard) for name, shard in graph.shards.items()}
