from microcosm.api import defaults
from microcosm.scoping.factories import ScopedFactory
from sqlalchemy.orm import Session, sessionmaker


@defaults(
    engine_routing_strategy="default_engine_routing_strategy",
)
def configure_sessionmaker(graph):
    """
    Create the SQLAlchemy session class.

    """
    engine_routing_strategy = getattr(graph, graph.config.sessionmaker.engine_routing_strategy)

    if engine_routing_strategy.supports_multiple_binds:
        ScopedFactory.infect(graph, "postgres")

    class RoutingSession(Session):
        """
        Route session bind to an appropriate engine.

        See: http://docs.sqlalchemy.org/en/latest/orm/persistence_techniques.html#partitioning-strategies

        """
        def get_bind(self, mapper=None, clause=None):
            return engine_routing_strategy.get_bind(mapper, clause)

    return sessionmaker(class_=RoutingSession)
