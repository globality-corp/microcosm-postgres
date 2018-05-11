from microcosm.api import defaults
from microcosm.config.types import boolean
from microcosm.config.validation import typed
from microcosm.scoping.factories import ScopedFactory
from microcosm.scoping.proxies import ScopedProxy
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker


ENGINE = "__engine__"


def unwrap(bind):
    return bind.__component__ if isinstance(bind, ScopedProxy) else bind


@defaults(
    # enable multiple engines (binds) via ScopedFactory
    multi=typed(boolean, default_value=False)
)
def configure_sessionmaker(graph):
    """
    Create the SQLAlchemy session class.

    """
    if graph.config.sessionmaker.multi:
        ScopedFactory.infect(graph, "postgres")

    class RoutingSession(Session):
        """
        Route session bind to an appropriate engine.

        See: http://docs.sqlalchemy.org/en/latest/orm/persistence_techniques.html#partitioning-strategies

        """
        def get_bind(self, mapper=None, clause=None):
            if mapper and mapper.class_:
                try:
                    engine_name = getattr(mapper.class_, ENGINE)
                    with graph.postgres.scoped_to(engine_name):
                        return unwrap(graph.postgres)
                except AttributeError:
                    pass

            return unwrap(graph.postgres)

    return sessionmaker(class_=RoutingSession)
