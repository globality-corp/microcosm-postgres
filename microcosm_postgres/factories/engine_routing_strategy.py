from abc import ABCMeta, abstractmethod

from microcosm.scoping.proxies import ScopedProxy


def unwrap(bind):
    return bind.__component__ if isinstance(bind, ScopedProxy) else bind


class EngineRoutingStrategy(metaclass=ABCMeta):

    @abstractmethod
    def get_bind(self, mapper, clause=None):
        pass

    @property
    @abstractmethod
    def supports_multiple_binds(self):
        pass


class DefaultEngineRoutingStrategy(EngineRoutingStrategy):
    """
    A pluggable strategy for use in our `RoutingSession`.

    """
    def __init__(self, graph):
        self.graph = graph

    def get_bind(self, mapper, clause=None):
        """
        Implement `Session.get_bind`

        """
        return unwrap(self.graph.postgres)

    @property
    def supports_multiple_binds(self):
        """
        Does this strategy support multiple concurrent engine binds?

        """
        return False


class ModelEngineRoutingStrategy(EngineRoutingStrategy):
    """
    Route based on a name embedded in a model object.

    """
    KEY = "__engine__"

    def __init__(self, graph):
        self.graph = graph

    def get_bind(self, mapper, clause=None):
        if mapper and mapper.class_:
            try:
                engine_name = getattr(mapper.class_, ModelEngineRoutingStrategy.KEY)
                with self.graph.postgres.scoped_to(engine_name):
                    return unwrap(self.graph.postgres)
            except AttributeError:
                pass

        return unwrap(self.graph.postgres)

    @property
    def supports_multiple_binds(self):
        return True
