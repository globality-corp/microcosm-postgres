"""
Test engine routing strategies.

"""
from hamcrest import (
    assert_that,
    equal_to,
    instance_of,
    is_,
)
from microcosm.api import create_object_graph, load_from_dict
from sqlalchemy.engine.base import Engine

from microcosm_postgres.tests.fixtures import Employee, EmployeeData


class TestEngineRoutingStrategy:

    def setup_method(self):
        loader = load_from_dict(
            secret=dict(
                postgres=dict(
                    host="127.0.0.1",
                ),
            ),
            sessionmaker=dict(
                engine_routing_strategy="model_engine_routing_strategy",
            ),
        )
        self.graph = create_object_graph(name="example", testing=True, loader=loader)
        self.graph.use("sessionmaker")

    def test_default_engine_routing_strategy(self):
        strategy = self.graph.default_engine_routing_strategy
        engine = strategy.get_bind(None)

        assert_that(engine, is_(instance_of(Engine)))

        assert_that(
            str(engine.url),
            is_(equal_to("postgresql://example:***@localhost:5432/example_test_db")),
        )

    def test_model_engine_routing_strategy(self):
        strategy = self.graph.model_engine_routing_strategy

        engine = strategy.get_bind(Employee.__mapper__)
        assert_that(engine, is_(instance_of(Engine)))
        assert_that(
            str(engine.url),
            is_(equal_to("postgresql://example:***@localhost:5432/example_test_db")),
        )

        engine = strategy.get_bind(EmployeeData.__mapper__)
        assert_that(engine, is_(instance_of(Engine)))
        assert_that(
            str(engine.url),
            is_(equal_to("postgresql://example:***@127.0.0.1:5432/example_test_db")),
        )
