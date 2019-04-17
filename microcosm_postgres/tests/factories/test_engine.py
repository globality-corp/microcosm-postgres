"""
Factory tests.

"""
from hamcrest import (
    assert_that,
    ends_with,
    equal_to,
    instance_of,
    is_,
    starts_with,
)
from microcosm.api import create_object_graph
from sqlalchemy.engine.base import Engine


def test_configure_engine():
    """
    Engine factory should work with zero configuration.

    """
    graph = create_object_graph(name="example", testing=True)
    engine = graph.postgres

    assert_that(engine, is_(instance_of(Engine)))

    # engine has expected configuration
    assert_that(
        str(engine.url),
        starts_with("postgresql://example:@"),
    )

    assert_that(
        str(engine.url),
        ends_with(":5432/example_test_db"),
    )

    # engine supports connections
    with engine.connect() as connection:
        row = connection.execute("SELECT 1;").fetchone()
        assert_that(row[0], is_(equal_to(1)))
