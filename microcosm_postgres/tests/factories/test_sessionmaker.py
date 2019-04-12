"""
Test sessionmaker factory

"""
from hamcrest import assert_that, instance_of, is_
from microcosm.api import create_object_graph
from sqlalchemy.orm import sessionmaker


def test_configure_sessionmaker():
    """
    Should create the `SQLAlchemy` sessionmaker

    """
    graph = create_object_graph(name="example", testing=True)
    assert_that(graph.sessionmaker, is_(instance_of(sessionmaker)))
