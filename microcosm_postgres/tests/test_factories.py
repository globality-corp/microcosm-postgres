"""
Factory tests.

"""
from hamcrest import (
    assert_that,
    instance_of,
    is_,
)

from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker

from microcosm.api import create_object_graph


def test_configure_sqlalchemy_engine():
    """
    Should create the `SQLAlchemy` engine

    """
    graph = create_object_graph("test")
    assert_that(graph.postgres, is_(instance_of(Engine)))


def test_configure_sqlalchemy_sessionmaker():
    """
    Should create the `SQLAlchemy` sessionmaker

    """
    graph = create_object_graph("test")
    assert_that(graph.Session, is_(instance_of(sessionmaker)))
