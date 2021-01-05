"""
Factory tests.

"""
from microcosm.api import create_object_graph
from sqlalchemy.engine.base import Engine


def test_configure_engine():
    """
    Engine factory should work with zero configuration.

    """
    graph = create_object_graph(name="example", testing=True)
    engine = graph.postgres

    assert isinstance(engine, Engine)

    # engine has expected configuration
    assert str(engine.url).startswith("postgresql://example:@")
    assert str(engine.url).endswith(":5432/example_test_db")

    # engine supports connections
    with engine.connect() as connection:
        row = connection.execute("SELECT 1;").fetchone()
        assert row[0] == 1
