"""
Test sessionmaker factory

"""
import json
from typing import Callable, Dict, Iterator

from microcosm.api import create_object_graph, load_from_dict
from microcosm.object_graph import ObjectGraph
from microcosm.registry import _registry
from pytest import fixture, mark
from sqlalchemy.orm import sessionmaker

from microcosm_postgres.factories.shards import configure_sharded_sessionmaker
from microcosm_postgres.operations import recreate_all
from microcosm_postgres.shards import create_shard_specific_graph
from microcosm_postgres.tests.fixtures import Company
from microcosm_postgres.tests.fixtures.company import CompanyType


SHARDED_CLIENT = "abcd1234"


@fixture
def config() -> dict:
    """Simulate the multi-sharded environment

    This is done by using a new db name instead of a new host.
    """
    return dict(
        secret=dict(
            postgres=dict(
                host="127.0.0.1",
            ),
        ),
        shards={
            "global": dict(
                postgres=dict(
                    host="127.0.0.1",
                )
            ),
            "secondary": dict(
                postgres=dict(
                    host="127.0.0.1", database_name="example_test_secondary_db"
                )
            ),
        },
        client_shard=dict(mapping=json.dumps({SHARDED_CLIENT: "secondary"})),
    )


@fixture(autouse=True)
def patch_sessionmaker():
    _registry.factories["sessionmaker"] = configure_sharded_sessionmaker
    try:
        yield
    finally:
        del _registry.factories["sessionmaker"]


@fixture
def graph(config: dict) -> ObjectGraph:
    return create_object_graph(
        name="example",
        testing=True,
        loader=load_from_dict(config),
        import_name="microcosm_postgres",
    )


@fixture(autouse=True)
def setup_db(graph: ObjectGraph) -> None:
    recreate_all(create_shard_specific_graph(graph, "global"))
    recreate_all(create_shard_specific_graph(graph, "secondary"))


@fixture
def get_shards_for_query(graph: ObjectGraph) -> Callable[..., Iterator[str]]:
    def _get_shards_for_query(model_class, *criterion):
        for name, sm in graph.sessionmakers.items():
            with sm() as session:
                obj = session.query(model_class).filter(*criterion).one_or_none()
                if obj is None:
                    continue

                yield name

    return _get_shards_for_query


def test_configure_shards(graph: ObjectGraph) -> None:
    assert graph.shards.keys() == {"global", "secondary"}


def test_configure_sessionmakers(graph: ObjectGraph) -> None:
    assert graph.sessionmakers.keys() == {"global", "secondary"}
    assert all(isinstance(sm, sessionmaker) for sm in graph.sessionmakers.values())


@mark.parametrize(
    ("opaque", "shard_name"),
    [
        ({}, "global"),
        ({"x-request-client": SHARDED_CLIENT}, "secondary"),
        ({"x-request-client": "other-clients"}, "global"),
        ({"x-request-shard": "secondary"}, "secondary"),
        ({"x-request-shard": "global", "x-request-client": SHARDED_CLIENT}, "global"),
    ],
)
def test_create_company(
    graph: ObjectGraph,
    get_shards_for_query: Callable[..., Iterator[str]],
    opaque: Dict,
    shard_name: str,
) -> None:
    with graph.opaque.initialize(lambda: opaque):
        with graph.sessionmaker() as session, session.begin():
            session.add(company := Company(name="name", type=CompanyType.public))
            session.flush()
            company_id = company.id

    assert set(get_shards_for_query(Company, Company.id == company_id)) == {shard_name}
