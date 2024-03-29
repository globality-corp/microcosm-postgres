"""
Test sessionmaker factory

"""
import json
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
)

from microcosm.api import (
    create_object_graph,
    load_each,
    load_from_dict,
    load_from_environ,
)
from microcosm.object_graph import ObjectGraph
from microcosm.registry import _registry
from pytest import fixture, mark
from sqlalchemy.orm import sessionmaker

from microcosm_postgres.constants import (
    GLOBAL_SHARD_NAME,
    X_REQUEST_CLIENT_HEADER,
    X_REQUEST_SHARD_HEADER,
)
from microcosm_postgres.factories.shards import configure_sharded_sessionmaker
from microcosm_postgres.shards import recreate_all
from microcosm_postgres.tests.fixtures import Company
from microcosm_postgres.tests.fixtures.company import CompanyType


SHARDED_CLIENT = "abcd1234"


@fixture
def loader():
    """Simulate the multi-sharded environment

    This is done by using a new db name instead of a new host.
    """
    return load_each(
        load_from_dict(
            secret=dict(
                postgres=dict(
                    host="127.0.0.1",
                ),
            ),
            shards={
                GLOBAL_SHARD_NAME: dict(
                    postgres=dict(
                        host="127.0.0.1",
                        username="example",
                        password="",
                    )
                ),
                "secondary": dict(
                    postgres=dict(
                        host="127.0.0.1",
                        database_name="example_test_secondary_db",
                        username="example",
                        password="",
                    )
                ),
            },
            client_shard=dict(mapping=json.dumps({SHARDED_CLIENT: "secondary"})),
        ),
        load_from_environ,
    )


@fixture(autouse=True)
def patch_sessionmaker():
    _registry.factories["sessionmaker"] = configure_sharded_sessionmaker
    try:
        yield
    finally:
        del _registry.factories["sessionmaker"]


@fixture
def graph(loader: Any) -> ObjectGraph:
    return create_object_graph(
        name="example",
        testing=True,
        loader=loader,
        import_name="microcosm_postgres",
    )


def test_load_bad_shard_configuration():
    loader = load_from_dict(
        secret=dict(
            postgres=dict(
                host="127.0.0.1",
            ),
        ),
        shards={
            GLOBAL_SHARD_NAME: dict(
                postgres=dict(
                    host="127.0.0.1",
                    username="example",
                    password="",
                )
            ),
            "secondary": dict(
                postgres=dict(
                    host="127.0.0.1",
                    database_name="example_test_secondary_db",
                )
            ),
        },
        client_shard=dict(mapping=json.dumps({SHARDED_CLIENT: "secondary"})),
    )
    graph = create_object_graph(
        name="example",
        testing=True,
        loader=loader,
        import_name="microcosm_postgres",
    )
    assert graph.shards.keys() == {GLOBAL_SHARD_NAME}


def test_load_bad_global_shard_configuration():
    loader = load_from_dict(
        secret=dict(
            postgres=dict(
                host="127.0.0.1",
            ),
        ),
        shards={
            GLOBAL_SHARD_NAME: dict(postgres=dict()),
            "secondary": dict(
                postgres=dict(
                    host="127.0.0.1",
                    database_name="example_test_secondary_db",
                    username="example",
                    password="",
                )
            ),
        },
        client_shard=dict(mapping=json.dumps({SHARDED_CLIENT: "secondary"})),
    )
    graph = create_object_graph(
        name="example",
        testing=True,
        loader=loader,
        import_name="microcosm_postgres",
    )
    assert graph.shards.keys() == {GLOBAL_SHARD_NAME}


@fixture(autouse=True)
def setup_db(graph: ObjectGraph) -> None:
    recreate_all(graph)


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
    assert graph.shards.keys() == {GLOBAL_SHARD_NAME, "secondary"}


def test_configure_sessionmakers(graph: ObjectGraph) -> None:
    assert graph.sessionmakers.keys() == {GLOBAL_SHARD_NAME, "secondary"}
    assert all(isinstance(sm, sessionmaker) for sm in graph.sessionmakers.values())


@mark.parametrize(
    ("opaque", "shard_name"),
    [
        ({}, GLOBAL_SHARD_NAME),
        ({X_REQUEST_CLIENT_HEADER: SHARDED_CLIENT}, "secondary"),
        ({X_REQUEST_CLIENT_HEADER: "other-clients"}, GLOBAL_SHARD_NAME),
        ({X_REQUEST_SHARD_HEADER: "secondary"}, "secondary"),
        (
            {
                X_REQUEST_SHARD_HEADER: GLOBAL_SHARD_NAME,
                X_REQUEST_CLIENT_HEADER: SHARDED_CLIENT,
            },
            GLOBAL_SHARD_NAME,
        ),
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
