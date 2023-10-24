from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Callable,
    ClassVar,
    Iterator,
)
from unittest.mock import MagicMock
from uuid import UUID, uuid4

from microcosm.api import (
    create_object_graph,
    load_each,
    load_from_dict,
    load_from_environ,
)
from microcosm.object_graph import ObjectGraph
from pytest import fixture
from sqlalchemy import Table
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import (
    Mapped,
    Session,
    mapped_column,
    sessionmaker as SessionMaker,
)

from microcosm_postgres.encryption.encryptor import SingleTenantEncryptor
from microcosm_postgres.encryption.v2.column import encryption
from microcosm_postgres.encryption.v2.encoders import StringEncoder
from microcosm_postgres.encryption.v2.encryptors import AwsKmsEncryptor
from microcosm_postgres.models import Model


class Employee(Model):
    __tablename__ = "test_employee_multiple_encryptor"
    if TYPE_CHECKING:
        __table__: ClassVar[Table]

    id: Mapped[UUID] = mapped_column(default=uuid4, primary_key=True)
    name: encryption[str] = encryption("name", AwsKmsEncryptor(), StringEncoder())
    name_encrypted = name.encrypted()
    name_unencrypted = name.unencrypted()


client_ids = [uuid4(), uuid4(), uuid4()]


@fixture(scope="module")
def config() -> dict:
    return dict(
        multi_tenant_key_registry=dict(
            context_keys=[str(client_id) for client_id in client_ids],
            key_ids=[f"key_id_{i}" for i in range(len(client_ids))],
            partitions=["aws" for _ in (client_ids)],
            account_ids=[str(i) for i in range(len(client_ids))],
        ),
    )


@fixture(scope="module")
def graph(config: dict) -> ObjectGraph:
    return create_object_graph(
        "example",
        testing=True,
        loader=load_each(
            load_from_dict(config),
            load_from_environ,
        ),
    )


@fixture(autouse=True, scope="module")
def create_tables(graph: ObjectGraph) -> None:
    try:
        Employee.__table__.drop(graph.postgres)
    except ProgrammingError:
        ...
    Employee.__table__.create(graph.postgres)


@fixture
def encryptors(graph: ObjectGraph) -> dict[str, SingleTenantEncryptor]:
    encryptors = {
        context_key: MagicMock(wraps=encryptor)
        for context_key, encryptor in graph.multi_tenant_encryptor.encryptors.items()
    }
    graph.multi_tenant_encryptor.encryptors = encryptors
    return encryptors  # type: ignore


@fixture
def sessionmaker(graph: ObjectGraph) -> SessionMaker:
    return graph.sessionmaker


@fixture
def session(sessionmaker: SessionMaker) -> Iterator[Session]:
    session = sessionmaker()
    try:
        yield session
        session.flush()  # Check that flush works
    finally:
        session.rollback()
        session.close()


@fixture
def client_encryptor_used(
    encryptors: dict[str, SingleTenantEncryptor]
) -> Callable[[], set[UUID]]:
    return lambda: {
        client_id
        for client_id in client_ids
        if encryptors[str(client_id)].encrypt.called
    }


@fixture
def client_decryptor_used(
    encryptors: dict[str, SingleTenantEncryptor]
) -> Callable[[], set[UUID]]:
    return lambda: {
        client_id
        for client_id in client_ids
        if encryptors[str(client_id)].decrypt.called
    }


@fixture
def reset_encryptor_mock(
    encryptors: dict[str, SingleTenantEncryptor]
) -> Callable[[], None]:
    def _inner() -> None:
        for encryptor in encryptors.values():
            encryptor.reset_mock()  # type: ignore

    return _inner


@fixture(autouse=True)
def auto_reset_mock(reset_encryptor_mock: Callable[[], None]) -> Iterator[None]:
    yield
    reset_encryptor_mock()


def test_encrypt(
    session: Session,
    encryptors: dict[str, SingleTenantEncryptor],
    client_encryptor_used: Callable[[], set[UUID]],
) -> None:
    with AwsKmsEncryptor.set_encryptor_context("test", encryptors[str(client_ids[0])]):
        session.add(employee := Employee(name="foo"))
        assert employee.name_unencrypted is None
        assert employee.name_encrypted is not None
        assert employee.name == "foo"

    assert client_encryptor_used() == {client_ids[0]}


def test_reencrypt_with_different_client(
    session: Session,
    encryptors: dict[str, SingleTenantEncryptor],
    client_encryptor_used: Callable[[], set[UUID]],
    client_decryptor_used: Callable[[], set[UUID]],
    reset_encryptor_mock: Callable[[], None],
) -> None:
    session.add(employee := Employee(name="foo"))

    with AwsKmsEncryptor.set_encryptor_context("test", encryptors[str(client_ids[0])]):
        employee.name = "bar"
        assert employee.name == "bar"

    assert client_encryptor_used() == {client_ids[0]}
    assert client_decryptor_used() == {client_ids[0]}
    reset_encryptor_mock()

    with AwsKmsEncryptor.set_encryptor_context("test", encryptors[str(client_ids[1])]):
        employee.name = "baz"
        assert employee.name == "baz"

    assert client_encryptor_used() == {client_ids[1]}
    assert client_decryptor_used() == {client_ids[1]}
