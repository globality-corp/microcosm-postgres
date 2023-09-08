from __future__ import annotations

from typing import TYPE_CHECKING, Iterator
from uuid import uuid4

from microcosm.api import (
    create_object_graph,
    load_each,
    load_from_dict,
    load_from_environ,
)
from microcosm.object_graph import ObjectGraph
from pytest import fixture
from sqlalchemy import (
    UUID,
    LargeBinary,
    String,
    Table,
)
from sqlalchemy.orm import Session, mapped_column, sessionmaker as SessionMaker
from microcosm_postgres.encryption.encryptor import (
    MultiTenantEncryptor,
    SingleTenantEncryptor,
)

from microcosm_postgres.encryption.v2.column import encryption
from microcosm_postgres.encryption.v2.encoders import StringEncoder
from microcosm_postgres.encryption.v2.encryptors import AwsKmsEncryptor
from microcosm_postgres.models import Model


class Employee(Model):
    __tablename__ = "test_encryption_employee"
    if TYPE_CHECKING:
        __table__: Table

    id = mapped_column(UUID, primary_key=True, default=uuid4)

    name_unencrypted = mapped_column("name", String, nullable=True)
    name_encrypted = mapped_column(LargeBinary, nullable=True)
    name = encryption("name", AwsKmsEncryptor(), StringEncoder())


client_id = uuid4()


@fixture(scope="module")
def config() -> dict:
    return dict(
        multi_tenant_key_registry=dict(
            context_keys=[
                str(client_id),
            ],
            key_ids=[
                "key_id",
            ],
            partitions=[
                "aws",
            ],
            account_ids=[
                "12345",
            ],
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
    Employee.__table__.drop(graph.postgres)
    Employee.__table__.create(graph.postgres)


@fixture
def multi_tenant_encryptor(graph: ObjectGraph) -> MultiTenantEncryptor:
    return graph.multi_tenant_encryptor


@fixture
def single_tenant_encryptor(
    multi_tenant_encryptor: MultiTenantEncryptor,
) -> SingleTenantEncryptor:
    return multi_tenant_encryptor.encryptors[str(client_id)]


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


def test_encrypt_no_context(session: Session) -> None:
    session.add(employee := Employee(name="foo"))
    assert employee.name_encrypted is None
    assert employee.name_unencrypted == "foo"


def test_encrypt_with_client(
    session: Session,
    single_tenant_encryptor: SingleTenantEncryptor,
) -> None:
    with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
        session.add(employee := Employee(name="foo"))
        assert employee.name_unencrypted is None
        assert employee.name_encrypted is not None
        assert employee.name == "foo"
