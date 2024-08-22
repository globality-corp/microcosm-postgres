from __future__ import annotations

from collections.abc import Iterator
from enum import Enum
from typing import TYPE_CHECKING, ClassVar
from uuid import uuid4

from microcosm.api import (
    create_object_graph,
    load_each,
    load_from_dict,
    load_from_environ,
)
from microcosm.object_graph import ObjectGraph
from pytest import fixture, mark
from sqlalchemy import (
    UUID,
    Table,
    select,
)
from sqlalchemy.orm import Session, mapped_column, sessionmaker as SessionMaker

from microcosm_postgres.encryption.constants import ENCRYPTION_V2_DEFAULT_KEY
from microcosm_postgres.encryption.encryptor import (
    MultiTenantEncryptor,
    SingleTenantEncryptor,
)
from microcosm_postgres.encryption.v2.column import encryption
from microcosm_postgres.encryption.v2.encoders import ArrayEncoder, StringEncoder
from microcosm_postgres.encryption.v2.encryptors import AwsKmsEncryptor
from microcosm_postgres.models import Model


class EmployeeType(Enum):
    FULL_TIME = "FULL_TIME"
    PART_TIME = "PART_TIME"


class Employee(Model):
    __tablename__ = "test_beacon_employee"
    if TYPE_CHECKING:
        __table__: ClassVar[Table]

    id = mapped_column(UUID, primary_key=True, default=uuid4)

    name = encryption("name", AwsKmsEncryptor(), StringEncoder())
    name_encrypted = name.encrypted()
    name_unencrypted = name.unencrypted(index=True)

    roles = encryption(
        "roles", AwsKmsEncryptor(), ArrayEncoder(StringEncoder()), use_beacon_array=True
    )
    roles_encrypted = roles.encrypted()
    roles_unencrypted = roles.unencrypted()
    roles_beacon = roles.beacon()


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
    Employee.__table__.drop(graph.postgres)  # type: ignore
    Employee.__table__.create(graph.postgres)  # type: ignore


@fixture
def multi_tenant_encryptor(graph: ObjectGraph) -> MultiTenantEncryptor:
    return graph.multi_tenant_encryptor  # type: ignore


@fixture
def single_tenant_encryptor(
    multi_tenant_encryptor: MultiTenantEncryptor,
) -> SingleTenantEncryptor:
    return multi_tenant_encryptor.encryptors[str(client_id)]


@fixture
def default_tenant_encryptor(
    multi_tenant_encryptor: MultiTenantEncryptor,
) -> SingleTenantEncryptor:
    return multi_tenant_encryptor.encryptors[ENCRYPTION_V2_DEFAULT_KEY]


@fixture
def sessionmaker(graph: ObjectGraph) -> SessionMaker:
    return graph.sessionmaker  # type: ignore


@fixture
def session(sessionmaker: SessionMaker) -> Iterator[Session]:
    session = sessionmaker()
    try:
        yield session
        session.flush()  # Check that flush works
    finally:
        session.rollback()
        session.close()


@mark.parametrize(
    ("query", "roles", "expected"),
    [
        (["admin"], ["dev", "qa"], False),
        (["admin", "dev", "support"], ["dev", "support"], False),
        (["admin"], ["admin"], True),
        (["admin"], ["admin", "dev"], True),
    ],
)
def test_baecon_contains(
    session: Session,
    single_tenant_encryptor: SingleTenantEncryptor,
    roles: list[str],
    query: list[str],
    expected: bool,
) -> None:
    with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
        session.add(Employee(name="foo", roles=roles))
        session.flush()

        assert (
            session.execute(select(Employee.roles.contains(query))).scalar_one()
            == expected
        )


@mark.parametrize(
    ("roles", "query", "expected"),
    [
        (["admin"], ["dev", "qa"], False),
        (["admin", "dev", "support"], ["dev", "support"], False),
        (["admin"], ["admin"], True),
        (["admin"], ["admin", "dev"], True),
    ],
)
def test_baecon_contained_by(
    session: Session,
    single_tenant_encryptor: SingleTenantEncryptor,
    roles: list[str],
    query: list[str],
    expected: bool,
) -> None:
    with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
        session.add(Employee(name="foo", roles=roles))
        session.flush()

        assert (
            session.execute(select(Employee.roles.contained_by(query))).scalar_one()
            == expected
        )


@mark.parametrize(
    ("roles", "query", "expected"),
    [
        (["admin", "dev", "support"], ["dev", "qa"], True),
        (["admin", "dev", "support"], ["dev", "support"], True),
        (["admin", "dev", "support"], ["dev", "qa", "support"], True),
        (["admin", "dev", "support"], ["qa", "support"], True),
        (["admin", "dev", "support"], ["qa"], False),
        (["admin", "dev", "support"], ["qa", "hr"], False),
        (["admin", "dev", "support"], ["qa", "contractor"], False),
    ],
)
def test_baecon_overlap(
    session: Session,
    single_tenant_encryptor: SingleTenantEncryptor,
    roles: list[str],
    query: list[str],
    expected: bool,
) -> None:
    with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
        session.add(Employee(name="foo", roles=roles))
        session.flush()

        assert (
            session.execute(select(Employee.roles.overlap(query))).scalar_one()
            == expected
        )
