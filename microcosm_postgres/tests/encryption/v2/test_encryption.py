from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, ClassVar, Iterator
from uuid import uuid4

from microcosm.api import (
    create_object_graph,
    load_each,
    load_from_dict,
    load_from_environ,
)
from microcosm.object_graph import ObjectGraph
from pytest import fixture, raises
from sqlalchemy import UUID, CheckConstraint, Table
from sqlalchemy.orm import Session, mapped_column, sessionmaker as SessionMaker

from microcosm_postgres.context import SessionContext
from microcosm_postgres.encryption.encryptor import MultiTenantEncryptor, SingleTenantEncryptor
from microcosm_postgres.encryption.v2.column import encryption
from microcosm_postgres.encryption.v2.encoders import (
    ArrayEncoder,
    Encoder,
    EnumEncoder,
    JSONEncoder,
    Nullable,
    StringEncoder,
)
from microcosm_postgres.encryption.v2.encryptors import AwsKmsEncryptor
from microcosm_postgres.models import Model
from microcosm_postgres.temporary import transient


class EmployeeType(Enum):
    FULL_TIME = "FULL_TIME"
    PART_TIME = "PART_TIME"


class Employee(Model):
    __tablename__ = "test_encryption_employee"
    if TYPE_CHECKING:
        __table__: ClassVar[Table]

    id = mapped_column(UUID, primary_key=True, default=uuid4)

    name = encryption("name", AwsKmsEncryptor(), StringEncoder())
    name_encrypted = name.encrypted()
    name_unencrypted = name.unencrypted(index=True)

    description = encryption(
        "description",
        AwsKmsEncryptor(),
        Nullable(StringEncoder()),
        default=None,
    )
    description_encrypted = description.encrypted()
    description_unencrypted = description.unencrypted()

    roles = encryption(
        "roles",
        AwsKmsEncryptor(),
        ArrayEncoder(StringEncoder()),
        default=list,
    )
    roles_encrypted = roles.encrypted()
    roles_unencrypted = roles.unencrypted()

    type = encryption("type", AwsKmsEncryptor(), EnumEncoder(EmployeeType))
    type_encrypted = type.encrypted()
    type_unencrypted = type.unencrypted()

    extras = encryption(
        "extras",
        AwsKmsEncryptor(),
        JSONEncoder(),
    )
    extras_encrypted = extras.encrypted()
    extras_unencrypted = extras.unencrypted()

    __table_args__ = (
        # NB check constraint to enforce null values in JSON columns
        CheckConstraint(
            name="employee_extras_or_encrypted_is_null",
            sqltext="extras IS NULL OR extras_encrypted IS NULL",
        ),
    )


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
        session.add(employee := Employee(
            name="foo",
            extras={"foo": "bar"},
            type=EmployeeType.FULL_TIME,
        ))
        assert employee.name_unencrypted is None
        assert employee.name_encrypted is not None
        assert employee.name == "foo"
        assert employee.extras == {"foo": "bar"}
        assert employee.type == EmployeeType.FULL_TIME


def test_encrypt_with_client_default(
    session: Session,
    single_tenant_encryptor: SingleTenantEncryptor,
) -> None:
    with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
        session.add(employee := Employee(name="foo"))
        session.flush()  # Defaults are applied post flush
        assert employee.description is None
        assert employee.description_unencrypted is None
        assert employee.description_encrypted is not None


def test_unencrypted_client_default(session: Session) -> None:
    session.add(employee := Employee(name="foo"))
    session.flush()  # Defaults are applied post flush
    assert employee.description is None
    assert employee.description_unencrypted is None
    assert employee.description_encrypted is None


def test_encrypt_with_client_default_factory(
    session: Session,
    single_tenant_encryptor: SingleTenantEncryptor,
) -> None:
    with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
        session.add(employee := Employee(name="foo"))
        session.flush()  # Defaults are applied post flush
        assert employee.roles == []
        assert employee.roles_unencrypted is None
        assert employee.roles_encrypted is not None


def test_add_encryption_to_existing(
    session: Session,
    single_tenant_encryptor: SingleTenantEncryptor,
) -> None:
    session.add(employee := Employee())
    employee.name = "foo"
    assert employee.name_encrypted is None
    assert employee.name_unencrypted == "foo"

    with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
        employee.name = "foo"
        assert employee.name_unencrypted is None
        assert employee.name_encrypted is not None


def test_remove_encryption_from_existing(
    session: Session,
    single_tenant_encryptor: SingleTenantEncryptor,
) -> None:
    session.add(employee := Employee())

    with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
        employee.name = "foo"
        assert employee.name_unencrypted is None
        assert employee.name_encrypted is not None

    employee.name = "foo"
    assert employee.name_encrypted is None
    assert employee.name_unencrypted == "foo"


def test_encode_none_on_non_nullable_raises_error(
    session: Session,
    single_tenant_encryptor: SingleTenantEncryptor,
) -> None:
    """
    Checks that if you pass in `None` when there is a default defined then
    the default is used.
    """
    with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
        session.add(employee := Employee())

        with raises(Encoder.EncodeException):
            employee.roles = None


def test_encrypt_with_transient_table(graph, single_tenant_encryptor: SingleTenantEncryptor):
    with (
        SessionContext(graph),
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor),
        transient(Employee) as transient_table,
    ):
        transient_table.insert_many([
            Employee(
                name="foo",
                extras={"foo": "bar"},
            ),
        ])
        # NB extras column check constraint ensures that encrypted and unencrypted
        #    columns are mutually exclusive
        transient_table.upsert_into(Employee)
        employees = transient_table.select_from(Employee)

        assert len(employees) == 1
        assert employees[0].extras == {"foo": "bar"}
