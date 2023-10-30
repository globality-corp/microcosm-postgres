from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Iterator,
)
from uuid import UUID, uuid4

from microcosm.api import (
    create_object_graph,
    load_each,
    load_from_dict,
    load_from_environ,
)
from microcosm.object_graph import ObjectGraph
from pytest import fixture, mark
from sqlalchemy import Table
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import (
    Mapped,
    Session,
    mapped_column,
    sessionmaker as SessionMaker,
)

from microcosm_postgres.encryption.encryptor import SingleTenantEncryptor
from microcosm_postgres.encryption.v2 import encoders
from microcosm_postgres.encryption.v2.column import encryption
from microcosm_postgres.encryption.v2.encryptors import AwsKmsEncryptor
from microcosm_postgres.models import Model


client_ids = [uuid4(), uuid4()]


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


@fixture
def encryptors(graph: ObjectGraph) -> dict[str, SingleTenantEncryptor]:
    return graph.multi_tenant_encryptor.encryptors


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


class AnEnum(Enum):
    FOO = "FOO"
    BAR = "BAR"


@mark.parametrize(
    ("encoder", "value"),
    [
        (encoders.StringEncoder(), "foo"),
        (encoders.TextEncoder(), "foo"),
        (encoders.IntEncoder(), 5000),
        (encoders.DecimalEncoder(), Decimal("1.5")),
        (encoders.ArrayEncoder(encoders.StringEncoder()), ["foo", "bar"]),
        (encoders.ArrayEncoder(encoders.IntEncoder()), [1, 2]),
        (encoders.JSONEncoder(), {"foo": "bar", "something_else": []}),
        (encoders.Nullable(encoders.StringEncoder()), None),
        (encoders.Nullable(encoders.StringEncoder()), "foo"),
        (encoders.DatetimeEncoder(), datetime.now()),
        (encoders.EnumEncoder(AnEnum), AnEnum.FOO),
        (encoders.Nullable(encoders.EnumEncoder(AnEnum)), None),
        (encoders.Nullable(encoders.EnumEncoder(AnEnum)), AnEnum.FOO),
    ],
)
def test_redacted_value_used(
    graph: ObjectGraph,
    session: Session,
    encryptors: dict[str, SingleTenantEncryptor],
    encoder: encoders.Encoder,
    value: Any,
) -> None:
    class TestModel(Model):
        __tablename__ = "test_employee_redacted"
        __table_args__ = {"extend_existing": True}

        if TYPE_CHECKING:
            __table__: ClassVar[Table]

        id: Mapped[UUID] = mapped_column(default=uuid4, primary_key=True)
        field: encryption[str] = encryption("field", AwsKmsEncryptor(), encoder)
        field_encrypted = field.encrypted()
        field_unencrypted = field.unencrypted()

    try:
        TestModel.__table__.drop(graph.postgres)
    except ProgrammingError:
        ...
    TestModel.__table__.create(graph.postgres)

    # Encrypt data with client1's keys only
    with AwsKmsEncryptor.set_encryptor_context("test", encryptors[str(client_ids[0])]):
        session.add(model := TestModel(field=value))

    # Attempt to decrypt data with the wrong set of keys to simulate disabled client key
    with AwsKmsEncryptor.set_encryptor_context("test", encryptors[str(client_ids[1])]):
        assert model.field == encoder.redacted_value


@mark.parametrize(
    ("inner_encoder", "value"),
    [
        (encoders.StringEncoder(), "foo"),
        (encoders.TextEncoder(), "foo"),
        (encoders.IntEncoder(), 5000),
        (encoders.DecimalEncoder(), Decimal("1.5")),
        (encoders.ArrayEncoder(encoders.StringEncoder()), ["foo", "bar"]),
        (encoders.ArrayEncoder(encoders.IntEncoder()), [1, 2]),
        (encoders.JSONEncoder(), {"foo": "bar", "something_else": []}),
        (encoders.DatetimeEncoder(), datetime.now()),
        (encoders.EnumEncoder(AnEnum), AnEnum.FOO),
    ],
)
def test_redacted_nullable_value(
    graph: ObjectGraph,
    session: Session,
    encryptors: dict[str, SingleTenantEncryptor],
    inner_encoder: encoders.Encoder,
    value: Any,
) -> None:
    """Nullable encoders should take the inner redacted value"""

    class TestModel(Model):
        __tablename__ = "test_employee_redacted_nullable"
        __table_args__ = {"extend_existing": True}

        if TYPE_CHECKING:
            __table__: ClassVar[Table]

        id: Mapped[UUID] = mapped_column(default=uuid4, primary_key=True)
        field = encryption("field", AwsKmsEncryptor(), encoders.Nullable(inner_encoder))
        field_encrypted = field.encrypted()
        field_unencrypted = field.unencrypted()

    try:
        TestModel.__table__.drop(graph.postgres)
    except ProgrammingError:
        ...
    TestModel.__table__.create(graph.postgres)

    # Encrypt data with client1's keys only
    with AwsKmsEncryptor.set_encryptor_context("test", encryptors[str(client_ids[0])]):
        session.add(model := TestModel(field=value))

    # Attempt to decrypt data with the wrong set of keys to simulate disabled client key
    with AwsKmsEncryptor.set_encryptor_context("test", encryptors[str(client_ids[1])]):
        assert model.field == inner_encoder.redacted_value


@mark.parametrize(
    ("inner_encoder", "value"),
    [
        (encoders.StringEncoder(), "foo"),
        (encoders.TextEncoder(), "foo"),
        (encoders.IntEncoder(), 5000),
        (encoders.DecimalEncoder(), Decimal("1.5")),
        (encoders.JSONEncoder(), {"foo": "bar", "something_else": []}),
        (encoders.DatetimeEncoder(), datetime.now()),
        (encoders.EnumEncoder(AnEnum), AnEnum.FOO),
        (encoders.Nullable(encoders.StringEncoder()), None),
        (encoders.Nullable(encoders.StringEncoder()), "foo"),
        (encoders.Nullable(encoders.EnumEncoder(AnEnum)), None),
        (encoders.Nullable(encoders.EnumEncoder(AnEnum)), AnEnum.FOO),
    ],
)
def test_redacted_array_value(
    graph: ObjectGraph,
    session: Session,
    encryptors: dict[str, SingleTenantEncryptor],
    inner_encoder: encoders.Encoder,
    value: Any,
) -> None:
    """Nullable encoders should take the inner redacted value"""

    class TestModel(Model):
        __tablename__ = "test_employee_redacted_array"
        __table_args__ = {"extend_existing": True}

        if TYPE_CHECKING:
            __table__: ClassVar[Table]

        id: Mapped[UUID] = mapped_column(default=uuid4, primary_key=True)
        field = encryption(
            "field", AwsKmsEncryptor(), encoders.ArrayEncoder(inner_encoder)
        )
        field_encrypted = field.encrypted()
        field_unencrypted = field.unencrypted()

    try:
        TestModel.__table__.drop(graph.postgres)
    except ProgrammingError:
        ...
    TestModel.__table__.create(graph.postgres)

    # Encrypt data with client1's keys only
    with AwsKmsEncryptor.set_encryptor_context("test", encryptors[str(client_ids[0])]):
        session.add(model := TestModel(field=[value]))

    # Attempt to decrypt data with the wrong set of keys to simulate disabled client key
    with AwsKmsEncryptor.set_encryptor_context("test", encryptors[str(client_ids[1])]):
        assert model.field == [inner_encoder.redacted_value]
