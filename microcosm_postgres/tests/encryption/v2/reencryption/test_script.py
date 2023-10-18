from typing import Iterator
from uuid import uuid4

from microcosm.loaders import load_each, load_from_dict, load_from_environ
from microcosm.object_graph import ObjectGraph, create_object_graph
from sqlalchemy import UUID
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session, mapped_column, sessionmaker as SessionMaker
from pytest import fixture

from microcosm_postgres.encryption.constants import ENCRYPTION_V2_DEFAULT_KEY
from microcosm_postgres.encryption.encryptor import MultiTenantEncryptor, SingleTenantEncryptor
from microcosm_postgres.encryption.v2.column import encryption
from microcosm_postgres.encryption.v2.encoders import StringEncoder
from microcosm_postgres.encryption.v2.encryptors import AwsKmsEncryptor
from microcosm_postgres.models import Model


class Person(Model):
    __tablename__ = "test_encryption_person"

    id = mapped_column(UUID, primary_key=True, default=uuid4)

    name = encryption("name", AwsKmsEncryptor(), StringEncoder())
    name_encrypted = name.encrypted()
    name_unencrypted = name.unencrypted(index=True)

    client_id = mapped_column(UUID, nullable=False, index=True)


client_id = uuid4()


@fixture(scope="module")
def config() -> dict:
    return dict(
        multi_tenant_key_registry=dict(
            context_keys=[
                str(client_id),
            ],
            key_ids=[
                "key_id_1",
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
    try:
        Person.__table__.drop(graph.postgres)
    except ProgrammingError:
        pass
    Person.__table__.create(graph.postgres)


@fixture
def multi_tenant_encryptor(graph: ObjectGraph) -> MultiTenantEncryptor:
    return graph.multi_tenant_encryptor


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


def test_reencryption(
    session: Session,
    single_tenant_encryptor: SingleTenantEncryptor,
):
    """
    This is a test case which is meant to give an example of a reencryption script

    """

    # First we encrypt some data with the first key
    with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
        session.add(person := Person(
            name="foo",
            client_id=client_id,
        ))
        assert person.name_unencrypted is None
        assert person.name_encrypted is not None
        assert person.name == "foo"

    # Then we want to renecrypt the data with the second key


