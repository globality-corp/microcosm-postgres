from typing import Iterator
from uuid import uuid4

import pytest
from aws_encryption_sdk.exceptions import DecryptKeyError
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
from microcosm_postgres.encryption.v2.reencryption.utils import reencrypt_instance
from microcosm_postgres.models import Model


class Person(Model):
    __tablename__ = "test_encryption_person"

    id = mapped_column(UUID, primary_key=True, default=uuid4)

    name = encryption("name", AwsKmsEncryptor(), StringEncoder())
    name_encrypted = name.encrypted()
    name_unencrypted = name.unencrypted(index=True)

    client_id = mapped_column(UUID, nullable=False, index=True)


client_id_1 = uuid4()
client_id_2 = uuid4()
client_id_3 = uuid4()


@fixture(scope="module")
def config() -> dict:
    """
    This config is meant to represent the config in three different stages
    of the reencryption process i.e first you have a single key (key 1)
    then you have key1 and key2 configured and finally you removed key1.

    """
    return dict(
        multi_tenant_key_registry=dict(
            context_keys=[
                str(client_id_1),str(client_id_2),str(client_id_3),
            ],
            key_ids=[
                "key_id_1", "key_id_2;key_id_1", "key_id_2",
            ],
            partitions=[
                "aws","aws","aws",
            ],
            account_ids=[
                "12345","12345","12345",
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
def single_tenant_encryptor_client_1(
    multi_tenant_encryptor: MultiTenantEncryptor,
) -> SingleTenantEncryptor:
    return multi_tenant_encryptor.encryptors[str(client_id_1)]


@fixture
def single_tenant_encryptor_client_2(
    multi_tenant_encryptor: MultiTenantEncryptor,
) -> SingleTenantEncryptor:
    return multi_tenant_encryptor.encryptors[str(client_id_2)]


@fixture
def single_tenant_encryptor_client_3(
    multi_tenant_encryptor: MultiTenantEncryptor,
) -> SingleTenantEncryptor:
    return multi_tenant_encryptor.encryptors[str(client_id_3)]


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


def test_reencrypt_instance(
    graph: ObjectGraph,
    single_tenant_encryptor_client_1: SingleTenantEncryptor,
    single_tenant_encryptor_client_2: SingleTenantEncryptor,
    single_tenant_encryptor_client_3: SingleTenantEncryptor,
) -> None:
    """
    Test that checks we can reencrypt an instance with a new key

    We have configured three different clients. Each "client" is meant to
    represent a different stage of the reencryption process i.e first you have
    a single key (key 1) then you have key1 and key2 configured and finally
    you removed key1.

    """
    # First we encrypt some data with the first key
    with (
        graph.sessionmaker() as session,
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor_client_1),
    ):
        session.add(person := Person(
            name="foo",
            client_id=client_id_1,
        ))
        assert person.name == "foo"
        session.commit()

        # Run test - check that two encrypted values aren't the same
        # session.add(person2 := Person(
        #     name="foo",
        #     client_id=client_id_1,
        # ))
        # assert person.name == "foo"
        # assert person.name_encrypted != person2.name_encrypted

    # Check that we can't read the data with client_id_3
    with (
        graph.sessionmaker() as session,
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor_client_3),
    ):
        person = session.query(Person).first()
        with pytest.raises(DecryptKeyError):
            assert person.name == "foo"

    # Then we want to renecrypt the data with the second key
    with (
        graph.sessionmaker() as session,
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor_client_2)
    ):
        person = session.query(Person).first()
        assert person.name_unencrypted is None
        assert person.name_encrypted is not None
        assert person.name == "foo"

        # We reencrypt the instance
        reencrypt_instance(
            session=session,
            instance=person,
            encryption_columns=["name"],
        )
        session.commit()

    # N.B at this point we can still read data with the first key from client_id_1
    # i.e no exception is raised
    with (
        graph.sessionmaker() as session,
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor_client_1)
    ):
        person = session.query(Person).first()
        assert person.name == "foo"

    # Now check that we can read the data with client_id_3
    with (
        graph.sessionmaker() as session,
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor_client_3)
    ):
        person = session.query(Person).first()
        assert person.name == "foo"

        # Now reencrypt with client_id_3
        reencrypt_instance(
            session=session,
            instance=person,
            encryption_columns=["name"],
        )
        session.commit()

    # Now check that we can't read the data with client_id_1
    with (
        graph.sessionmaker() as session,
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor_client_1)
    ):
        person = session.query(Person).first()
        with pytest.raises(DecryptKeyError):
            assert person.name == "foo"
