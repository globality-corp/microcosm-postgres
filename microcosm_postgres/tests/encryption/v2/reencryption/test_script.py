from typing import Iterator, Any
from uuid import uuid4

from microcosm.decorators import binding
from microcosm.loaders import load_each, load_from_dict, load_from_environ
from microcosm.object_graph import ObjectGraph, create_object_graph
from sqlalchemy import UUID
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session, mapped_column, sessionmaker as SessionMaker
from pytest import fixture

from microcosm_postgres.context import transaction, SessionContext
from microcosm_postgres.encryption.constants import ENCRYPTION_V2_DEFAULT_KEY
from microcosm_postgres.encryption.encryptor import MultiTenantEncryptor, SingleTenantEncryptor
from microcosm_postgres.encryption.v2.column import encryption
from microcosm_postgres.encryption.v2.encoders import StringEncoder
from microcosm_postgres.encryption.v2.encryptors import AwsKmsEncryptor
from microcosm_postgres.encryption.v2.reencryption.utils import verify_client_has_some_encryption_config, \
    verify_planning_to_handle_all_tables, ModelWithEncryptionSearch, reencrypt_instance, ModelWithEncryption
from microcosm_postgres.models import Model
from microcosm_postgres.store import Store


class Person(Model):
    __tablename__ = "test_encryption_person"

    id = mapped_column(UUID, primary_key=True, default=uuid4)

    name = encryption("name", AwsKmsEncryptor(), StringEncoder())
    name_encrypted = name.encrypted()
    name_unencrypted = name.unencrypted(index=True)

    client_id = mapped_column(UUID, nullable=False, index=True)


@binding("encryption_person_store")
class EncryptionPersonStore(Store):

    def __init__(self, graph):
        super().__init__(graph, Person, auto_filter_fields=[Person.client_id])


client_id_1 = uuid4()
client_id_2 = uuid4()
client_id_no_encryption = uuid4()


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
                str(client_id_1),str(client_id_2),
            ],
            key_ids=[
                "key_id_1", "key_id_2;key_id_1",
            ],
            partitions=[
                "aws","aws",
            ],
            account_ids=[
                "12345","12345",
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
        import_name="microcosm_postgres",
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




MODELS_TO_REENCRYPT = [
    ModelWithEncryptionSearch(model=Person),
]


def find_instances(model: Any, client_id: UUID, search_kwargs: dict) -> Iterator[Model]:
    """
    This function is meant to find all the instances of a given model that belong to a given client.

    """
    return model.store.search(client_id=client_id, **search_kwargs)


def reencrypt_model(session: Session, model_with_encryption: ModelWithEncryptionSearch, client_id: UUID) -> None:
    """
    This function is meant to reencrypt all the relevant instances that are part of a
     given model.

     N.B this function hasn't been included in the library as the searches for the relevant
     instances are very specific to the application.

    """
    search_kwargs = {}
    if model_with_encryption.search_kwargs:
        search_kwargs = model_with_encryption.search_kwargs

    instances = find_instances(
        model=model_with_encryption.model, client_id=client_id, search_kwargs=search_kwargs
    )

    for instance in instances:
        reencrypt_instance(
            session=session,
            instance=instance,
            encryption_columns=model_with_encryption.encryption_columns(),
        )


def reencrypt_script(graph: ObjectGraph, client_id: UUID, single_tenant_encryptor: SingleTenantEncryptor) -> None:
    verify_client_has_some_encryption_config(graph, client_id)
    verify_planning_to_handle_all_tables(MODELS_TO_REENCRYPT)

    with (
        SessionContext(graph),
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor),
    ):
        for model_with_encryption in MODELS_TO_REENCRYPT:
            reencrypt_model(
                session=SessionContext.session,
                model_with_encryption=model_with_encryption,
                # Not all applications will have a client_id column
                # you can update this to be whatever you want
                client_id=client_id,
            )


def test_reencryption_script(
    graph: ObjectGraph,
    session: Session,
    single_tenant_encryptor_client_1: SingleTenantEncryptor,
    single_tenant_encryptor_client_2: SingleTenantEncryptor,
):
    """
    This is a test case which is meant to give an example of a reencryption script

    """
    # Initialise the person store
    _ = graph.encryption_person_store

    # First we encrypt some data with the first key
    # Store a map of the encrypted data
    encrypted_data = dict()
    with (
        graph.sessionmaker() as session,
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor_client_1),
    ):
        for i in range(10):
            session.add(person := Person(
                id=uuid4(),
                name=f"foo{i}",
                client_id=client_id_1,
            ))
            encrypted_data[person.id] = person.name_encrypted

        session.commit()

    # Then create some unencrypted data
    unencrypted_data = dict()
    with graph.sessionmaker() as session:
        for i in range(10):
            session.add(person := Person(
                id=uuid4(),
                name=f"foo{i}-unencrypted",
                client_id=client_id_no_encryption,
            ))
            unencrypted_data[person.id] = person.name_unencrypted

        session.commit()

    reencrypt_script(graph, client_id_1, single_tenant_encryptor_client_2)

    # Verify that we have correctly touched the encrypted data
    # We can assert that the encrypted data is not the same between the first / second encryption
    # processes
    with (
        graph.sessionmaker() as session,
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor_client_2),
    ):
        # Retrieve the encrypted person data
        for person in session.query(Person).filter(Person.client_id == client_id_1).all():
            assert person.name_encrypted != encrypted_data[person.id]

    # Retrieve the unencrypted person data
    with graph.sessionmaker() as session:
        for person in session.query(Person).filter(Person.client_id == client_id_no_encryption).all():
            assert person.name_unencrypted == unencrypted_data[person.id]
