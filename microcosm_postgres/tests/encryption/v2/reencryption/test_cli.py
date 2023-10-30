import logging
import sys
from types import SimpleNamespace
from typing import TYPE_CHECKING, ClassVar, Iterator
from uuid import uuid4

from _pytest.logging import LogCaptureFixture
from microcosm.api import (
    create_object_graph,
    load_each,
    load_from_dict,
    load_from_environ,
)
from microcosm.object_graph import ObjectGraph
from pytest import fixture
from sqlalchemy import UUID, Table
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import mapped_column, sessionmaker as SessionMaker, Session

from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.encryption.encryptor import MultiTenantEncryptor, SingleTenantEncryptor
from microcosm_postgres.encryption.v2.column import encryption
from microcosm_postgres.encryption.v2.encoders import StringEncoder
from microcosm_postgres.encryption.v2.encryptors import AwsKmsEncryptor
from microcosm_postgres.encryption.v2.reencryption.cli import ReencryptionCli
from microcosm_postgres.models import Model


class Employee(Model):
    __tablename__ = "test_encryption_employee_v4"
    if TYPE_CHECKING:
        __table__: ClassVar[Table]

    id = mapped_column(UUID, primary_key=True, default=uuid4)

    # Name requires beacon value for search
    name = encryption("name", AwsKmsEncryptor(), StringEncoder())
    name_encrypted = name.encrypted()
    name_unencrypted = name.unencrypted(index=True)
    name_beacon = name.beacon()

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
                "key_id",
            ],
            partitions=[
                "aws",
            ],
            account_ids=[
                "12345",
            ],
            beacon_keys=[
                "beacon_key",
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
        Employee.__table__.drop(graph.postgres)
    except ProgrammingError:
        pass
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


def find_employee_instances_iter(session: Session, client_id: str, **kwargs) -> list[Employee]:
    return session.query(Employee).filter(Employee.client_id == client_id).all()


def test_reencrypt_cli(graph: ObjectGraph, single_tenant_encryptor: SingleTenantEncryptor, caplog: LogCaptureFixture):
    caplog.set_level(logging.INFO)

    with (
        SessionContext(graph) as context,
        transaction(),
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor)
    ):
        context.recreate_all()
        session = context.session

        # Create 10 models for the given client_id
        for i in range(10):
            session.add(Employee(name=f"foo-{i}", client_id=client_id))

        original_names_encrypted = [employee.name_encrypted for employee in session.query(Employee).all()]

    # Non encrypted session
    with (
        SessionContext(graph) as context,
        transaction(),
    ):
        session = context.session

        # Create 10 models for a random client_id
        random_client_id = uuid4()
        for _ in range(10):
            session.add(Employee(name="foo", client_id=random_client_id))

    cli = ReencryptionCli(
        instance_iterators=[find_employee_instances_iter],  # type: ignore
        base_models_mapping={Model: [Employee]},
        graph=graph,
    )
    args_mock = SimpleNamespace(
        client_id=str(client_id),
        no_dry_run=True,
        testing=True,
    )
    cli.reencrypt(args_mock)

    with (
        SessionContext(graph) as context,
        transaction(),
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor)
    ):
        session = context.session
        new_names_encrypted = [employee.name_encrypted for employee in session.query(Employee).all()]

        # Assertions
        # Check that we reencrypted all the names for the given client_id
        for orig, new in zip(original_names_encrypted, new_names_encrypted):
            assert orig != new

        # For random client id: Assert name doesn't change and name_encrypted is None
        random_employees = session.query(Employee).filter_by(client_id=random_client_id).all()
        for employee in random_employees:
            assert employee.name == "foo"
            assert employee.name_encrypted is None

    # Asserting on the log output
    assert caplog.messages[-3] == "Success!"
    assert "Time taken to run: {elapsed_time}" in caplog.messages[-2]
    assert caplog.messages[-1] == \
           "ReencryptionStatistic(model_name='Employee', total_instances_found=10, " \
           "instances_found_to_be_unencrypted=0, instances_reencrypted=10)"


def test_reencrypt_cli_dry_run(graph: ObjectGraph, single_tenant_encryptor: SingleTenantEncryptor):
    with (
        SessionContext(graph) as context,
        transaction(),
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor)
    ):
        context.recreate_all()
        session = context.session

        # Create 10 models for the given client_id
        for i in range(10):
            session.add(Employee(name=f"foo-{i}", client_id=client_id))

        original_names_encrypted = [employee.name_encrypted for employee in session.query(Employee).all()]

    # Non encrypted session
    with (
        SessionContext(graph) as context,
        transaction(),
    ):
        session = context.session

        # Create 10 models for a random client_id
        random_client_id = uuid4()
        for _ in range(10):
            session.add(Employee(name="foo", client_id=random_client_id))

    cli = ReencryptionCli(
        instance_iterators=[find_employee_instances_iter],  # type: ignore
        base_models_mapping={Model: [Employee]},
        graph=graph,
    )
    args_mock = SimpleNamespace(
        client_id=str(client_id),
        no_dry_run=False,
        testing=True,
    )
    cli.reencrypt(args_mock)

    with (
        SessionContext(graph) as context,
        transaction(),
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor)
    ):
        session = context.session
        new_names_encrypted = [employee.name_encrypted for employee in session.query(Employee).all()]

        # Assertions
        # Check that we reencrypted all the names for the given client_id
        for orig, new in zip(original_names_encrypted, new_names_encrypted):
            assert orig == new

        # For random client id: Assert name doesn't change and name_encrypted is None
        random_employees = session.query(Employee).filter_by(client_id=random_client_id).all()
        for employee in random_employees:
            assert employee.name == "foo"
            assert employee.name_encrypted is None


def test_reencrypt_cli_validation_error():
    pass


def test_audit_cli(graph: ObjectGraph, single_tenant_encryptor: SingleTenantEncryptor, caplog: LogCaptureFixture):
    caplog.set_level(logging.INFO)

    with (
        SessionContext(graph) as context,
        transaction(),
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor)
    ):
        context.recreate_all()
        session = context.session

        # Create 10 models for the given client_id
        for i in range(10):
            session.add(Employee(name=f"foo-{i}", client_id=client_id))

    # Non encrypted session
    with (
        SessionContext(graph) as context,
        transaction(),
    ):
        session = context.session

        # Create 10 models for a random client_id
        random_client_id = uuid4()
        for _ in range(10):
            session.add(Employee(name="foo", client_id=random_client_id))

    cli = ReencryptionCli(
        instance_iterators=[find_employee_instances_iter],  # type: ignore
        base_models_mapping={Model: [Employee]},
        graph=graph,
    )
    cli.audit(SimpleNamespace())
    assert caplog.messages[-2] == "Found {} table(s) with encryption usage:"
    assert caplog.messages[-1] == "Model name: {}, Cols used: {}"


def test_reencrypt_command_setup(graph: ObjectGraph, single_tenant_encryptor, caplog: LogCaptureFixture):
    caplog.set_level(logging.INFO)
    sys.argv = ["prog_name", "reencrypt", "--client-id", str(client_id)]

    def mock_iterator(*args, **kwargs):
        return []

    cli = ReencryptionCli(
        instance_iterators=[mock_iterator],
        base_models_mapping={Model: [Employee]},
        graph=graph,
    )
    cli()
    assert caplog.messages[-3] == "Success!"


def test_reencrypt_command_setup_dry_run(graph: ObjectGraph, single_tenant_encryptor, caplog: LogCaptureFixture):
    caplog.set_level(logging.INFO)
    sys.argv = ["prog_name", "reencrypt", "--client-id", str(client_id), "--no-dry-run"]

    def mock_iterator(*args, **kwargs):
        return []

    cli = ReencryptionCli(
        instance_iterators=[mock_iterator],
        base_models_mapping={Model: [Employee]},
        graph=graph,
    )
    cli()
    assert caplog.messages[-3] == "Success!"


def test_audit_command_setup(graph: ObjectGraph, single_tenant_encryptor, caplog: LogCaptureFixture):
    caplog.set_level(logging.INFO)
    sys.argv = ["prog_name", "audit"]

    def mock_iterator(*args, **kwargs):
        return []

    cli = ReencryptionCli(
        instance_iterators=[mock_iterator],
        base_models_mapping={Model: [Employee]},
        graph=graph,
    )
    cli()
    assert caplog.messages[-2] == "Found {} table(s) with encryption usage:"
    assert caplog.messages[-1] == "Model name: {}, Cols used: {}"
