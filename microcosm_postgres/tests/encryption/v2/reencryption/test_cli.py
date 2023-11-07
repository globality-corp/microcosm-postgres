import io
import sys
from contextlib import contextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, ClassVar
from uuid import uuid4

from microcosm.api import (
    create_object_graph,
    load_each,
    load_from_dict,
    load_from_environ,
)
from microcosm.object_graph import ObjectGraph
from pytest import fixture
from sqlalchemy import Column, Table
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session, declarative_base, sessionmaker as SessionMaker
from sqlalchemy_utils import UUIDType

from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.encryption.encryptor import MultiTenantEncryptor, SingleTenantEncryptor
from microcosm_postgres.encryption.v2.beacons import BeaconHashAlgorithm
from microcosm_postgres.encryption.v2.column import encryption
from microcosm_postgres.encryption.v2.encoders import StringEncoder
from microcosm_postgres.encryption.v2.encryptors import AwsKmsEncryptor
from microcosm_postgres.encryption.v2.reencryption.cli import ReencryptionCli


NewModel: Any = declarative_base()


class Employee(NewModel):
    __tablename__ = "test_encryption_employee_v4"
    if TYPE_CHECKING:
        __table__: ClassVar[Table]

    id = Column(UUIDType(), primary_key=True, default=uuid4)

    # Name requires beacon value for search
    name = encryption("name", AwsKmsEncryptor(), StringEncoder(), beacon_algorithm=BeaconHashAlgorithm.HMAC_SHA_256)
    name_encrypted = name.encrypted()
    name_unencrypted = name.unencrypted(index=True)
    name_beacon = name.beacon()

    client_id = Column(UUIDType(), nullable=False)


client_id = uuid4()


@contextmanager
def captured_output():
    original_stdout = sys.stdout
    sys.stdout = buffer = io.StringIO()
    try:
        yield buffer
    finally:
        sys.stdout = original_stdout


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


@fixture
def reencryption_cli(graph: ObjectGraph) -> ReencryptionCli:
    return ReencryptionCli(
        instance_iterators=[find_employee_instances_iter],  # type: ignore
        base_models_mapping={NewModel: [Employee]},
        graph=graph,
    )


def test_reencrypt_cli(
    graph: ObjectGraph,
    single_tenant_encryptor: SingleTenantEncryptor,
    reencryption_cli: ReencryptionCli
) -> None:

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

    args_mock = SimpleNamespace(
        client_id=str(client_id),
        no_dry_run=True,
        testing=True,
    )
    with captured_output() as output:
        reencryption_cli.reencrypt(args_mock)

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

    # Assert on print statements
    # Reset stdout to its original state
    output.seek(0)
    lines = output.readlines()

    assert lines[0] == "Success!\n"
    assert "Time taken to run:" in lines[1]
    assert lines[2] == "Model: Employee\n"
    assert lines[3] == "- Total Instances Found: 10\n"
    assert lines[4] == "- Instances Found to be Unencrypted: 0\n"
    assert lines[5] == "- Instances Reencrypted: 10\n"


def test_reencrypt_cli_no_dry_run(
    graph: ObjectGraph,
    single_tenant_encryptor: SingleTenantEncryptor,
    reencryption_cli: ReencryptionCli
) -> None:
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

    args_mock = SimpleNamespace(
        client_id=str(client_id),
        no_dry_run=False,
        testing=True,
    )
    reencryption_cli.reencrypt(args_mock)

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


def test_audit_cli(
    graph: ObjectGraph,
    single_tenant_encryptor: SingleTenantEncryptor,
    reencryption_cli: ReencryptionCli
) -> None:

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

    with captured_output() as output:
        reencryption_cli.audit(SimpleNamespace())

    output.seek(0)
    lines = output.readlines()
    assert lines[0] == "Found 1 table(s) with encryption usage:\n"
    assert lines[1] == "Model name: Employee, Cols used: name\n"


@fixture
def reencryption_cli_with_mocked_iterator(
        graph: ObjectGraph,
) -> ReencryptionCli:
    def mock_iterator(*args, **kwargs):
        return []

    return ReencryptionCli(
        instance_iterators=[mock_iterator],
        base_models_mapping={NewModel: [Employee]},
        graph=graph,
    )


def test_reencrypt_command_setup(
    graph: ObjectGraph,
    single_tenant_encryptor,
    reencryption_cli_with_mocked_iterator: ReencryptionCli
):
    sys.argv = ["prog_name", "reencrypt", "--client-id", str(client_id)]

    with captured_output() as output:
        reencryption_cli_with_mocked_iterator()

    output.seek(0)
    lines = output.readlines()
    assert lines[0] == "Success!\n"


def test_reencrypt_command_setup_dry_run(
    graph: ObjectGraph,
    single_tenant_encryptor,
    reencryption_cli_with_mocked_iterator: ReencryptionCli
):
    sys.argv = ["prog_name", "reencrypt", "--client-id", str(client_id), "--no-dry-run"]

    with captured_output() as output:
        reencryption_cli_with_mocked_iterator()

    output.seek(0)
    lines = output.readlines()
    assert lines[0] == "Success!\n"


def test_audit_command_setup(
    graph: ObjectGraph,
    single_tenant_encryptor,
    reencryption_cli_with_mocked_iterator: ReencryptionCli,
):
    sys.argv = ["prog_name", "audit"]

    # Call the function that prints
    with captured_output() as output:
        reencryption_cli_with_mocked_iterator()

    # Reset stdout to its original state
    output.seek(0)
    lines = output.readlines()

    # Assertions
    assert lines[0] == "Found 1 table(s) with encryption usage:\n"
    assert lines[1] == "Model name: Employee, Cols used: name\n"
