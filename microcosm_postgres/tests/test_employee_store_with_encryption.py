from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Iterator
from uuid import uuid4

from microcosm.api import (
    create_object_graph,
    load_each,
    load_from_dict,
    load_from_environ,
)
from microcosm.decorators import binding
from microcosm.object_graph import ObjectGraph
from pytest import fixture
from sqlalchemy import UUID, Table
from sqlalchemy.orm import Session, mapped_column, sessionmaker as SessionMaker

from microcosm_postgres.context import SessionContext
from microcosm_postgres.encryption.encryptor import MultiTenantEncryptor, SingleTenantEncryptor
from microcosm_postgres.encryption.v2.column import encryption
from microcosm_postgres.encryption.v2.encoders import ArrayEncoder, Nullable, StringEncoder
from microcosm_postgres.encryption.v2.encryptors import AwsKmsEncryptor
from microcosm_postgres.models import Model
from microcosm_postgres.store import Store


@binding("employee_store_with_encryption")
class EmployeeStore(Store):

    def __init__(self, graph):
        super().__init__(graph, Employee)

    def search_by_name(self, name):
        return self.search(Employee.name == name)

    def _order_by(self, query, **kwargs):
        # What if the order by order is on the name_encrypted column?
        return query.order_by(Employee.id.asc())

    def _filter(self, query, **kwargs):
        name = kwargs.get("name")
        if name is not None:
            query = query.filter(Employee.name == name)
        return super(EmployeeStore, self)._filter(query, **kwargs)


class Employee(Model):
    __tablename__ = "test_encryption_employee_v2"
    if TYPE_CHECKING:
        __table__: ClassVar[Table]

    id = mapped_column(UUID, primary_key=True, default=uuid4)

    name = encryption("name", AwsKmsEncryptor(), StringEncoder())
    name_encrypted = name.encrypted()
    name_unencrypted = name.unencrypted(index=True)
    name_beacon = name.beacon()


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


def test_encrypt_and_search(
    session: Session,
    single_tenant_encryptor: SingleTenantEncryptor,
    graph: ObjectGraph,
) -> None:
    with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
        session.add(employee := Employee(name="foo"))
        assert employee.name_unencrypted is None
        assert employee.name_encrypted is not None
        assert employee.name_beacon is not None
        assert employee.name == "foo"
        session.commit()

    # Now we test that we can search for the employee
    # Note that this should use the defined beacon under the hood
    with SessionContext(graph):
        with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
            retrieved_employees = graph.employee_store_with_encryption.search_by_name("foo")
            assert len(retrieved_employees) == 1
            retrieved_employee = retrieved_employees[0]
            assert retrieved_employee.name == "foo"
            assert retrieved_employee.id == employee.id



