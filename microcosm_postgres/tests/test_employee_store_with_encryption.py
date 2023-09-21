from __future__ import annotations

import re
from typing import TYPE_CHECKING, ClassVar, Iterator
from uuid import uuid4

import pytest
from microcosm.api import (
    create_object_graph,
    load_each,
    load_from_dict,
    load_from_environ,
)
from microcosm.decorators import binding
from microcosm.object_graph import ObjectGraph
from pytest import fixture
from sqlalchemy import UUID, Table, select
from sqlalchemy.orm import Session, mapped_column, sessionmaker as SessionMaker

from microcosm_postgres.context import SessionContext
from microcosm_postgres.encryption.encryptor import MultiTenantEncryptor, SingleTenantEncryptor
from microcosm_postgres.encryption.v2.column import encryption
from microcosm_postgres.encryption.v2.encoders import IntEncoder, StringEncoder
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

    # Name requires beacon value for search
    name = encryption("name", AwsKmsEncryptor(), StringEncoder())
    name_encrypted = name.encrypted()
    name_unencrypted = name.unencrypted(index=True)
    name_beacon = name.beacon()

    # Salary does not require beacon value
    salary = encryption("salary", AwsKmsEncryptor(), IntEncoder())
    salary_encrypted = salary.encrypted()
    salary_unencrypted = salary.unencrypted()


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


def test_beacon_value_generation():
    """
    Test that checks that the beacon value is generated as expected

    """
    pass


def test_encrypt_and_search_using_beacon(
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


def test_encrypt_and_search_using_beacon_with_no_beacon_key():
    # No beacon key is defined in the config
    config = dict(
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

    graph = create_object_graph(
        "example",
        testing=True,
        loader=load_each(
            load_from_dict(config),
            load_from_environ,
        ),
        import_name="microcosm_postgres",
    )

    with graph.sessionmaker() as session:
        single_tenant_encryptor = graph.multi_tenant_encryptor.encryptors[str(client_id)]
        with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
            with pytest.raises(AwsKmsEncryptor.BeaconKeyNotSet):
                session.add(employee := Employee(name="foo"))
                assert employee.name_unencrypted is None
                assert employee.name_encrypted is not None
                assert employee.name_beacon is None
                assert employee.name == "foo"
                session.commit()


def test_encryptor_not_bound_when_beacon_used_without_context(
    session: Session,
    single_tenant_encryptor: SingleTenantEncryptor,
    graph: ObjectGraph,
) -> None:
    with pytest.raises(AwsKmsEncryptor.EncryptorNotBound):
        session.add(Employee(name="foo"))

        query = session.query(Employee).filter(Employee.name == "foo")
        query.all()


def test_encrypt_no_beacon_used(
    session: Session,
    single_tenant_encryptor: SingleTenantEncryptor,
    graph: ObjectGraph,
) -> None:
    with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
        session.add(employee := Employee(name="foo", salary=100))
        assert employee.salary_unencrypted is None
        assert employee.salary_encrypted is not None
        with pytest.raises(AttributeError):
            assert employee.salary_beacon is None  # type: ignore[attr-defined]

        assert employee.name == "foo"
        assert employee.salary == 100


def test_order_by_with_beacon(
    single_tenant_encryptor: SingleTenantEncryptor,
    graph: ObjectGraph,
) -> None:
    """
    Test that when we order by with the beaconised field, then it
    doesn't error.

    Note that if you try to order with a beaconised field it will
    be essentially random.

    """
    with SessionContext(graph) as context:
        context.recreate_all()

        session = context.session
        with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
            session.add(Employee(name="foo", salary=1000))
            session.add(Employee(name="bar", salary=1000))
            session.commit()

    with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
        query = select(Employee).order_by(Employee.name.asc())
        # Use regex to match the compiled sql
        regex = re.compile(r"ORDER BY .*?name_beacon ASC")
        assert regex.search(str(query))

        results = session.execute(query).scalars().all()
        assert len(results) == 2
        for r in results:
            assert r.salary == 1000

        # Same for desc
        query = select(Employee).order_by(Employee.name.desc())
        regex = re.compile(r"ORDER BY .*?name_beacon DESC")
        assert regex.search(str(query))

        results = session.execute(query).scalars().all()
        assert len(results) == 2
        for r in results:
            assert r.salary == 1000


def test_searching_on_encrypted_field_with_no_beacon(
    session: Session,
    single_tenant_encryptor: SingleTenantEncryptor,
    graph: ObjectGraph,
):
    """
    This test checks that it doesn't break if you try to search for a field
    which is encrypted but with no beacon field defined.

    """

    with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
        session.add(Employee(name="foo", salary=1000))
        session.add(Employee(name="bar", salary=1000))
        session.commit()

    query = select(Employee).filter(Employee.salary == 1000).order_by(Employee.name.asc())
    results = session.execute(query).scalars().all()

    assert len(results) == 0


def test_search_with_array_of_beacons(
    single_tenant_encryptor: SingleTenantEncryptor,
    graph: ObjectGraph,
) -> None:
    """

    """
    with SessionContext(graph) as context:
        context.recreate_all()

        session = context.session
        with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
            session.add(Employee(name="foo", salary=1000))
            session.add(Employee(name="bar", salary=1000))
            session.commit()

            query = select(Employee).filter(Employee.name.in_(["foo", "bar"]))

            regex = re.compile(r"WHERE test_encryption_employee_v2.name_beacon IN .*?name_beacon_1")
            assert regex.search(str(query))

            results = session.execute(query).scalars().all()

        assert len(results) == 2
