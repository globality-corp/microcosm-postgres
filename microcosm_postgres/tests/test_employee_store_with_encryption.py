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
from sqlalchemy import (
    UUID,
    String,
    Table,
    UniqueConstraint,
    select,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, mapped_column, sessionmaker as SessionMaker

from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.encryption.encryptor import MultiTenantEncryptor, SingleTenantEncryptor
from microcosm_postgres.encryption.v2.column import encryption
from microcosm_postgres.encryption.v2.encoders import IntEncoder, StringEncoder
from microcosm_postgres.encryption.v2.encryptors import AwsKmsEncryptor
from microcosm_postgres.encryption.v2.utils import members_override
from microcosm_postgres.models import Model
from microcosm_postgres.store import Store


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

    age = encryption("age", AwsKmsEncryptor(), IntEncoder())
    age_encrypted = age.encrypted()
    age_unencrypted = age.unencrypted()
    age_beacon = age.beacon()

    # Non encrypted field
    department = mapped_column(String())

    # In the encrypted world, we need to make sure that the combination of name and department is unique
    __table_args__ = (
        UniqueConstraint(
            name_unencrypted,
            department,
            name="uq_employee_v2_name_department_unencrypted",
        ),
        UniqueConstraint(
            name_beacon,
            department,
            name="uq_employee_v2_name_department_encrypted",
        ),
    )

    def _members(self, for_insert: bool = False, using_encryption: bool = False):
        return members_override(
            self.__dict__,
            ["name", "salary", "age"],
            for_insert=for_insert,
            using_encryption=using_encryption,
        )


@binding("employee_store_with_encryption")
class EmployeeStore(Store):

    def __init__(self, graph):
        super().__init__(
            graph,
            Employee,
            auto_filter_fields=(
                Employee.name,
                Employee.age,
                Employee.department,
            )
        )

    def search_by_name(self, name):
        return self.search(Employee.name == name)

    def _order_by(self, query, **kwargs):
        return query.order_by(Employee.id.asc())

    def _filter(self, query, **kwargs):
        name = kwargs.get("name")
        if name is not None:
            query = query.filter(Employee.name == name)
        return super()._filter(query, **kwargs)

    def upsert(self, instance):
        using_encryption = self._check_if_using_encryption()
        constraint_name = "uq_employee_v2_name_department_encrypted" \
            if using_encryption \
            else "uq_employee_v2_name_department_unencrypted"

        with self.flushing():
            if instance.id is None:
                instance.id = self.new_object_id()
            self.session.execute(
                insert(self.model_class).values(instance._members(
                    using_encryption=using_encryption
                )).on_conflict_do_update(
                    constraint=constraint_name,
                    set_=instance._members(for_insert=True, using_encryption=using_encryption),
                ),
            )

        return self._retrieve(
            Employee.name == instance.name,
            Employee.department == instance.department,
        )

    def _check_if_using_encryption(self) -> bool:
        return AwsKmsEncryptor().encryptor_context is not None


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

        postgres=dict(
            echo=True,
        )
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


@fixture()
def clean_db(graph: ObjectGraph) -> None:
    """
    To be used when we want to explicitly clean the database

    """
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


def test_unique_constraint_name_department_encrypted(
    session: Session,
    graph: ObjectGraph,
    single_tenant_encryptor: SingleTenantEncryptor,
) -> None:
    """
    Checks that the unique constraint name_department is enforced

    """
    with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
        session.add(Employee(name="foo", department="bar"))
        session.commit()
        with pytest.raises(IntegrityError):
            session.add(Employee(name="foo", department="bar"))
            session.commit()


def test_unique_constraint_name_department_no_encryption(
    session: Session,
    graph: ObjectGraph,
    single_tenant_encryptor: SingleTenantEncryptor,
) -> None:
    """
    Checks that the unique constraint name_department is enforced

    """
    session.add(Employee(name="foo", department="bar"))
    session.commit()
    with pytest.raises(IntegrityError):
        session.add(Employee(name="foo", department="bar"))
        session.commit()


def test_beacon_value_generation(
    session: Session,
    single_tenant_encryptor: SingleTenantEncryptor,
    graph: ObjectGraph,
) -> None:
    """
    Test that checks that the beacon value is generated as expected

    """
    beacon = single_tenant_encryptor.beacon("test")
    assert beacon == "6fadab32a97ee7ee93eef7ff537cf4b977e7e736d8a2fea7023c3cca59573096"


def test_encrypt_and_search_using_beacon(
    session: Session,
    single_tenant_encryptor: SingleTenantEncryptor,
    graph: ObjectGraph,
    clean_db: None,
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
    with (
        SessionContext(graph),
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor)
    ):
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
    session.add(Employee(name="baz1"))
    session.add(Employee(name="baz2"))

    query = session.query(Employee).filter(Employee.name == "baz1")
    results = query.all()
    assert len(results) == 1


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


def test_search_by_beaconised_field_with_no_encryption(
    graph: ObjectGraph,
):
    """
    Test that checks that the normal searches remain unaffected by
    the addition of beacons i.e for clients that don't have encryption enabled

    """
    with SessionContext(graph) as context:
        context.recreate_all()

        session = context.session
        session.add(employee1 := Employee(name="foo", salary=1000))
        session.add(Employee(name="bar", salary=1000))
        session.commit()

    with SessionContext(graph):
        retrieved_employees = graph.employee_store_with_encryption.search_by_name("foo")
        assert len(retrieved_employees) == 1
        retrieved_employee = retrieved_employees[0]
        assert retrieved_employee.name == "foo"
        assert retrieved_employee.id == employee1.id


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


def test_search_with_auto_filter_field(
    session: Session,
    single_tenant_encryptor: SingleTenantEncryptor,
    graph: ObjectGraph,
) -> None:
    with AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor):
        session.add(employee := Employee(name="foo", age=100, department="bar"))
        session.add(employee2 := Employee(name="foo2", age=101, department="bar2"))
        session.commit()

    # Now we test that we can search for the employee
    # Note that this should use the defined beacon under the hood
    with (
        SessionContext(graph),
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor)
    ):
        retrieved_employees = graph.employee_store_with_encryption.search(age=100)
        assert len(retrieved_employees) == 1
        retrieved_employee = retrieved_employees[0]
        assert retrieved_employee.id == employee.id
        assert retrieved_employee.name == "foo"
        assert retrieved_employee.age == 100
        assert retrieved_employee.department == "bar"

        # Search with department - non encrypted field
        retrieved_employees = graph.employee_store_with_encryption.search(department="bar2")
        assert len(retrieved_employees) == 1
        retrieved_employee2 = retrieved_employees[0]
        assert retrieved_employee2.id == employee2.id
        assert retrieved_employee2.name == "foo2"
        assert retrieved_employee2.age == 101
        assert retrieved_employee2.department == "bar2"


def test_insert_employee_no_encryption(graph: ObjectGraph):
    with SessionContext(graph) as context, transaction():
        context.recreate_all()

        employee = Employee(
            name="Alice",
            salary=1000,
            age=30,
            department="IT"
        )

        session = context.session
        insert_stmt = insert(Employee).values(employee._members(for_insert=True, using_encryption=False))

        # Insert the data into the db
        session.execute(insert_stmt)

    with SessionContext(graph):
        # Check that the data is there
        employees = graph.employee_store_with_encryption.search(name="Alice")
        assert len(employees) == 1
        assert employees[0].name == "Alice"
        assert employees[0].name_beacon is None


def test_insert_employee_with_encryption(graph: ObjectGraph, single_tenant_encryptor: SingleTenantEncryptor):
    with (
        SessionContext(graph) as context,
        transaction(),
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor)
    ):
        context.recreate_all()

        employee = Employee(
            name="Alice",
            salary=1000,
            age=30,
            department="IT"
        )

        session = context.session
        insert_stmt = insert(Employee).values(employee._members(using_encryption=True))

        # Insert the data into the db
        session.execute(insert_stmt)

    with (
        SessionContext(graph),
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor)
    ):
        # Check that the data is in the database
        employees = graph.employee_store_with_encryption.search(name="Alice")
        assert len(employees) == 1
        assert employees[0].name == "Alice"
        assert employees[0].name_beacon == "144586366ce538da6cf694c9ba0e50a4bdb45446b9de2e1ffe2ae70e16508516"


def test_upsert_new_employee(
    graph: ObjectGraph,
) -> None:
    new_employee = Employee(
        name="Alice",
        salary=1000,
        age=30,
        department="IT"
    )

    with SessionContext(graph) as context, transaction():
        context.recreate_all()
        result = graph.employee_store_with_encryption.upsert(new_employee)

    assert result is not None
    assert result.name == new_employee.name
    assert result.salary == new_employee.salary
    assert result.age == new_employee.age
    assert result.department == new_employee.department


def test_upsert_existing_employee_2(
    session: Session,
    graph: ObjectGraph,
) -> None:
    existing_employee = Employee(
        name="Bob",
        department="Finance",
        age=40,
        salary=1200,
    )
    session.add(existing_employee)
    session.commit()

    with SessionContext(graph), transaction():
        updated_employee = Employee(
            id=existing_employee.id,
            name="Bob",
            department="Finance",
            age=40,
            salary=1300,
        )
        result = graph.employee_store_with_encryption.upsert(updated_employee)

    assert result is not None
    assert result.name == updated_employee.name
    assert result.salary == 1300  # Updated salary
    assert result.age == updated_employee.age
    assert result.department == updated_employee.department


def test_upsert_new_employee_with_encryption(
    graph: ObjectGraph,
    single_tenant_encryptor: SingleTenantEncryptor,
) -> None:
    with (
        SessionContext(graph) as context,
        transaction(),
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor)
    ):
        context.recreate_all()

        new_employee = Employee(
            name="Alice",
            salary=1000,
            age=30,
            department="IT"
        )

        graph.employee_store_with_encryption.upsert(new_employee)

    with (
        SessionContext(graph),
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor)
    ):
        # Check that the data is in the database
        employees = graph.employee_store_with_encryption.search(name="Alice")
        assert len(employees) == 1
        assert employees[0].name == "Alice"
        assert employees[0].name_beacon == "144586366ce538da6cf694c9ba0e50a4bdb45446b9de2e1ffe2ae70e16508516"
        assert employees[0].salary == 1000
        assert employees[0].age == 30


def test_upsert_existing_employee_with_encryption(
        graph: ObjectGraph,
        single_tenant_encryptor: SingleTenantEncryptor,
) -> None:
    with (
        SessionContext(graph) as context,
        transaction(),
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor)
    ):
        context.recreate_all()
        session = context.session

        existing_employee = Employee(
            name="Bob",
            department="Finance",
            age=40,
            salary=1200,
        )
        session.add(existing_employee)
        session.commit()

        updated_employee = Employee(
            name="Bob",
            department="Finance",
            age=40,
            salary=1300,
        )
        graph.employee_store_with_encryption.upsert(updated_employee)

    # Separate transaction
    with (
        SessionContext(graph),
        AwsKmsEncryptor.set_encryptor_context("test", single_tenant_encryptor)
    ):
        # Check that the data is in the database
        employees = graph.employee_store_with_encryption.search(name="Bob")
        assert len(employees) == 1
        assert employees[0].name == "Bob"
        assert employees[0].name_beacon == "b7ba82ea80985bd15f7e9909c6ff831c6c019d916bc0aff43646584c7901f7a5"
        assert employees[0].salary == 1300
        assert employees[0].age == 40
