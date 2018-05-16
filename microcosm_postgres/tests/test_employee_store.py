"""
Persistence tests for employee store.

"""
from hamcrest import (
    assert_that,
    calling,
    contains,
    contains_inanyorder,
    equal_to,
    is_,
    raises,
)

from microcosm.api import create_object_graph
from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.errors import (
    ModelIntegrityError,
    ModelNotFoundError,
)
from microcosm_postgres.tests.fixtures import Company, Employee


class TestEmployeeStore:

    def setup(self):
        self.graph = create_object_graph(name="example", testing=True, import_name="microcosm_postgres")
        self.company_store = self.graph.company_store
        self.employee_store = self.graph.employee_store

        self.context = SessionContext(self.graph)
        self.context.recreate_all()
        self.context.open()

        with transaction():
            self.company = Company(
                name="name"
            ).create()

    def teardown(self):
        self.context.close()
        self.graph.postgres.dispose()

    def test_create(self):
        """
        Should be able to retrieve an employee after creating it.

        """
        with transaction():
            employee = Employee(
                first="first",
                last="last",
                company_id=self.company.id,
            ).create()

        retrieved_employee = Employee.retrieve(employee.id)
        assert_that(retrieved_employee.first, is_(equal_to("first")))
        assert_that(retrieved_employee.last, is_(equal_to("last")))

    def test_create_requires_foreign_key(self):
        """
        Should not be able to create an employee without a company.

        """
        employee = Employee(
            first="first",
            last="last",
        )

        assert_that(calling(employee.create), raises(ModelIntegrityError))

    def test_update(self):
        """
        Should be able to update an employee after creating it.

        """
        with transaction():
            employee = Employee(
                first="first",
                last="last",
                company_id=self.company.id,
            ).create()

        with transaction():
            updated_employee = Employee(
                id=employee.id,
                first="Jane",
                last="Doe",
            ).update()
            assert_that(updated_employee.first, is_(equal_to("Jane")))
            assert_that(updated_employee.last, is_(equal_to("Doe")))
            assert_that(updated_employee.company_id, is_(equal_to(self.company.id)))

        with transaction():
            retrieved_employee = Employee.retrieve(employee.id)
            assert_that(retrieved_employee.first, is_(equal_to("Jane")))
            assert_that(retrieved_employee.last, is_(equal_to("Doe")))
            assert_that(Employee.count(), is_(equal_to(1)))

    def test_update_with_diff(self):
        """
        Should be able to update an employee after creating it and get a diff.

        """
        with transaction():
            employee = Employee(
                first="first",
                last="last",
                company_id=self.company.id,
            ).create()

        with transaction():
            _, diff = Employee(
                id=employee.id,
                last="Doe",
            ).update_with_diff()
            assert_that(list(diff.keys()), contains_inanyorder("last", "updated_at"))
            assert_that(diff["last"].before, is_(equal_to("last")))
            assert_that(diff["last"].after, is_(equal_to("Doe")))

        with transaction():
            retrieved_employee = Employee.retrieve(employee.id)
            assert_that(retrieved_employee.first, is_(equal_to("first")))
            assert_that(retrieved_employee.last, is_(equal_to("Doe")))
            assert_that(Employee.count(), is_(equal_to(1)))

    def test_update_not_found(self):
        """
        Should not be able to update an employee that does not exist.

        """
        with transaction():
            employee = Employee(
                first="first",
                last="last",
                company_id=self.company.id,
            )
            assert_that(calling(employee.update), raises(ModelNotFoundError))

    def test_replace(self):
        """
        Should be able to replace an employee after creating it.

        """
        with transaction():
            employee = Employee(
                first="first",
                last="last",
                company_id=self.company.id,
            ).create()

        with transaction():
            updated_employee = Employee(
                id=employee.id,
                first="Jane",
                last="Doe",
            ).replace()
            assert_that(updated_employee.first, is_(equal_to("Jane")))
            assert_that(updated_employee.last, is_(equal_to("Doe")))

        with transaction():
            retrieved_employee = Employee.retrieve(employee.id)
            assert_that(retrieved_employee.first, is_(equal_to("Jane")))
            assert_that(retrieved_employee.last, is_(equal_to("Doe")))
            assert_that(Employee.count(), is_(equal_to(1)))

    def test_replace_not_found(self):
        """
        Should be able to replace an employee that does not exist.

        """
        with transaction():
            employee = Employee(
                first="first",
                last="last",
                company_id=self.company.id,
            ).replace()

        with transaction():
            retrieved_employee = Employee.retrieve(employee.id)
            assert_that(retrieved_employee.first, is_(equal_to("first")))
            assert_that(retrieved_employee.last, is_(equal_to("last")))
            assert_that(Employee.count(), is_(equal_to(1)))

    def test_search_by_company(self):
        """
        Should be able to retrieve an employee after creating it.

        """
        with transaction():
            employee1 = Employee(
                first="first",
                last="last",
                company_id=self.company.id,
            ).create()
            employee2 = Employee(
                first="Jane",
                last="Doe",
                company_id=self.company.id,
            ).create()
            company2 = Company(
                name="other",
            ).create()
            employee3 = Employee(
                first="John",
                last="Doe",
                company_id=company2.id,
            ).create()

        assert_that(Employee.count(), is_(equal_to(3)))
        assert_that(
            [employee.last for employee in self.employee_store.search_by_company(self.company.id)],
            contains("Doe", "last"),
        )
        assert_that(
            [employee.id for employee in self.employee_store.search_by_company(self.company.id)],
            contains_inanyorder(employee1.id, employee2.id)
        )
        assert_that(
            [employee.id for employee in self.employee_store.search_by_company(company2.id)],
            contains_inanyorder(employee3.id)
        )

    def test_search_by_company_kwargs(self):
        """
        Should be able to filter searches using kwargs.

        """
        with transaction():
            employee1 = Employee(
                first="first",
                last="last",
                company_id=self.company.id,
            ).create()
            employee2 = Employee(
                first="Jane",
                last="Doe",
                company_id=self.company.id,
            ).create()
            company2 = Company(
                name="other",
            ).create()
            employee3 = Employee(
                first="John",
                last="Doe",
                company_id=company2.id,
            ).create()

        assert_that(Employee.count(), is_(equal_to(3)))
        assert_that(
            [employee.id for employee in self.employee_store.search(company_id=self.company.id, offset=0, limit=10)],
            contains_inanyorder(employee1.id, employee2.id)
        )
        assert_that(self.employee_store.count(company_id=self.company.id, offset=0, limit=10), is_(equal_to(2)))
        assert_that(
            [employee.id for employee in self.employee_store.search(company_id=company2.id, offset=0, limit=10)],
            contains_inanyorder(employee3.id)
        )
        assert_that(self.employee_store.count(company_id=company2.id, offset=0, limit=10), is_(equal_to(1)))

    def test_search_first(self):
        """
        Should be able to search for the first item with matching criteria after creation.

        """
        with transaction():
            Employee(
                first="first",
                last="last",
                company_id=self.company.id,
            ).create()
            Employee(
                first="Jane",
                last="Doe",
                company_id=self.company.id,
            ).create()

        retrieved_real_employee = self.employee_store.search_first(first="Jane")
        assert_that(retrieved_real_employee.last, is_(equal_to("Doe")))
        retrieved_fake_employee = self.employee_store.search_first(first="Tarzan")
        assert_that(retrieved_fake_employee, is_(equal_to(None)))
