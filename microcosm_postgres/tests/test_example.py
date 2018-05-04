"""
Persistence tests using examples.

"""
from hamcrest import (
    assert_that,
    calling,
    contains,
    contains_inanyorder,
    empty,
    equal_to,
    is_,
    raises,
)

from microcosm.api import create_object_graph
from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.errors import (
    DuplicateModelError,
    ModelIntegrityError,
    ModelNotFoundError,
    ReferencedModelError,
)
from microcosm_postgres.tests.example import Company, CompanyType, Employee


class TestCompany:

    def setup(self):
        self.graph = create_object_graph(name="example", testing=True, import_name="microcosm_postgres")
        self.company_store = self.graph.company_store
        self.employee_store = self.graph.employee_store

        self.context = SessionContext(self.graph)
        self.context.recreate_all()
        self.context.open()

    def teardown(self):
        self.context.close()

    def test_create_retrieve_company(self):
        """
        Should be able to retrieve a company after creating it.

        """
        with transaction():
            company = Company(
                name="name",
                type=CompanyType.private,
            ).create()

        retrieved_company = Company.retrieve(company.id)
        assert_that(retrieved_company.name, is_(equal_to("name")))
        assert_that(retrieved_company.type, is_(equal_to(CompanyType.private)))

    def test_search_company(self):
        """
        Should be able to search for companies.

        """
        with transaction():
            company = Company(
                name="name",
                type=CompanyType.private,
            ).create()

        assert_that(Company.search(), contains(company))
        assert_that(Company.search(name="whatever"), is_(empty()))
        assert_that(Company.search(name=company.name), contains(company))
        # NB: filtering is skipped if None
        assert_that(Company.search(name=None), contains(company))

    def test_create_duplicate_company(self):
        """
        Should not be able to retrieve a company with a duplicate name.

        """
        with transaction():
            Company(name="name").create()

        company = Company(name="name")
        assert_that(calling(company.create), raises(DuplicateModelError))

    def test_create_delete_company(self):
        """
        Should not be able to retrieve a company after deleting it.

        """
        with transaction():
            company = Company(name="name").create()

        with transaction():
            company.delete()

        assert_that(
            calling(Company.retrieve).with_args(company.id),
            raises(ModelNotFoundError, pattern="Company not found"),
        )

    def test_create_search_count_company(self):
        """
        Should be able to search and count companies after creation.

        """
        with transaction():
            company1 = Company(name="name1").create()
            company2 = Company(name="name2").create()

        assert_that(Company.count(), is_(equal_to(2)))
        assert_that([company.id for company in Company.search()], contains_inanyorder(company1.id, company2.id))

    def test_create_update_company(self):
        """
        Should be able to update a company after creating it.

        """
        with transaction():
            company = Company(name="name").create()

        with transaction():
            updated_company = Company(
                id=company.id,
                name="new_name",
            ).update()
            assert_that(updated_company.name, is_(equal_to("new_name")))

        with transaction():
            retrieved_company = Company.retrieve(company.id)
            assert_that(retrieved_company.name, is_(equal_to("new_name")))

    def test_create_update_with_diff_company(self):
        """
        Should be able to update a company after creating it and get a diff.

        """
        with transaction():
            company = Company(name="name").create()

        with transaction():
            _, diff = Company(
                id=company.id,
                name="new_name",
            ).update_with_diff()
            assert_that(list(diff.keys()), contains_inanyorder("name", "updated_at"))
            assert_that(diff["name"].before, is_(equal_to("name")))
            assert_that(diff["name"].after, is_(equal_to("new_name")))

        with transaction():
            retrieved_company = Company.retrieve(company.id)
            assert_that(retrieved_company.name, is_(equal_to("new_name")))

    def test_create_update_duplicate_company(self):
        """
        Should be not able to update a company to a duplicate name.

        """
        with transaction():
            Company(name="name1").create()
            company = Company(name="name2").create()

        company.name = "name1"
        assert_that(calling(company.update), raises(DuplicateModelError))

    def test_delete_company_with_employees(self):
        """
        Should be not able to delete a company with employees.

        """
        with transaction():
            Company(name="name1").create()
            company = Company(name="name2").create()
            Employee(
                first="first",
                last="last",
                company_id=company.id,
            ).create()

        assert_that(calling(company.delete), raises(ReferencedModelError))


class TestEmployee:

    def setup(self):
        self.graph = create_object_graph(name="example", testing=True, import_name="microcosm_postgres")
        self.company_store = self.graph.company_store
        self.employee_store = self.graph.employee_store

        self.context = SessionContext(self.graph)
        self.context.recreate_all()
        self.context.open()
        with transaction():
            self.company = Company(name="name").create()

    def teardown(self):
        self.context.close()

    def test_create_employee(self):
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

    def test_create_employee_without_company(self):
        """
        Should not be able to create an employee without a company.

        """
        employee = Employee(
            first="first",
            last="last",
        )

        assert_that(calling(employee.create), raises(ModelIntegrityError))

    def test_update_employee_that_exists(self):
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

    def test_update_with_diff_employee_that_exists(self):
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

    def test_update_employee_that_does_not_exit(self):
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

    def test_replace_employee_that_exists(self):
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

    def test_replace_employee_that_does_not_exist(self):
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

    def test_search_for_employees_by_company(self):
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
            company2 = Company(name="other").create()
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

    def test_search_filter_employees_by_company(self):
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
            company2 = Company(name="other").create()
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

    def test_create_search_first(self):
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
