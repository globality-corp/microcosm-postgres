"""
Persistence tests for employee "secret" data.

"""
from hamcrest import assert_that, equal_to, is_
from microcosm.api import create_object_graph, load_from_environ

from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.tests.fixtures import Company, Employee, EmployeeData


class TestEmployeeDataStore:

    def setup_method(self):
        self.graph = create_object_graph(
            name="example",
            testing=True,
            import_name="microcosm_postgres",
            loader=load_from_environ,
        )
        self.graph.use("sessionmaker")
        self.company_store = self.graph.company_store
        self.employee_store = self.graph.employee_store
        self.employee_data_store = self.graph.employee_data_store

        with SessionContext(self.graph) as context:
            context.recreate_all()

            with transaction():
                self.company = Company(
                    name="name",
                ).create()
                self.employee = Employee(
                    first="first",
                    last="last",
                    company_id=self.company.id,
                ).create()

    def teardown_method(self):
        self.graph.postgres.dispose()

    def test_create(self):
        """
        Should be able to retrieve an employee after creating it.

        """
        with SessionContext(self.graph), transaction():
            employee_data = self.employee_data_store.create(
                EmployeeData(
                    password="secret",
                    employee_id=self.employee.id,
                ),
            )

            retrieved_employee_data = self.employee_data_store.retrieve(employee_data.id)
            assert_that(retrieved_employee_data.password, is_(equal_to("secret")))
