"""
Identity model tests.

"""
from hamcrest import (
    assert_that,
    calling,
    raises,
)
from microcosm.api import create_object_graph
from microcosm_postgres.errors import ModelIntegrityError
from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.tests.fixtures import Company, Employee, EmployeeIdentity


class TestOperations:

    def setup(self):
        self.graph = create_object_graph(name="example", testing=True, import_name="microcosm_postgres")

        self.context = SessionContext(self.graph)
        self.context.recreate_all()
        self.context.open()

        with transaction():
            self.company = Company(
                name="name"
            ).create()

    def teardown(self):
        self.context.close()

    def test_1(self):
        print("Hi")
        # with transaction():
        #     employee_identity = EmployeeIdentity(
        #         created_by=self.graph.employee_store.new_object_id()
        #     ).create()

        #     Employee(
        #         first="first",
        #         last="last",
        #         company_id=self.company.id,
        #         employee_identity_id=employee_identity.id,
        #     ).create()

        # self.context = SessionContext(self.graph)
        # assert_that(calling(self.context.recreate_all()), raises(ModelIntegrityError))
