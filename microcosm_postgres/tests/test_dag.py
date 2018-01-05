"""
Test DAG.

"""
from hamcrest import assert_that, contains, has_entries, has_length
from microcosm.api import create_object_graph

from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.dag import DAG, Edge
from microcosm_postgres.tests.example import Company, CompanyType, Employee


class TestCloning:

    def setup(self):
        self.graph = create_object_graph(name="example", testing=True, import_name="microcosm_postgres")
        self.company_store = self.graph.company_store
        self.employee_store = self.graph.employee_store

        self.context = SessionContext(self.graph)
        self.context.recreate_all()
        self.context.open()

        with transaction():
            self.company = Company(
                name="name",
                type=CompanyType.private,
            ).create()
            self.employee = Employee(
                first="first",
                last="last",
                company_id=self.company.id,
            ).create()

    def teardown(self):
        self.context.close()

    def test_explain(self):
        dag = DAG.from_nodes(self.company, self.employee)
        assert_that(dag.nodes, has_entries({
            self.company.id: self.company,
            self.employee.id: self.employee,
        }))
        assert_that(dag.edges, contains(
            Edge(self.company.id, self.employee.id),
        ))
        assert_that(dag.ordered_nodes, contains(
            self.company,
            self.employee,
        ))

    def test_clone(self):
        substitutions = dict(name="newname")
        dag = DAG.from_nodes(self.company, self.employee).clone(substitutions)
        assert_that(dag.nodes, has_length(2))
        assert_that(dag.edges, has_length(1))
