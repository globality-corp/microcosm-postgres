"""
Test DAG.

"""
from hamcrest import (
    assert_that,
    calling,
    contains,
    equal_to,
    has_entries,
    has_length,
    is_,
    raises,
)
from microcosm.api import create_object_graph

from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.dag import DAG, Edge
from microcosm_postgres.errors import ModelNotFoundError
from microcosm_postgres.tests.example import Company, CompanyType, Employee


class TestCloning:

    def setup(self):
        self.graph = create_object_graph(name="example", testing=True, import_name="microcosm_postgres")
        self.company_dag_store = self.graph.company_dag_store
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

    def test_explain_anonymize(self):
        dag = self.company_dag_store.explain(company_id=self.company.id, anonymize=True)
        assert_that(dag.nodes, has_entries({
            self.company.id: self.company,
            self.employee.id: self.employee,
        }))
        persisted_employee = self.employee_store.retrieve(self.employee.id)
        assert_that(dag.nodes_map["employee"][0].last, is_(equal_to("doe")))
        assert_that(persisted_employee.last, is_(equal_to("last")))

    def _model_to_dict(self, instance):
        return {
            c.name: getattr(instance, c.name)
            for c in instance.__table__.columns
        }

    def test_create_from_serialized_dag(self):
        explain_dag = DAG.from_nodes(self.company, self.employee)
        assert_that(sorted(explain_dag.nodes_map.keys()), contains("company", "employee"))
        serialized_node_map = {
            model_name: [
                self._model_to_dict(node)
                for node in model_nodes
            ]
            for model_name, model_nodes in explain_dag.nodes_map.items()
        }
        serialized_edges = [edge._asdict() for edge in explain_dag.edges]

        self.employee_store.delete(self.employee.id)
        self.company_store.delete(self.company.id)

        # make sure events got erased
        assert_that(
            calling(self.company_store.retrieve).with_args(str(self.company.id)),
            raises(ModelNotFoundError),
        )

        self.company_dag_store.replace_dag(
            nodes_map=serialized_node_map,
            edges=serialized_edges,
        )
        assert_that(self.company_store.retrieve(self.company.id).name, is_(equal_to(self.company.name)))
        assert_that(self.employee_store.retrieve(self.employee.id).first, is_(equal_to(self.employee.first)))
