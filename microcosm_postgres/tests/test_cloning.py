"""
Test cloning.

"""
from hamcrest import assert_that, equal_to, is_, is_not, not_none
from microcosm.api import create_object_graph

from microcosm_postgres.cloning import clone
from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.tests.example import Company, CompanyType


class TestCloning:

    def setup(self):
        self.graph = create_object_graph(name="example", testing=True, import_name="microcosm_postgres")
        self.company_store = self.graph.company_store

        self.context = SessionContext(self.graph)
        self.context.recreate_all()
        self.context.open()

    def teardown(self):
        self.context.close()

    def test_clone(self):
        with transaction():
            company = Company(
                name="name",
                type=CompanyType.private,
            ).create()

        with transaction():
            copy = clone(company, dict(name="newname"))

        assert_that(copy.id, is_not(equal_to(company.id)))
        assert_that(self.company_store.retrieve(copy.id), is_(not_none()))
