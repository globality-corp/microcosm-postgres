"""
Identity model tests.

"""
from hamcrest import (
    assert_that,
    equal_to,
    is_,
    is_not,
)
from microcosm.api import create_object_graph

from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.tests.fixtures import Company, CompanyType


class TestIdentity:

    def setup_method(self):
        self.graph = create_object_graph(name="example", testing=True, import_name="microcosm_postgres")
        self.graph.use("company_store")

        context = SessionContext(self.graph)
        context.recreate_all()

    def teardown(self):
        self.graph.postgres.dispose()

    def _make_company(self):
        with SessionContext(self.graph), transaction():
            return Company(
                name="name",
                type=CompanyType.private,
            ).create()

    def _retrieve_company(self, company_id):
        with SessionContext(self.graph):
            return Company.retrieve(company_id)

    def test_identity(self):
        # load companies in different sessions
        company1 = self._make_company()
        company2 = self._retrieve_company(company1.id)

        # these are different object
        assert_that(id(company1), is_not(equal_to(id(company2))))

        # but have the same id
        assert_that(company1.id, is_(equal_to(company2.id)))

        # and should evaluate as equal
        assert_that(company1, is_(equal_to(company2)))

        # and should have the same hash value
        assert_that(hash(company1), is_(equal_to(hash(company2))))
