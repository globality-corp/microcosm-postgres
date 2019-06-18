"""
Persistence tests for company store.

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
from microcosm_postgres.errors import DuplicateModelError, ModelNotFoundError, ReferencedModelError
from microcosm_postgres.tests.fixtures import Company, CompanyType, Employee


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
        self.graph.postgres.dispose()

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

    def test_create_delete_company_complicated_expression(self):
        """
        Delete should support more complicated criterion with the `fetch` synchronization strategy enabled.

        """
        with transaction():
            company = Company(name="name").create()

        with transaction():
            self.company_store._delete(Company.name.in_(["name"]), synchronize_session="fetch")

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

        # Pagination fields do not affect count calculations
        assert_that(self.company_store.count(offset=1, limit=1), is_(equal_to(2)))

        assert_that([company.id for company in Company.search()], contains_inanyorder(company1.id, company2.id))

    def test_create_update_company(self):
        """
        Should be able to update a company after creating it.

        """
        with transaction():
            company = Company(
                name="name",
            ).create()

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
            Company(
                name="name1",
            ).create()
            company = Company(
                name="name2",
            ).create()

        with transaction():
            Employee(
                first="first",
                last="last",
                company_id=company.id,
            ).create()

        assert_that(calling(company.delete), raises(ReferencedModelError))
