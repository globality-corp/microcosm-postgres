from datetime import datetime

from hamcrest import (
    assert_that,
    contains,
    equal_to,
    greater_than,
    has_properties,
    is_,
)
from microcosm.api import create_object_graph

from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.temporary import transient
from microcosm_postgres.tests.fixtures.company import Company, CompanyType


class TestTransient:

    def setup(self):
        self.graph = create_object_graph(
            name="example",
            testing=True,
            import_name="microcosm_postgres",
        )
        self.company_store = self.graph.company_store

        self.companies = [
            Company(
                name="name1",
                type=CompanyType.private,
            ),
            Company(
                name="name2",
                type=CompanyType.private,
            ),
            Company(
                name="name3",
                type=CompanyType.private,
            ),
        ]

        with SessionContext(self.graph) as context:
            context.recreate_all()

    def test_upsert_into(self):
        with SessionContext(self.graph):
            with transaction():
                # NB: create() will set the id of companies[0]
                self.companies[0].create()

            with transaction():
                with transient(Company) as transient_company:
                    assert_that(
                        transient_company.insert_many(self.companies),
                        is_(equal_to(3)),
                    )
                    assert_that(
                        transient_company.upsert_into_on_conflict_do_nothing(Company),
                        is_(equal_to(2)),
                    )
                    assert_that(
                        self.company_store.count(),
                        is_(equal_to(3)),
                    )

    def test_upsert_into_on_conflict_do_update(self):
        with SessionContext(self.graph):
            with transaction():
                # NB: create() will set the id of companies[0]
                self.companies[0].create()

            old_updated_at = self.companies[0].updated_at
            with transaction():
                with transient(Company) as transient_company:
                    assert_that(
                        transient_company.insert_many(self.companies),
                        is_(equal_to(3)),
                    )
                    assert_that(
                        transient_company.upsert_into_on_conflict_do_update(
                            Company,
                            index_elements=["id"],
                            set_=dict(
                                type=CompanyType.public,
                                updated_at=datetime.now(),
                            ),
                        ),
                        is_(equal_to(3)),
                    )
                    assert_that(
                        self.company_store.count(),
                        is_(equal_to(3)),
                    )

        with SessionContext(self.graph):
            company_0 = self.company_store.retrieve(self.companies[0].id)
            assert_that(
                company_0.type,
                equal_to(CompanyType.public),
            )
            assert_that(
                company_0.updated_at,
                greater_than(old_updated_at),
            )

    def test_select_from_none(self):
        with SessionContext(self.graph):
            with transient(Company) as transient_company:
                assert_that(
                    transient_company.select_from(Company),
                    contains(),
                )

    def test_select_from_partial(self):
        with SessionContext(self.graph):
            with transaction():
                with transient(Company) as transient_company:
                    transient_company.insert_many(self.companies)
                    self.companies[0].create()
                    transient_company.upsert_into(Company)

                assert_that(
                    transient_company.select_from(Company),
                    contains(
                        has_properties(
                            name="name2",
                        ),
                        has_properties(
                            name="name3",
                        ),
                    )
                )
