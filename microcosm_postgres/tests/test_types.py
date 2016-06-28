"""
Test custom types.

"""
from hamcrest import (
    assert_that,
    calling,
    equal_to,
    is_,
    not_none,
    raises,
)
from microcosm.api import create_object_graph
from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.errors import ModelIntegrityError
from microcosm_postgres.models import EntityMixin, Model
from microcosm_postgres.store import Store
from microcosm_postgres.types import Serial
from sqlalchemy import Column, FetchedValue


class Sequential(EntityMixin, Model):
    __tablename__ = "example"

    value = Column(Serial, server_default=FetchedValue(), nullable=False)


class TestSequential(object):

    def setup(self):
        self.graph = create_object_graph(name="example", testing=True, import_name="microcosm_postgres")
        self.context = SessionContext(self.graph)
        self.context.recreate_all()
        self.context.open()
        self.store = Store(self.graph, Sequential)

    def teardown(self):
        self.context.close()

    def test_create_sequence_values(self):
        """
        Creating new values should trigger auto increments.

        """
        with transaction():
            examples = [
                self.store.create(Sequential())
                for _ in range(10)
            ]

        for index, example in enumerate(examples):
            assert_that(examples[index].id, is_(not_none()))
            assert_that(examples[index].value, is_(equal_to(index + 1)))

    def test_retrieve_sequence_value(self):
        """
        Retrieving existing values should return the previously generated sequence.

        """
        with transaction():
            example = self.store.create(Sequential())

        self.context.session.expunge(example)

        example = self.store.retrieve(example.id)

        assert_that(example.value, is_(equal_to(1)))

    def test_retrieve_by_sequence_value(self):
        """
        Retrieving existing values should return the previously generated sequence.

        """
        with transaction():
            example = self.store.create(Sequential())

        retrieved = self.store._query().filter(
            Sequential.value == example.value,
        ).one()
        assert_that(retrieved.id, is_(equal_to(example.id)))

    def test_update_sequence(self):
        """
        Updating a sequence is allowed (but not advised).

        """
        with transaction():
            example = self.store.create(Sequential())

        example.value = example.value + 1

        with transaction():
            self.store.replace(example.id, example)

        self.context.session.expunge(example)

        example = self.store.retrieve(example.id)

        assert_that(example.value, is_(equal_to(2)))

    def test_delete_does_not_reset_sequence(self):
        """
        Deletion does not reset the sequence.

        """
        with transaction():
            example = self.store.create(Sequential())

        with transaction():
            self.store.delete(example.id)

        with transaction():
            example = self.store.create(Sequential())

        assert_that(example.value, is_(equal_to(2)))
