"""
Test custom types.

"""
from hamcrest import (
    assert_that,
    equal_to,
    is_,
    not_none,
)
from microcosm.api import create_object_graph
from sqlalchemy import Column, FetchedValue

from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.models import EntityMixin, Model
from microcosm_postgres.store import Store
from microcosm_postgres.types import Serial


class WithSerial(EntityMixin, Model):
    __tablename__ = "serial_example"

    value = Column(Serial, server_default=FetchedValue(), nullable=False)


class TestSerialType:

    def setup(self):
        self.graph = create_object_graph(name="example", testing=True, import_name="microcosm_postgres")
        self.store = Store(self.graph, WithSerial)

        with SessionContext(self.graph) as context:
            context.recreate_all()

    def teardown(self):
        self.graph.postgres.dispose()

    def test_create_sequence_values(self):
        """
        Creating new values should trigger auto increments.

        """
        with SessionContext(self.graph), transaction():
            for index in range(10):
                example = self.store.create(WithSerial())

                assert_that(example.id, is_(not_none()))
                assert_that(example.value, is_(equal_to(index + 1)))

    def test_retrieve_sequence_value(self):
        """
        Retrieving existing values should return the previously generated sequence.

        """
        with SessionContext(self.graph) as context:
            with transaction():
                example = self.store.create(WithSerial())

            value = example.value
            context.session.expunge(example)

            example = self.store.retrieve(example.id)

        assert_that(example.value, is_(equal_to(value)))

    def test_retrieve_by_sequence_value(self):
        """
        Retrieving existing values should return the previously generated sequence.

        """
        with SessionContext(self.graph):
            with transaction():
                example = self.store.create(WithSerial())

            retrieved = self.store._query().filter(
                WithSerial.value == example.value,
            ).one()

        assert_that(retrieved.id, is_(equal_to(example.id)))

    def test_update_sequence(self):
        """
        Updating a sequence is allowed (but not advised).

        """
        with SessionContext(self.graph) as context:
            with transaction():
                example = self.store.create(WithSerial())

            previous_value = example.value
            example.value = example.value + 1

            with transaction():
                self.store.replace(example.id, example)

            context.session.expunge(example)

            example = self.store.retrieve(example.id)

        assert_that(example.value, is_(previous_value + 1))

    def test_delete_does_not_reset_sequence(self):
        """
        Deletion does not reset the sequence.

        """
        with SessionContext(self.graph):
            with transaction():
                example = self.store.create(WithSerial())

            previous_value = example.value

            with transaction():
                self.store.delete(example.id)

            with transaction():
                example = self.store.create(WithSerial())

            assert_that(example.value, is_(equal_to(previous_value + 1)))
