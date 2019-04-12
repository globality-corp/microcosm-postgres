from unittest import SkipTest

from hamcrest import (
    assert_that,
    calling,
    equal_to,
    has_properties,
    is_,
    is_not,
    none,
    raises,
)
from microcosm.api import create_object_graph, load_from_dict

import microcosm_postgres.encryption.factories  # noqa: F401
from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.errors import ModelIntegrityError
from microcosm_postgres.tests.encryption.fixtures.encryptable import Encryptable
from microcosm_postgres.tests.encryption.fixtures.json_encryptable import JsonEncryptable
from microcosm_postgres.tests.encryption.fixtures.nullable_encryptable import NullableEncryptable


try:
    from aws_encryption_sdk import decrypt, encrypt  # noqa: F401
except ImportError:
    raise SkipTest


class TestEncryptable:

    def setup(self):
        loader = load_from_dict(
            multi_tenant_key_registry=dict(
                context_keys=[
                    "private",
                ],
                key_ids=[
                    "key_id",
                ],
            ),
        )
        self.graph = create_object_graph(
            name="example",
            testing=True,
            import_name="microcosm_postgres",
            loader=loader,
        )
        self.encryptable_store = self.graph.encryptable_store
        self.encrypted_store = self.graph.encrypted_store
        self.json_encryptable_store = self.graph.json_encryptable_store
        self.json_encrypted_store = self.graph.json_encrypted_store
        self.nullable_encryptable_store = self.graph.nullable_encryptable_store
        self.nullable_encrypted_store = self.graph.nullable_encrypted_store
        self.encryptor = self.graph.multi_tenant_encryptor

        with SessionContext(self.graph) as context:
            context.recreate_all()

    def teardown(self):
        self.graph.postgres.dispose()

    def test_not_encrypted(self):
        with SessionContext(self.graph):
            with transaction():
                encryptable = self.encryptable_store.create(
                    Encryptable(
                        key="key",
                        value="value",
                    ),
                )

            assert_that(
                encryptable,
                has_properties(
                    key=is_(equal_to("key")),
                    value=is_(equal_to("value")),
                    encrypted_id=is_(none()),
                ),
            )
            assert_that(
                self.encryptable_store.count(), is_(equal_to(1)),
            )
            assert_that(
                self.encrypted_store.count(), is_(equal_to(0)),
            )

            res = self.encryptable_store.retrieve(encryptable.id)
            assert_that(
                res,
                has_properties(
                    key=is_(equal_to("key")),
                    value=is_(equal_to("value")),
                    encrypted_id=is_(none()),
                ),
            )

            with transaction():
                self.encryptable_store.delete(encryptable.id)

            assert_that(
                self.encryptable_store.count(), is_(equal_to(0)),
            )

    def test_encrypted(self):
        with SessionContext(self.graph):
            with transaction():
                encryptable = self.encryptable_store.create(
                    Encryptable(
                        key="private",
                        value="value",
                    ),
                )

            assert_that(
                encryptable,
                has_properties(
                    key=is_(equal_to("private")),
                    value=is_(none()),
                    encrypted_id=is_not(none()),
                ),
            )
            assert_that(
                encryptable._members(),
                is_(equal_to(dict(
                    created_at=encryptable.created_at,
                    encrypted_id=encryptable.encrypted_id,
                    id=encryptable.id,
                    key=encryptable.key,
                    updated_at=encryptable.updated_at,
                ))),
            )
            assert_that(
                self.encryptable_store.count(), is_(equal_to(1)),
            )
            assert_that(
                self.encrypted_store.count(), is_(equal_to(1)),
            )

            # NB: ORM events will not trigger if we can reuse the object from the session cache
            self.encryptable_store.expunge(encryptable)

            encryptable = self.encryptable_store.retrieve(encryptable.id)
            assert_that(
                encryptable,
                has_properties(
                    key=is_(equal_to("private")),
                    value=is_(equal_to("value")),
                    encrypted_id=is_not(none()),
                ),
            )

            with transaction():
                self.encryptable_store.delete(encryptable.id)

            assert_that(
                self.encryptable_store.count(), is_(equal_to(0)),
            )
            assert_that(
                self.encrypted_store.count(), is_(equal_to(0)),
            )

    def test_throw_model_integrity_when_value_is_none(self):
        with SessionContext(self.graph):
            assert_that(
                calling(self.encryptable_store.create).with_args(
                    Encryptable(
                        key="private",
                        value=None,
                    ),
                ),
                raises(ModelIntegrityError),
            )

    def test_json_encrypted(self):
        with SessionContext(self.graph):
            with transaction():
                encryptable = self.json_encryptable_store.create(
                    JsonEncryptable(
                        key="private",
                        value=["foo", {"bar": "baz"}],
                    ),
                )

            assert_that(
                encryptable,
                has_properties(
                    key=is_(equal_to("private")),
                    value=is_(none()),
                    json_encrypted_id=is_not(none()),
                ),
            )
            assert_that(
                encryptable._members(),
                is_(equal_to(dict(
                    created_at=encryptable.created_at,
                    json_encrypted_id=encryptable.json_encrypted_id,
                    id=encryptable.id,
                    key=encryptable.key,
                    updated_at=encryptable.updated_at,
                ))),
            )
            assert_that(
                self.json_encryptable_store.count(), is_(equal_to(1)),
            )
            assert_that(
                self.json_encrypted_store.count(), is_(equal_to(1)),
            )

            # NB: ORM events will not trigger if we can reuse the object from the session cache
            self.json_encryptable_store.expunge(encryptable)

            encryptable = self.json_encryptable_store.retrieve(encryptable.id)
            assert_that(
                encryptable,
                has_properties(
                    key=is_(equal_to("private")),
                    value=is_(equal_to(["foo", {"bar": "baz"}])),
                    json_encrypted_id=is_not(none()),
                ),
            )

    def test_update(self):
        with SessionContext(self.graph):
            with transaction():
                encryptable = self.encryptable_store.create(
                    Encryptable(
                        key="private",
                        value="value",
                    ),
                )

        with SessionContext(self.graph):
            with transaction():
                res = self.encryptable_store.update(
                    encryptable.id,
                    Encryptable(
                        id=encryptable.id,
                        # We don't have to pass the key again in order to encrypt the new value
                        value="new-value",
                    ),
                )
                assert_that(
                    res,
                    has_properties(
                        key=is_(equal_to("private")),
                        value=is_(equal_to("new-value")),
                        encrypted_id=is_not(none()),
                    ),
                )

        with SessionContext(self.graph):
            encryptable = self.encryptable_store.retrieve(encryptable.id)

            assert_that(
                encryptable,
                has_properties(
                    key=is_(equal_to("private")),
                    value=is_(equal_to("new-value")),
                    encrypted_id=is_not(none()),
                ),
            )

            assert_that(
                self.encryptable_store.count(), is_(equal_to(1)),
            )
            assert_that(
                self.encrypted_store.count(), is_(equal_to(1)),
            )

    def test_update_with_key(self):
        with SessionContext(self.graph):
            with transaction():
                encryptable = self.encryptable_store.create(
                    Encryptable(
                        key="private",
                        value="value",
                    ),
                )

        with SessionContext(self.graph):
            with transaction():
                res = self.encryptable_store.update(
                    encryptable.id,
                    Encryptable(
                        id=encryptable.id,
                        # Pass the key
                        key="private",
                        value="new-value",
                    ),
                )
                assert_that(
                    res,
                    has_properties(
                        key=is_(equal_to("private")),
                        value=is_(equal_to("new-value")),
                        encrypted_id=is_not(none()),
                    ),
                )

        with SessionContext(self.graph):
            encryptable = self.encryptable_store.retrieve(encryptable.id)

            assert_that(
                encryptable,
                has_properties(
                    key=is_(equal_to("private")),
                    value=is_(equal_to("new-value")),
                    encrypted_id=is_not(none()),
                ),
            )

            assert_that(
                self.encryptable_store.count(), is_(equal_to(1)),
            )
            assert_that(
                self.encrypted_store.count(), is_(equal_to(1)),
            )

    def test_update_from_null(self):
        with SessionContext(self.graph):
            with transaction():
                encryptable = self.nullable_encryptable_store.create(
                    NullableEncryptable(
                        key="private",
                        value=None,
                    ),
                )

        with SessionContext(self.graph):
            with transaction():
                res = self.nullable_encryptable_store.update(
                    encryptable.id,
                    NullableEncryptable(
                        id=encryptable.id,
                        value="new-value",
                    ),
                )
                assert_that(
                    res,
                    has_properties(
                        key=is_(equal_to("private")),
                        value=is_(equal_to("new-value")),
                        encrypted_id=is_not(none()),
                    ),
                )

        with SessionContext(self.graph):
            encryptable = self.nullable_encryptable_store.retrieve(encryptable.id)

            assert_that(
                encryptable,
                has_properties(
                    key=is_(equal_to("private")),
                    value=is_(equal_to("new-value")),
                    encrypted_id=is_not(none()),
                ),
            )

            assert_that(
                self.nullable_encryptable_store.count(), is_(equal_to(1)),
            )
            assert_that(
                self.nullable_encrypted_store.count(), is_(equal_to(1)),
            )
