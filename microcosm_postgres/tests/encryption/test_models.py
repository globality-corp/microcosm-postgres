from unittest import SkipTest

try:
    from aws_encryption_sdk import decrypt, encrypt  # noqa: F401
except ImportError:
    raise SkipTest
from hamcrest import (
    assert_that,
    calling,
    equal_to,
    has_properties,
    is_,
    is_not,
    raises,
    none,
)
from microcosm.api import create_object_graph, load_from_dict

from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.errors import ModelIntegrityError
from microcosm_postgres.tests.encryption.fixtures.encrpytable import Encryptable
from microcosm_postgres.tests.encryption.fixtures.json_encrpytable import JsonEncryptable

import microcosm_postgres.encryption.factories  # noqa: F401


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
