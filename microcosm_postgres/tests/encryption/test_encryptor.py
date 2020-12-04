from unittest import SkipTest
from unittest.mock import patch

from hamcrest import (
    assert_that,
    contains_inanyorder,
    equal_to,
    is_,
    is_not,
)
from microcosm.api import create_object_graph, load_from_dict

import microcosm_postgres.encryption.factories  # noqa: F401


try:
    from aws_encryption_sdk import EncryptionSDKClient  # noqa: F401
except ImportError:
    raise SkipTest


def cycle(encryptor, encryption_context_key, key_ids):
    plaintext = "The quick brown fox jumped over the lazy dog"

    ciphertext, used_key_ids = encryptor.encrypt(encryption_context_key, plaintext)
    assert_that(ciphertext, is_not(equal_to(plaintext)))
    assert_that(used_key_ids, contains_inanyorder(*key_ids))

    cycled_plaintext = encryptor.decrypt(encryption_context_key, ciphertext)
    assert_that(cycled_plaintext, is_(equal_to(plaintext)))


def test_cycle_single_tenant():
    loader = load_from_dict(
        multi_tenant_key_registry=dict(
            context_keys=[
                "default",
            ],
            key_ids=[
                ["key1", "key2"],
            ],
        ),
    )
    graph = create_object_graph(
        name="example",
        testing=True,
        import_name="microcosm_postgres",
        loader=loader,
    )

    cycle(
        encryptor=graph.multi_tenant_encryptor,
        encryption_context_key="whatever",
        key_ids=["key1", "key2"],
    )


def test_cycle_multi_tenant():
    loader = load_from_dict(
        multi_tenant_key_registry=dict(
            context_keys=[
                "foo",
                "bar",
            ],
            key_ids=[
                ["foo1", "foo2"],
                ["bar1", "bar2"],
            ],
        ),
    )
    graph = create_object_graph(
        name="example",
        testing=True,
        import_name="microcosm_postgres",
        loader=loader,
    )

    cycle(
        encryptor=graph.multi_tenant_encryptor,
        encryption_context_key="foo",
        key_ids=["foo1", "foo2"],
    )

    cycle(
        encryptor=graph.multi_tenant_encryptor,
        encryption_context_key="bar",
        key_ids=["bar1", "bar2"],
    )


def test_cycle_cache():
    loader = load_from_dict(
        materials_manager=dict(
            enable_cache=True,
        ),
        multi_tenant_key_registry=dict(
            context_keys=[
                "default",
            ],
            key_ids=[
                ["key1", "key2"],
            ],
        ),
    )
    graph = create_object_graph(
        name="example",
        testing=True,
        import_name="microcosm_postgres",
        loader=loader,
    )
    encryptor = graph.multi_tenant_encryptor.encryptors["default"]
    master_key_provider = encryptor.materials_manager.master_key_provider
    decrypt_data_key = master_key_provider.decrypt_data_key

    with patch.object(master_key_provider, "decrypt_data_key") as mocked_decrypt_data_key:
        mocked_decrypt_data_key.side_effect = decrypt_data_key
        for _ in range(5):
            cycle(
                encryptor=graph.multi_tenant_encryptor,
                encryption_context_key="whatever",
                key_ids=["key1", "key2"],
            )

    assert_that(
        mocked_decrypt_data_key.call_count,
        is_(equal_to(1)),
    )
