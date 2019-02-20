from unittest import SkipTest

try:
    from aws_encryption_sdk import decrypt, encrypt  # noqa: F401
except ImportError:
    raise SkipTest
from hamcrest import (
    assert_that,
    contains_inanyorder,
    equal_to,
    is_,
    is_not,
)
from microcosm.api import create_object_graph, load_from_dict

import microcosm_postgres.encryption.factories  # noqa: F401


def cycle(encryptor, encryption_context_key, key_ids):
    plaintext = "The quick brown fox jumped over the lazy dog"

    ciphertext, used_key_ids = encryptor.encrypt(encryption_context_key, plaintext)
    assert_that(ciphertext, is_not(equal_to(plaintext)))
    assert_that(used_key_ids, contains_inanyorder(*key_ids))

    cycled_plaintext = encryptor.decrypt(encryption_context_key, ciphertext)
    assert_that(cycled_plaintext, is_(equal_to(plaintext)))


def test_cycle_single_tenant():
    loader = load_from_dict(
        encryptor=dict(
            default=["key1", "key2"],
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
        encryptor=dict(
            bar=["bar1", "bar2"],
            foo=["foo1", "foo2"],
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
