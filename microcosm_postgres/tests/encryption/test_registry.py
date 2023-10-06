"""
Test registry configuration.

"""
from unittest import SkipTest

from hamcrest import assert_that, has_entries
from microcosm.loaders import load_from_dict
from microcosm.object_graph import create_object_graph

from microcosm_postgres.encryption.constants import ENCRYPTION_V2_DEFAULT_KEY
from microcosm_postgres.encryption.registry import parse_config


try:
    from aws_encryption_sdk import EncryptionSDKClient  # noqa: F401
except ImportError:
    raise SkipTest("aws_encryption_sdk not installed")


def test_parse_config_simple():
    assert_that(
        parse_config(
            context_keys=["foo"],
            key_ids=["bar"],
            account_ids=["12345"],
            partitions=["aws"],
            restricted_kms_policy=["false"],
        ),
        has_entries(
            foo=has_entries(
                key_ids=["bar"],
                account_ids=["12345"],
                partition="aws",
            ),
        ),
    )


def test_parse_config_key_ids():
    assert_that(
        parse_config(
            context_keys=["foo", "quux"],
            key_ids=["bar;baz", "quuz;corge"],
            partitions=["aws", "aws-cn"],
            account_ids=["12345;67890", "23456;78901"],
            restricted_kms_policy=["false", "true"],
        ),
        has_entries(
            foo=has_entries(
                key_ids=["bar", "baz"],
                partition="aws",
                account_ids=["12345", "67890"],
                restricted=False,
            ),
            quux=has_entries(
                key_ids=["quuz", "corge"],
                partition="aws-cn",
                account_ids=["23456", "78901"],
                restricted=True,
            ),
        ),
    )


def test_default_encryptor_not_created_when_no_config_available():
    """
    Tests that when we have no config then we don't try to make
    a default encryptor as it can cause errors in the application.

    """
    loader = load_from_dict(
        multi_tenant_key_registry=dict(),
    )

    graph = create_object_graph(
        "example",
        testing=True,
        loader=loader,
        import_name="microcosm_postgres",
    )

    multi_tenant_encryptor = graph.multi_tenant_key_registry.make_encryptor(graph)
    assert len(multi_tenant_encryptor.encryptors) == 0


def test_first_beacon_key_used_in_default_encryptor():
    loader = load_from_dict(
        multi_tenant_key_registry=dict(
            context_keys=["foo", "quux"],
            key_ids=["bar;baz", "quuz;corge"],
            partitions=["aws", "aws-cn"],
            account_ids=["12345;67890", "23456;78901"],
            restricted_kms_policy=["false", "true"],
            beacon_keys=["beacon1", "beacon2"],
        ),
    )

    graph = create_object_graph(
        "example",
        testing=True,
        loader=loader,
        import_name="microcosm_postgres",
    )

    multi_tenant_encryptor = graph.multi_tenant_key_registry.make_encryptor(graph)
    default_encryptor = multi_tenant_encryptor.encryptors[ENCRYPTION_V2_DEFAULT_KEY]
    assert default_encryptor._beacon_key == b"beacon1"
