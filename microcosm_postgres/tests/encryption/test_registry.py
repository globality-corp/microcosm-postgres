"""
Test registry configuration.

"""
from unittest import SkipTest

from hamcrest import assert_that, has_entries

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
        ),
        has_entries(
            foo=has_entries(
                key_ids=["bar", "baz"],
                partition="aws",
                account_ids=["12345", "67890"],
            ),
            quux=has_entries(
                key_ids=["quuz", "corge"],
                partition="aws-cn",
                account_ids=["23456", "78901"],
            ),
        ),
    )
