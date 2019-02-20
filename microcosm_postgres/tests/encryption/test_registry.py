"""
Test registry configuration.

"""
from unittest import SkipTest

try:
    from aws_encryption_sdk import decrypt, encrypt  # noqa: F401
except ImportError:
    raise SkipTest
from hamcrest import assert_that, equal_to, is_

from microcosm_postgres.encryption.registry import parse_config


def test_parse_config_simple():
    assert_that(
        parse_config(
            context_keys=["foo"],
            key_ids=["bar"],
        ),
        is_(equal_to(dict(
            foo=["bar"],
        ))),
    )


def test_parse_config_key_ids():
    assert_that(
        parse_config(
            context_keys=["foo"],
            key_ids=["bar,baz"],
        ),
        is_(equal_to(dict(
            foo=["bar", "baz"],
        ))),
    )
