from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

import pytest

from microcosm_postgres.encryption.v2 import encoders
from microcosm_postgres.encryption.v2.encoders import Encoder


class AnEnum(Enum):
    FOO = "FOO"
    BAR = "BAR"


@pytest.mark.parametrize(
    ("encoder", "value"),
    [
        (encoders.StringEncoder(), "foo"),
        (encoders.TextEncoder(), "The quick brown fox jumps over the lazy dog"),
        (encoders.IntEncoder(), 1),
        (encoders.DecimalEncoder(), Decimal(1.0)),
        (encoders.ArrayEncoder(encoders.StringEncoder()), ["foo", "bar"]),
        (encoders.ArrayEncoder(encoders.IntEncoder()), [1, 2]),
        (encoders.JSONEncoder(), {"foo": "bar", "something_else": []}),
        (encoders.Nullable(encoders.StringEncoder()), "foo"),
        (encoders.Nullable(encoders.StringEncoder()), None),
        (encoders.DatetimeEncoder(), datetime.now()),
        (encoders.DatetimeEncoder(), datetime.now(timezone.utc)),
        (encoders.EnumEncoder(AnEnum), AnEnum.FOO),
        (encoders.Nullable(encoders.EnumEncoder(AnEnum)), None),
    ],
)
def test_encode_decode(encoder, value):
    assert encoder.decode(encoder.encode(value)) == value


@pytest.mark.parametrize(
    ("input", "output"),
    [
        ("foo", "foo"),
        (1, "1"),
        (Decimal(1.0), "1"),
        (True, "True"),
    ],
)
def test_string_encoder(input, output):
    assert encoders.StringEncoder().encode(input) == output


@pytest.mark.parametrize(
    ("input", "output"),
    [
        ("foo", "foo"),
        (1, "1"),
        (Decimal(1.0), "1"),
        (True, "True"),
    ],
)
def test_text_encoder(input, output):
    assert encoders.TextEncoder().encode(input) == output


@pytest.mark.parametrize(
    ("encoder", "value", "exception"),
    [
        # Decoding an invalid string to int
        (encoders.IntEncoder(), "abc", Encoder.DecodeException),

        # Decoding an invalid string to decimal
        (encoders.DecimalEncoder(), "abc.xyz", Encoder.DecodeException),

        # Decoding a non-list encoded string to array
        (encoders.ArrayEncoder(encoders.StringEncoder()), "{foo,bar}", Encoder.DecodeException),

        # Decoding a non-integer list encoded string
        (encoders.ArrayEncoder(encoders.IntEncoder()), "[foo, bar]", Encoder.DecodeException),

        # Decoding an invalid JSON string
        (encoders.JSONEncoder(), "{'foo':'bar'}", Encoder.DecodeException),

        # Decoding an invalid Nullable value
        (encoders.Nullable(encoders.StringEncoder()), 123, Encoder.DecodeException),
        (encoders.Nullable(encoders.TextEncoder()), 123, Encoder.DecodeException),

        # Decoding an invalid datetime string
        (encoders.DatetimeEncoder(), "2021-25-09T25:63:75", Encoder.DecodeException),

        # Decoding an invalid enum string
        (encoders.EnumEncoder(AnEnum), "INVALID_ENUM", Encoder.DecodeException),

        # Decoding an invalid nullable enum string
        (encoders.Nullable(encoders.EnumEncoder(AnEnum)), "INVALID_ENUM", Encoder.DecodeException),
    ],
)
def test_encode_decode_errors(encoder, value, exception):
    with pytest.raises(exception):
        if exception == Encoder.EncodeException:
            encoder.encode(value)
        else:
            encoder.decode(value)
