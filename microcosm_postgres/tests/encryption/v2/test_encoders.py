from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

import pytest

from microcosm_postgres.encryption.v2 import encoders


class AnEnum(Enum):
    FOO = "FOO"
    BAR = "BAR"


@pytest.mark.parametrize(
    ("encoder", "value"),
    [
        (encoders.StringEncoder(), "foo"),
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
