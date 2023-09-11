from decimal import Decimal
import pytest

from microcosm_postgres.encryption.v2 import encoders


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
    ],
)
def test_encode_decode(encoder, value):
    assert encoder.decode(encoder.encode(value)) == value
