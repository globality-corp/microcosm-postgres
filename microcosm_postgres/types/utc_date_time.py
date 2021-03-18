from datetime import datetime

from dateutil.tz import tzutc
from pytz import utc
from sqlalchemy import types


EPOCH = datetime(1970, 1, 1)


def utcnow():
    """
    Create a non-naive UTC datetime for the current time.

    Needed when *updating* UTCDateTime values because result values are currently
    converted to non-naive datetimes and SQLAlchemy cannot compare these values
    with naive datetimes generated from `datetime.utcnow()`

    """
    return datetime.now(utc)


class UTCDateTime(types.TypeDecorator):
    """
    SQLAlchemy type definition that converts stored datetime to UTC automatically.
    Source: http://stackoverflow.com/a/2528453

    """

    impl = types.DateTime

    def process_bind_param(self, value, engine):
        if value is not None:
            result = value.replace(tzinfo=None)
            return result
        else:
            return value

    def process_result_value(self, value, engine):
        if value is not None:
            result = datetime(
                value.year, value.month, value.day,
                value.hour, value.minute, value.second,
                value.microsecond, tzinfo=tzutc(),
            )
            return result
        else:
            return value
