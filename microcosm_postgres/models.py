"""
Support for building models.

Every model must inherit from `Model` and should inherit from the `EntityMixin`.

"""
from datetime import datetime
from dateutil.tz import tzutc
from uuid import uuid4

from sqlalchemy import Column, types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import UUIDType


Model = declarative_base()


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


class EntityMixin(object):
    """
    A convention for persistent entities:

     - Use a randomized UUID primary key.
     - Track created and updated times in UTC
     - Short cuts for CRUD operations via store indirection

    """
    id = Column(UUIDType(), primary_key=True, default=uuid4)
    created_at = Column(UTCDateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(UTCDateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Entity short cuts to current store; will fail if no store was configured

    def create(self):
        return self.__class__.store.create(self)

    def delete(self):
        return self.__class__.store.delete(self.id)

    def update(self):
        return self.__class__.store.update(self.id, self)

    def replace(self):
        return self.__class__.store.replace(self.id, self)

    @classmethod
    def search(cls, *criterion, **kwargs):
        return cls.store.search(*criterion, **kwargs)

    @classmethod
    def count(cls, *criterion):
        return cls.store.count(*criterion)

    @classmethod
    def retrieve(cls, identifier):
        return cls.store.retrieve(identifier)
