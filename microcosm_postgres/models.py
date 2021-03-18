"""
Support for building models.

Every model must inherit from `Model` and should inherit from the `EntityMixin`.

"""
from time import time
from uuid import uuid4

from sqlalchemy import Column, Float
from sqlalchemy.ext.declarative import declarative_base

from microcosm_postgres.types import EPOCH, UTCDateTime, UUIDType, utcnow


Model = declarative_base()


class PrimaryKeyMixin:
    """
    Define a model with a randomized UUID primary key and tracking created/updated times.

    """
    id = Column(UUIDType(), primary_key=True, default=uuid4)
    created_at = Column(UTCDateTime, default=utcnow, nullable=False)
    updated_at = Column(UTCDateTime, default=utcnow, onupdate=utcnow, nullable=False)

    def new_timestamp(self):
        return utcnow()

    @property
    def created_timestamp(self):
        return (self.created_at.replace(tzinfo=None) - EPOCH).total_seconds()

    @property
    def updated_timestamp(self):
        return (self.updated_at.replace(tzinfo=None) - EPOCH).total_seconds()


class UnixTimestampPrimaryKeyMixin:
    """
    Define a model with a randomized UUID primary key and tracking created/updated times.

    """
    id = Column(UUIDType(), primary_key=True, default=uuid4)
    created_at = Column(Float, default=time, nullable=False)
    updated_at = Column(Float, default=time, onupdate=time, nullable=False)

    def new_timestamp(self):
        return time()

    @property
    def created_timestamp(self):
        return self.created_at

    @property
    def updated_timestamp(self):
        return self.updated_at


class IdentityMixin:
    """
    Define model identity in terms of members.

    This form of equality isn't always appropriate, but it's a good place to start,
    especially for writing test assertions.

    """
    def _members(self):
        """
        Return a dict of non-private members.

        """
        return {
            key: value
            for key, value in self.__dict__.items()
            # NB: ignore internal SQLAlchemy state and nested relationships
            if not key.startswith("_") and not isinstance(value, Model)
        }

    def __eq__(self, other):
        return type(other) is type(self) and self._members() == other._members()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return id(self) if self.id is None else hash(self.id)


class SmartMixin:
    """
    Define a model with short cuts for CRUD operations against its `Store`.

    These short cuts still delegate responsibility for persistence to the store (which must be
    instantiated first).

    """
    def create(self):
        return self.__class__.store.create(self)

    def delete(self):
        return self.__class__.store.delete(self.id)

    def update(self):
        return self.__class__.store.update(self.id, self)

    def update_with_diff(self):
        return self.__class__.store.update_with_diff(self.id, self)

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


class EntityMixin(PrimaryKeyMixin, IdentityMixin, SmartMixin):
    """
    Convention for persistent entities combining other mixins.

    """
    pass


class UnixTimestampEntityMixin(UnixTimestampPrimaryKeyMixin, IdentityMixin, SmartMixin):
    """
    Convention for persistent entities combining other mixins.

    """
    pass
