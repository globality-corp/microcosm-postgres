"""
Compute ORM differences.

"""
from collections import namedtuple
from itertools import chain

from sqlalchemy.orm import class_mapper, ColumnProperty


Change = namedtuple("Change", ["before", "after"])


class Version(dict):
    """
    Capture of object state a specific time.

    """
    def __init__(self, instance):
        super(Version, self).__init__({
            prop.key: getattr(instance, prop.key)
            for prop in class_mapper(instance.__class__).iterate_properties
            if isinstance(prop, ColumnProperty)
        })

    def difference(self, other):
        return Delta(self, other)

    def __sub__(self, other):
        return Delta(self, other)


class Delta(dict):
    """
    Difference beween two object versions.

    """
    def __init__(self, left, right):
        super(Delta, self).__init__({
            key: Change(left.get(key), right.get(key))
            for key in set(chain(list(left.keys()), list(right.keys())))
            if left.get(key) != right.get(key)
        })
