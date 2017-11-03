"""
Object cloning.

"""
from sqlalchemy.inspection import inspect

from microcosm_postgres.identifiers import new_object_id


def clone(instance, substitutions, ignore=()):
    """
    Clone an instance of `Model` that uses `IdentityMixin`.

    :param instance: the instance to clonse
    :param substitutions: a dictionary of substitutions
    :param ignore: a tuple of column names to ignore

    """
    substitutions[instance.id] = new_object_id()

    def substitute(value):
        try:
            hash(value)
        except Exception:
            return value
        else:
            return substitutions.get(value, value)

    return instance.__class__(**{
        key:  substitute(value)
        for key, value in iter_items(instance, ignore)
    }).create()


def iter_items(instance, ignore):
    cls = instance.__class__
    mapper = inspect(cls)
    for key, value in instance.__dict__.items():
        if key not in mapper.c:
            continue
        if key in ignore:
            continue
        yield key, value
