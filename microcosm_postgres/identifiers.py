"""
Identifier utilities.

"""
from uuid import uuid4


def new_object_id():
    """
    Use randomized UUIDs by default.

    """
    return uuid4()
