"""
Custom types.

"""
from six import text_type

from sqlalchemy.types import TypeDecorator, Unicode


class EnumType(TypeDecorator):
    """
    SQLAlchemy enum type that persists the enum name (not value).

    Note that this type is very similar to the `ChoiceType` from `sqlalchemy_utils`,
    with the key difference being persisting by name (and not value).

    """
    impl = Unicode(255)

    def __init__(self, enum_class):
        self.enum_class = enum_class

    @property
    def python_type(self):
        return self.impl.python_type

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return text_type(self.enum_class(value).name)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return self.enum_class[value]
