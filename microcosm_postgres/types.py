"""
Custom types.

"""
from enum import Enum

from sqlalchemy.types import TypeDecorator, Unicode, UserDefinedType


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
        if isinstance(value, Enum):
            return str(self.enum_class(value).name)
        return str(self.enum_class[value].name)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return self.enum_class[value]


class Serial(UserDefinedType):
    """
    A postgres Serial type.

    Intended for use with auto-incrementing immuatable columns that are NOT primary keys.

    Use in conjuction with `server_default` to ensure that SQLAlchemy fetches the generated value.

        mycolumn = Column(Serial, server_default=FetchedValue(), nullable=False)

    """
    def __init__(self, big=False):
        self.big = big

    def get_col_spec(self, **kwargs):
        """
        Column type is either SERIAL or BIGSERIAL.

        """
        return "BIGSERIAL" if self.big else "SERIAL"

    def bind_processor(self, dialect):
        """
        Always bind null to coerce auto-generation.

        """
        def process(value):
            return value
        return process

    def result_processor(self, dialect, coltype):
        """
        Always return the generated value.

        """
        def process(value):
            return value
        return process
