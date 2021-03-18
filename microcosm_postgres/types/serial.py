from sqlalchemy.types import UserDefinedType


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
