"""
Extension methods on top of a temporary table.

"""
import warnings

from sqlalchemy.dialects.postgresql import insert

from microcosm_postgres.context import SessionContext


def to_dict(item, columns):
    column_values = (
        (column, getattr(item, column.name))
        for column in columns
    )
    return dict(
        (column.name, value)
        for column, value in column_values
        # discard nulls if defaulted
        if value is not None or not column.default
    )


def insert_many(self, items):
    """
    Insert many items at once into a temporary table.

    """
    return SessionContext.session.execute(
        self.insert(values=[
            to_dict(item, self.c)
            for item in items
        ]),
    ).rowcount


def upsert_into(self, table):
    """
    Deprecated. Use `upsert_into_on_conflict_do_nothing` instead

    """
    warnings.warn(
        "`upsert_into` is deprecated. Please use `upsert_into_on_conflict_do_nothing`",
        DeprecationWarning,
    )
    return self.upsert_into_on_conflict_do_nothing(table)


def upsert_into_on_conflict_do_nothing(self, table):
    """
    Upsert from a temporarty table into another table.

    """
    return SessionContext.session.execute(
        insert(table).from_select(
            self.c,
            self,
        ).on_conflict_do_nothing(),
    ).rowcount


def upsert_into_on_conflict_do_update(self, table, **on_conflict_kwargs):
    return SessionContext.session.execute(
        insert(table).from_select(
            self.c,
            self,
        ).on_conflict_do_update(
            **on_conflict_kwargs,
        ),
    ).rowcount


def select_from(self, table):
    return SessionContext.session.query(
        table
    ).filter(
        table.id == self.c.id,
    ).all()
