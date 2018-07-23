"""
Extension methods on top of a temporary table.

"""
from microcosm_postgres.context import SessionContext
from sqlalchemy.dialects.postgresql import insert


def insert_many(self, items):
    """
    Insert many items at once into a temporary table.

    Items are expected to extend `IdentityMixin`

    """
    return SessionContext.session.execute(
        self.insert(values=[
            item._members()
            for item in items
        ]),
    ).rowcount


def upsert_into(self, table):
    """
    Upsert from a temporarty table into another table.

    """
    return SessionContext.session.execute(
        insert(table).from_select(
            self.c,
            self,
        ).on_conflict_do_nothing(),
    ).rowcount


def select_from(self, table):
    return SessionContext.session.query(
        table
    ).filter(
        table.id == self.c.id,
    ).all()
