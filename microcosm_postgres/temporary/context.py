"""
Transient table context manager.

"""
from contextlib import contextmanager

from microcosm_postgres.context import SessionContext
from microcosm_postgres.temporary.factories import create_transient_table


@contextmanager
def transient(from_table):
    if not SessionContext.session:
        raise Exception("transient() must be called within a SessionContext")

    transient_table = create_transient_table(from_table)

    bind = SessionContext.session.connection()
    transient_table.create(bind=bind)

    try:
        yield transient_table
    finally:
        transient_table.metadata.remove(transient_table)
