"""
Temporary table support.

"""
from types import MethodType

from microcosm_postgres.temporary.copy import copy_table
from microcosm_postgres.temporary.methods import (
    insert_many,
    select_from,
    upsert_into,
)


def create_temporary_table(from_table, name=None, on_commit=None):
    """
    Create a new temporary table from another table.

    """
    from_table = from_table.__table__ if hasattr(from_table, "__table__") else from_table

    name = name or f"temporary_{from_table.name}"

    # copy the origin table into the metadata with the new name.
    temporary_table = copy_table(from_table, name)

    # change create clause to: CREATE TEMPORARY TABLE
    temporary_table._prefixes = list(from_table._prefixes)
    temporary_table._prefixes.append("TEMPORARY")

    # change post create clause to: ON COMMIT DROP
    if on_commit:
        temporary_table.dialect_options["postgresql"].update(
            on_commit=on_commit,
        )

    temporary_table.insert_many = MethodType(insert_many, temporary_table)
    temporary_table.select_from = MethodType(select_from, temporary_table)
    temporary_table.upsert_into = MethodType(upsert_into, temporary_table)

    return temporary_table


def create_transient_table(from_table, name=None):
    """
    Create a transient table.

    The transient table will be dropped on commit.

    """
    return create_temporary_table(from_table, name=name, on_commit="drop")
