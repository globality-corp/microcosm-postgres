"""
Copy a table.

"""
from sqlalchemy import Table

from microcosm_postgres.types import Serial


def copy_column(column, schema):
    """
    Safely create a copy of a column.

    """
    return column.copy(schema=schema)


def should_copy(column):
    """
    Determine if a column should be copied.

    """
    if not isinstance(column.type, Serial):
        return True

    if column.nullable:
        return True

    if not column.server_default:
        return True

    # do not create temporary serial values; they will be defaulted on upsert/insert
    return False


def copy_table(from_table, name):
    """
    Copy a table.

    Based on `Table.tometadata`, but simplified to remove constraints and indexes.

    """
    metadata = from_table.metadata

    if name in metadata.tables:
        return metadata.tables[name]

    schema = metadata.schema

    columns = [
        copy_column(column, schema)
        for column in from_table.columns
        if should_copy(column)
    ]

    return Table(
        name,
        metadata,
        schema=schema,
        comment=from_table.comment,
        *columns,
        **from_table.kwargs,
    )
