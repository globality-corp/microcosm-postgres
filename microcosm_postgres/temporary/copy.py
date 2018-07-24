"""
Copy a table.

"""
from sqlalchemy import Table


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
        column.copy(schema=schema)
        for column in from_table.columns
    ]

    return Table(
        name,
        metadata,
        schema=schema,
        comment=from_table.comment,
        *columns,
        **from_table.kwargs,
    )
