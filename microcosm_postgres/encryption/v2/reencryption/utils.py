from collections.abc import Iterator
from contextlib import contextmanager
from time import time
from typing import Any

from sqlalchemy.orm import Session


def reencrypt_instance(
    session: Session,
    instance: Any,
    encryption_columns: list[str],
    dry_run: bool = False,
    only_unencrypted: bool = False,
) -> tuple[bool, bool, list[str]]:
    """
    Update the instance so that encryption is (re-)applied.

    For each column in `encryption_columns`, this function will check if the column
    is unencrypted by either:
      1. The original check (i.e. the '_unencrypted' attribute is not None), or
      2. The new check (i.e. the '_encrypted' attribute is not None and the
         corresponding '_beacon' attribute is None).

    Args:
        session: The current SQLAlchemy session.
        instance: The instance to update.
        encryption_columns: List of column names to reencrypt.
        dry_run: If True, no actual updates/commits are performed.
        only_unencrypted: If True, only reencrypt columns that are detected as unencrypted.

    Returns:
        A tuple of:
         - found_to_be_unencrypted (bool): True if any column is unencrypted.
         - change_committed (bool): True if any change was committed.
         - unencrypted_fields (list[str]): List of column names found unencrypted.
    """
    found_to_be_unencrypted = False
    change_committed = False
    unencrypted_fields = []

    for column_name in encryption_columns:
        # Retrieve the relevant attributes for the column.
        unencrypted_attr = getattr(instance, f"{column_name}_unencrypted")
        encrypted_val = getattr(instance, f"{column_name}_encrypted")
        beacon_val = getattr(instance, f"{column_name}_beacon")

        # Determine if the column is unencrypted by checking:
        #   (original condition) OR (new condition)
        condition1 = unencrypted_attr is not None
        condition2 = (encrypted_val is not None) and (beacon_val is None)
        is_unencrypted = condition1 or condition2

        if is_unencrypted:
            found_to_be_unencrypted = True
            unencrypted_fields.append(column_name)

        if not dry_run:
            # If only_unencrypted is True, then only process this column if it's unencrypted.
            if only_unencrypted and not is_unencrypted:
                continue

            # "Touch" the field to trigger the encryption mechanism.
            setattr(instance, column_name, getattr(instance, column_name))
            session.merge(instance)
            session.commit()
            change_committed = True

    return found_to_be_unencrypted, change_committed, unencrypted_fields


@contextmanager
def elapsed_time(target: dict[str, Any], milliseconds: bool = True) -> Iterator[float]:
    """
    Returns back time in milliseconds / seconds given the `milliseconds` flag passed in

    """
    start_time = time()
    try:
        yield start_time
    finally:
        elapsed_ms = time() - start_time
        if milliseconds:
            elapsed_ms *= 1000

        target["elapsed_time"] = elapsed_ms
