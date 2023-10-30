from contextlib import contextmanager
from time import time
from typing import Any, Iterator

from sqlalchemy.orm import Session


def reencrypt_instance(
    session: Session, instance: Any, encryption_columns: list[str], dry_run: bool = False
) -> tuple[bool, bool]:
    """
    Update the instance in a way in which the ORM is leveraged, so that writes leveraging
    encryption are used.

    We return back a tuple of (found_to_be_unencrypted, change_commited)
    - found_to_be_unencrypted: True if any of the columns of the instance are unencrypted
    - change_commited: True if the instance was changed and committed to the database
    """
    found_to_be_unencrypted = False
    change_commited = False
    for column_name in encryption_columns:
        # Check if the column is unencrypted
        if getattr(instance, f"{column_name}_unencrypted") is not None:
            found_to_be_unencrypted = True

        if not dry_run:
            # Make the encryption attributes dirty by setting their values.
            # ie. instance.my_column = instance.my_column
            setattr(instance, column_name, getattr(instance, column_name))
            session.merge(instance)
            session.commit()
            change_commited = True

    return found_to_be_unencrypted, change_commited


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
