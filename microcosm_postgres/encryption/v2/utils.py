from copy import deepcopy
from typing import Any

from microcosm_postgres.models import Model


def members_override(
        members_dict: dict[str, Any],
        encrypted_field_names: list[str],
        for_insert: bool = False,
        using_encryption: bool = False,
) -> dict[str, Any]:
    """
    Override the base _members method to ensure no return of values for non-existing DB columns.
    Building the dict for direct insert in the database is slightly different.
    """

    # Filter out internal SQLAlchemy state and nested relationships
    base_dict = {
        key: value
        for key, value in members_dict.items()
        if not key.startswith("_") and not isinstance(value, Model)
    }

    if for_insert:
        return _members_override_for_insert(base_dict, encrypted_field_names, using_encryption)

    return _process_encryption_context(base_dict, encrypted_field_names)


def _process_encryption_context(base_dict: dict[str, Any], encrypted_field_names: list[str]) -> dict[str, Any]:
    """
    Process the encryption context for members dictionary.
    """
    for field in encrypted_field_names:
        beacon_key = f"{field}_beacon"
        unencrypted_key = f"{field}_unencrypted"

        if base_dict.get(beacon_key) is not None:
            base_dict[field] = base_dict.pop(beacon_key)
        else:
            base_dict[field] = base_dict.pop(unencrypted_key, None)

    return base_dict


def _members_override_for_insert(
        members_dict: dict[str, Any], encrypted_field_names: list[str], using_encryption: bool = False
) -> dict[str, Any]:
    """
    Modify the members dictionary for direct insertion into the database.
    Behavior varies with the use of encryption.
    """

    base_dict = deepcopy(members_dict)

    if using_encryption:
        for field in encrypted_field_names:
            unencrypted_key = f"{field}_unencrypted"
            base_dict[field] = base_dict.pop(unencrypted_key, None)
    else:
        base_dict = _process_encryption_context(base_dict, encrypted_field_names)

    return base_dict
