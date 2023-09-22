from typing import Any

from microcosm_postgres.models import Model


def members_override(
    members_dict: dict[str, Any], encrypted_field_names: list[str]
) -> dict[str, Any]:
    """
    We override the base _members method to ensure that we don't return
    values for columns that don't exist in the database e.g *_unencrypted
    """
    base_dict = {
        key: value
        for key, value in members_dict.items()
        # NB: ignore internal SQLAlchemy state and nested relationships
        if not key.startswith("_") and not isinstance(value, Model)
    }

    for field in encrypted_field_names:
        try:
            base_dict[field] = base_dict[f"{field}_unencrypted"]
            del base_dict[f"{field}_unencrypted"]
        except KeyError:
            pass

    return base_dict
