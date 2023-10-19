from dataclasses import dataclass
from typing import Any, Sequence

from microcosm_postgres.encryption.v2.column import encryption
from microcosm_postgres.models import Model
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Session


@dataclass
class ModelWithEncryption:
    # declarative_type() uses metaclasses, so not sure if there's a better type here.
    model: type
    encryption_type: type = encryption

    def encryption_columns(self) -> list[str]:
        inspected_model = inspect(self.model)
        if inspected_model is None:
            return []

        return [
            col.name  # type: ignore
            for col in inspected_model.all_orm_descriptors
            if isinstance(col, self.encryption_type)
        ]

    def possible_encryption_columns(self) -> list[str]:
        """
        TODO: We may be able to offer something here to try detect when we go off the rails.

        Eg. looking for cols which contain the name `_encrypted` in the name.
        Would need to then exclude the actual encryption_columns from those found.

        Unclear what a nice UX for providing warning here would be.
        """
        return []


@dataclass
class ModelWithEncryptionSearch(ModelWithEncryption):
    search_kwargs: None | dict = None


def reencrypt_instance(session: Session, instance: Any, encryption_columns: list[str]) -> None:
    """
    Update the instance in a way in which the ORM is leveraged, so that writes leveraging
    encryption are used.
    """
    for column_name in encryption_columns:
        # Make the encryption attributes dirty by setting their values.
        # ie. instance.my_column = instance.my_column
        setattr(instance, column_name, getattr(instance, column_name))
        session.merge(instance)
        session.commit()


def find_models_using_encryption(base_model=Model, encryption_type=encryption):
    """Given a base model as a reference for finding all tables, find all tables + columns
    that appear to use encryption.

    Uses the microcosm-postgres base model, and looks for v2 encryption approach by default.
    """
    models = base_model.__subclasses__()

    encryption_models = []
    for model in models:
        m = ModelWithEncryption(model)
        if m.encryption_columns():
            encryption_models.append(m)

    return encryption_models


def print_reencryption_usage_info(models_with_encryption: list[ModelWithEncryption]) -> None:
    if not models_with_encryption:
        print("No models found using encryption.")
        return

    print(f"Found {len(models_with_encryption)} table(s) with encryption usage:")
    for model_with_encryption in models_with_encryption:
        cols_used = ", ".join([col for col in model_with_encryption.encryption_columns()])
        print(f"{model_with_encryption.model.__name__}: {cols_used}")


def verify_client_has_some_encryption_config(graph, client_id):
    if str(client_id) not in graph.multi_tenant_encryptor.encryptors:
        raise ValueError("Client does not appear to have any encryption config, cannot run re-encryption.")


def verify_planning_to_handle_all_tables(models_to_encrypt: Sequence[ModelWithEncryption]) -> None:
    model_with_encryption = find_models_using_encryption()
    expected_models = set(m.model.__name__ for m in model_with_encryption)
    actual_models = set(m.model.__name__ for m in models_to_encrypt)

    diff = expected_models.difference(actual_models)
    if diff:
        raise ValueError(f"Looks like we might be missing a table(s) using encryption: {', '.join(diff)}")