from argparse import ArgumentParser
from typing import Any, Iterator, Protocol

from microcosm.object_graph import ObjectGraph
from sqlalchemy import inspect
from sqlalchemy.orm import Session

from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.encryption.v2.column import encryption as encryption_column
from microcosm_postgres.encryption.v2.contexts import encryptor_session_context_as_client
from microcosm_postgres.encryption.v2.reencryption.stats import (
    ReencryptionStatistic,
    ReencryptionStatsCollector,
)
from microcosm_postgres.encryption.v2.reencryption.utils import elapsed_time, reencrypt_instance
from microcosm_postgres.models import Model


class InstanceIterator(Protocol):
    def __call__(self, session: Session, client_id: str, graph: ObjectGraph, **kwargs) -> Iterator[Model]:
        ...


class ReencryptionCli:

    def __init__(
        self,
        instance_iterators:
        list[InstanceIterator],
        base_models_mapping: dict[type, list[type]],
        graph: ObjectGraph
    ):
        """
        base_models_mapping is a mapping of base model -> models that inherit from it
        We need the base model so we can perform validation to verify that we're handling
        all the encrypted models under that base model.

        """
        self.iterators = instance_iterators
        self.base_models_mapping = base_models_mapping
        self.graph = graph

        # Squash into single list of models
        self.all_models = [item for sublist in base_models_mapping.values() for item in sublist]

        self.parser = ArgumentParser(description="Reencryption CLI")
        self.subparsers = self.parser.add_subparsers()

        self._add_reencrypt_command(self.reencrypt)
        self._add_audit_command(self.audit)

    def __call__(self, *args, **kwargs):
        # Parse arguments
        args = self.parser.parse_args()

        # If no command is provided, display help
        if not hasattr(args, 'func'):
            self.parser.print_help()
            return

        # Call the function associated with the chosen subcommand
        args.func(args)

    def reencrypt(self, args: Any):
        client_id, dry_run = self._get_reencrypt_args(args)
        self._run_validations(client_id)

        elapsed_time_data: dict[str, Any] = dict()
        with (
            elapsed_time(elapsed_time_data),
            encryptor_session_context_as_client(self.graph, client_id=client_id),
            transaction(),
        ):
            session = SessionContext.session
            stats: list[ReencryptionStatistic] = []

            # We assume that we have one iterator per model type
            collector = ReencryptionStatsCollector()
            for instance_iterator in self.iterators:
                for instance in instance_iterator(session=session, client_id=client_id, graph=self.graph):
                    found_to_be_unencrypted, changed_committed = reencrypt_instance(
                        session=session,
                        instance=instance,
                        encryption_columns=self._get_encryption_columns(instance),
                        dry_run=dry_run,
                    )
                    model = self._find_model_for_instance(instance)
                    collector.update(found_to_be_unencrypted, changed_committed, model_name=model.__name__)

        stats = collector.get_stats()
        self._write_reenrypt_logs(elapsed_time_data, stats)

    def audit(self, args: Any):
        for base_model in self.base_models_mapping:
            models = self._find_models_using_encryption(base_model=base_model)
            self._log_reencryption_usage_info(models)

    def _find_model_for_instance(self, instance: Model) -> type:
        """
        Find the model class for the given instance.

        To speed this up we can pass in the previously used model_type?

        """
        for m in self.all_models:
            if isinstance(instance, m):
                return m

        raise ValueError(
            f"This shouldn't happen. Instance type not found in any of the models. instance: {instance}"
        )

    def _get_reencrypt_args(self, args: Any) -> tuple[str, bool]:
        """
        Client id is required for reencryption so we throw an exception
        if it's not provided.

        """
        client_id = args.client_id
        if client_id is None:
            raise RuntimeError("Client id is required: --client-id <client_id>")

        return client_id, not args.no_dry_run

    def _run_validations(self, client_id: str):
        # Validate that we have some encryption config
        self._verify_client_has_some_encryption_config(client_id)

        self._verify_handle_all_tables()

    def _verify_client_has_some_encryption_config(self, client_id: str):
        if str(client_id) not in self.graph.multi_tenant_encryptor.encryptors:
            raise ValueError("Client does not appear to have any encryption config, cannot run re-encryption.")

    def _verify_handle_all_tables(self):
        for base_model, models_with_encryption in self.base_models_mapping.items():
            self._verify_planning_to_handle_all_tables(base_model=base_model, models_to_encrypt=models_with_encryption)

    def _verify_planning_to_handle_all_tables(self, base_model: type, models_to_encrypt: list[type]):
        models_with_encryption = self._find_models_using_encryption(base_model)
        expected_models = set(m.__name__ for m in models_with_encryption)
        actual_models = set(m.__name__ for m in models_to_encrypt)

        diff = expected_models.difference(actual_models)
        if diff:
            raise ValueError(f"Looks like we might be missing a table(s) using encryption: {', '.join(diff)}")

    def _write_reenrypt_logs(self, elapsed_time_data: dict[str, Any], stats: list[ReencryptionStatistic]):
        print("Success!")  # noqa: T201
        print(f"Time taken to run: {elapsed_time_data['elapsed_time']}")  # noqa: T201

        # Log stats
        for stat in stats:
            stat.log_stats()

    def _get_encryption_columns(self, instance: Model | type):
        if isinstance(instance, type):
            model = instance
        else:
            model = instance.__class__

        try:
            inspected_model: Any = inspect(model)
        except Exception:
            print(f"Unable to inspect model: {model.__name__}")  # noqa: T201
            return []

        if inspected_model is None:
            return []

        return [
            col.name  # type: ignore
            for col in inspected_model.all_orm_descriptors
            if isinstance(col, encryption_column)
        ]

    def _add_reencrypt_command(self, fn):
        reencrypt_command_parser = self.subparsers.add_parser('reencrypt', help='Reencrypt some data')
        reencrypt_command_parser.add_argument("--client-id", help="The client id to reencrypt")

        # Adding the dry_run argument
        reencrypt_command_parser.add_argument(
            "--no-dry-run",
            action='store_true',
            default=False,
            help="Execute the command without making actual changes. Default is True."
        )

        # Adding testing argument
        reencrypt_command_parser.add_argument(
            "--testing",
            action='store_true',
            default=False,
            help="Execute the command without making actual changes. Default is True."
        )

        reencrypt_command_parser.set_defaults(func=fn)

    def _add_audit_command(self, fn):
        audit_command_parser = self.subparsers.add_parser('audit', help='Audit some data')
        audit_command_parser.set_defaults(func=fn)

    def _find_models_using_encryption(self, base_model: type = Model) -> list[type]:
        """Given a base model as a reference for finding all tables, find all tables + columns
        that appear to use encryption.

        Uses the microcosm-postgres base model, and looks for v2 encryption approach by default.
        """
        models = base_model.__subclasses__()

        encryption_models = []
        for model in models:
            cols = self._get_encryption_columns(model)
            if len(cols) > 0:
                encryption_models.append(model)

        return encryption_models

    def _log_reencryption_usage_info(self, models_with_encryption: list[type]) -> None:
        if not models_with_encryption:
            print("No models found using encryption.")  # noqa: T201
            return

        print(f"Found {len(models_with_encryption)} table(s) with encryption usage:")  # noqa: T201
        for m in models_with_encryption:
            cols_used = ", ".join([col for col in self._get_encryption_columns(m)])
            print(f"Model name: {m.__name__}, Cols used: {cols_used}")  # noqa: T201
