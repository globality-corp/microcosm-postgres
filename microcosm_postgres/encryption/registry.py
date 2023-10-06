"""
A registry for context keys and their master key ids.

"""
from typing import Mapping, Sequence, Union

from microcosm.api import defaults
from microcosm.config.types import comma_separated_list
from microcosm.config.validation import typed
from microcosm_logging.decorators import logger

from microcosm_postgres.encryption.constants import ENCRYPTION_V2_DEFAULT_KEY
from microcosm_postgres.encryption.encryptor import MultiTenantEncryptor, SingleTenantEncryptor
from microcosm_postgres.encryption.providers import (
    configure_decrypting_key_provider,
    configure_encrypting_key_provider,
    configure_materials_manager,
)


def parse_config(
    context_keys: Sequence[str],
    key_ids: Sequence[Union[str, Sequence[str]]],
    account_ids: Sequence[Union[str, Sequence[str]]],
    partitions: Sequence[Union[str, Sequence[str]]],
    restricted_kms_policy: Sequence[str],
    beacon_keys: Sequence[str] | None = None,
) -> Mapping[str, Mapping[str, Union[str, Sequence[str], bool, None]]]:
    _beacon_keys: Sequence[str] = [] if beacon_keys is None else beacon_keys
    config = {}

    for ix, context_config in enumerate(zip(context_keys, key_ids, account_ids, partitions)):
        context_key, key_id, account_id, partition = context_config
        config[context_key] = {
            # NB: split key id on non-comma to avoid confusion with config parsing
            "key_ids": key_id.split(";") if isinstance(key_id, str) else key_id,
            "account_ids": account_id.split(";") if isinstance(account_id, str) else account_id,
            "partition": partition,
            "beacon_key": _beacon_keys[ix] if ix < len(_beacon_keys) and _beacon_keys[ix] else None,
            "restricted": restricted_kms_policy[ix] == "true" if ix < len(restricted_kms_policy) else False,
        }

    return config


@defaults(
    context_keys=typed(comma_separated_list, default_value=""),
    key_ids=typed(comma_separated_list, default_value=""),
    partitions=typed(comma_separated_list, default_value=""),
    account_ids=typed(comma_separated_list, default_value=""),
    beacon_keys=typed(comma_separated_list, default_value=""),
    restricted_kms_policy=typed(comma_separated_list, default_value=""),
)
@logger
class MultiTenantKeyRegistry:
    """
    Registry for encryption context keys and their associated master key id(s).

    """
    def __init__(self, graph):
        self.keys = parse_config(
            account_ids=graph.config.multi_tenant_key_registry.account_ids,
            context_keys=graph.config.multi_tenant_key_registry.context_keys,
            key_ids=graph.config.multi_tenant_key_registry.key_ids,
            partitions=graph.config.multi_tenant_key_registry.partitions,
            beacon_keys=graph.config.multi_tenant_key_registry.beacon_keys,
            restricted_kms_policy=graph.config.multi_tenant_key_registry.restricted_kms_policy,
        )

        for context_key, key_ids in self.keys.items():
            self.logger.info(
                "Encryption enabled for: {context_key}",
                extra=dict(
                    context_key=context_key,
                    key_ids=key_ids,
                ),
            )

        # Accumulate all account_ids and key_ids
        all_account_ids = set()
        all_key_ids = set()
        all_beacon_keys = []
        for context_data in self.keys.values():
            all_account_ids.update(context_data["account_ids"])
            all_key_ids.update(context_data["key_ids"])
            if context_data["beacon_key"]:
                all_beacon_keys.append(context_data["beacon_key"])

        self.all_account_ids = list(all_account_ids)
        self.all_key_ids = list(all_key_ids)
        self.all_beacon_keys = all_beacon_keys

    def make_encryptor(self, graph) -> MultiTenantEncryptor:
        encryptors = {
            context_key: SingleTenantEncryptor(
                encrypting_materials_manager=configure_materials_manager(
                    graph,
                    key_provider=configure_encrypting_key_provider(
                        graph,
                        key_ids=context_data["key_ids"],
                        restricted=context_data["restricted"],
                    ),
                ),
                decrypting_materials_manager=configure_materials_manager(
                    graph,
                    key_provider=configure_decrypting_key_provider(
                        graph,
                        context_data["account_ids"],
                        context_data["partition"],
                        context_data["key_ids"],
                    ),
                ),
                beacon_key=context_data["beacon_key"],
            )
            for context_key, context_data in self.keys.items()
        }

        if len(self.all_account_ids) > 0 and len(self.all_key_ids) > 0:
            # We'll only create a default encryptor if we have at least one
            # account_id and key_id
            if len(self.all_beacon_keys) > 0:
                # For now we'll take the first beacon key
                # in future, we'll need to be able to use multiple beacons
                # within the default encryptor
                beacon_key = self.all_beacon_keys[0]
            else:
                beacon_key = "test-key"
            encryptors[ENCRYPTION_V2_DEFAULT_KEY] = SingleTenantEncryptor(
                encrypting_materials_manager=None,
                decrypting_materials_manager=configure_materials_manager(
                    graph,
                    key_provider=configure_decrypting_key_provider(
                        graph,
                        self.all_account_ids,  # Use all accumulated account_ids
                        "aws",  # Assuming the partition is always "aws"
                        self.all_key_ids,  # Use all accumulated key_ids
                    ),
                ),
                beacon_key=beacon_key,
            )

        return MultiTenantEncryptor(
            encryptors=encryptors,
        )
