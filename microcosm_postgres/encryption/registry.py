"""
A registry for context keys and their master key ids.

"""
from typing import Mapping, Sequence

from microcosm.api import defaults
from microcosm.config.types import comma_separated_list
from microcosm.config.validation import typed
from microcosm_logging.decorators import logger

from microcosm_postgres.encryption.encryptor import MultiTenantEncryptor, SingleTenantEncryptor
from microcosm_postgres.encryption.providers import configure_key_provider, configure_materials_manager


def parse_config(context_keys: Sequence[str],
                 key_ids: Sequence[str]) -> Mapping[str, Sequence[str]]:
    return {
        # NB: split key id on non-comma to avoid confusion with config parsing
        context_key: key_id.split(";") if isinstance(key_id, str) else key_id
        for context_key, key_id in zip(context_keys, key_ids)
    }


@defaults(
    context_keys=typed(comma_separated_list, default_value=""),
    key_ids=typed(comma_separated_list, default_value=""),
)
@logger
class MultiTenantKeyRegistry:
    """
    Registry for encryption context keys and their associated master key id(s).

    """
    def __init__(self, graph):
        self.keys = parse_config(
            context_keys=graph.config.multi_tenant_key_registry.context_keys,
            key_ids=graph.config.multi_tenant_key_registry.key_ids,
        )

        for context_key, key_ids in self.keys.items():
            self.logger.info(
                "Encryption enabled for: {context_key}",
                extra=dict(
                    context_key=context_key,
                    key_ids=key_ids,
                ),
            )

    def make_encryptor(self, graph) -> MultiTenantEncryptor:
        return MultiTenantEncryptor(
            encryptors={
                context_key: SingleTenantEncryptor(
                    materials_manager=configure_materials_manager(
                        graph,
                        key_provider=configure_key_provider(graph, key_ids),
                    )
                )
                for context_key, key_ids in self.keys.items()
            },
        )
