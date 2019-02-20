from typing import Sequence, Union

from aws_encryption_sdk.key_providers.kms import KMSMasterKeyProvider
from microcosm.api import defaults
from microcosm.config.validation import typed

from microcosm_postgres.encryption.encryptor import MultiTenantEncryptor, SingleTenantEncryptor
from microcosm_postgres.encryption.models import EncryptableMixin
from microcosm_postgres.encryption.providers import StaticMasterKeyProvider


def configure_key_provider(graph, key_ids):
    """
    Configure a key provider.

    During unit tests, use a static key provider (e.g. without AWS calls).

    """
    if graph.metadata.testing:
        # use static provider
        provider = StaticMasterKeyProvider()
        provider.add_master_keys_from_list(key_ids)
        return provider

    # use AWS provider
    return KMSMasterKeyProvider(key_ids=key_ids)


def strings(value: Union[Sequence[str], str]) -> Sequence[str]:
    return value.split(",") if isinstance(value, str) else value


@defaults(
    default=typed(strings),
)
def configure_encryptor(graph):
    """
    Configure a multi-tenant encryptor.

    The configuration structure is a mapping from encryption context key to list of strings:

        {
          "foo": ["bar", "baz"]
        }

    The string list may be specified as either a Python type or a comma-delimited string (e.g. to
    pass as an enviroment variable). The special key `default` will be used as a fallback if it is
    configured. That is, if the configuration contains a default, the resulting key ids will
    be used *unconditionally* if no other encryption context keys match.

    """
    encryptors = {
        key: SingleTenantEncryptor(
            key_provider=configure_key_provider(graph, strings(value))
        )
        for key, value in graph.config.multi_tenant_encryptor.items()
        if value
    }
    encryptor = MultiTenantEncryptor(encryptors=encryptors)

    # register the encryptor will each encryptable type
    for encryptable in EncryptableMixin.__subclasses__():
        encryptable.register(encryptor)

    return encryptor
