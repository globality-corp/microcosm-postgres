"""
Custom key providers

"""
from os import urandom

from aws_encryption_sdk import (
    CachingCryptoMaterialsManager,
    DefaultCryptoMaterialsManager,
    LocalCryptoMaterialsCache,
)
from aws_encryption_sdk.identifiers import EncryptionKeyType, WrappingAlgorithm
from aws_encryption_sdk.internal.crypto.wrapping_keys import WrappingKey
from aws_encryption_sdk.key_providers.kms import (
    DiscoveryAwsKmsMasterKeyProvider,
    DiscoveryFilter,
    StrictAwsKmsMasterKeyProvider,
)
from aws_encryption_sdk.key_providers.raw import RawMasterKeyProvider
from microcosm.api import defaults
from microcosm.config.types import boolean
from microcosm.config.validation import typed


class StaticMasterKeyProvider(RawMasterKeyProvider):
    """
    A master key provider that uses static keys.

    Intended for unit testing.

    """
    # Declared statically to allow for multiple key_providers to be used
    # in the same unit test
    wrapping_key = urandom(32)

    @property
    def provider_id(self) -> str:
        return "static"

    def _get_raw_key(self, key_id) -> WrappingKey:
        return WrappingKey(
            wrapping_algorithm=WrappingAlgorithm.AES_256_GCM_IV12_TAG16_NO_PADDING,
            wrapping_key=self.wrapping_key,
            wrapping_key_type=EncryptionKeyType.SYMMETRIC,
        )


def configure_encrypting_key_provider(graph, key_ids):
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
    return StrictAwsKmsMasterKeyProvider(key_ids=key_ids)


def configure_decrypting_key_provider(graph, account_ids, partition, key_ids):
    """
    Configure a key provider.

    During unit tests, use a static key provider (e.g. without AWS calls).

    """
    if graph.metadata.testing:
        # use static provider
        provider = StaticMasterKeyProvider()
        provider.add_master_keys_from_list(key_ids)
        return provider

    discovery_filter = DiscoveryFilter(
        account_ids=account_ids,
        partition=partition,
    )

    # use AWS provider
    return DiscoveryAwsKmsMasterKeyProvider(discovery_filter=discovery_filter)


@defaults(
    enable_cache=typed(boolean, default_value=False),
    cache_capacity=typed(int, default_value=100),
    cache_max_age=typed(float, default_value=3600.0),
    cache_max_messages_encrypted=typed(int, default_value=1000),
)
def configure_materials_manager(graph, key_provider):
    """
    Configure a crypto materials manager

    """
    if graph.config.materials_manager.enable_cache:
        return CachingCryptoMaterialsManager(
            cache=LocalCryptoMaterialsCache(graph.config.materials_manager.cache_capacity),
            master_key_provider=key_provider,
            max_age=graph.config.materials_manager.cache_max_age,
            max_messages_encrypted=graph.config.materials_manager.cache_max_messages_encrypted,
        )
    return DefaultCryptoMaterialsManager(master_key_provider=key_provider)
