"""
Custom key providers

"""
from os import urandom

from aws_encryption_sdk.identifiers import EncryptionKeyType, WrappingAlgorithm
from aws_encryption_sdk.internal.crypto.wrapping_keys import WrappingKey
from aws_encryption_sdk.key_providers.raw import RawMasterKeyProvider


class StaticMasterKeyProvider(RawMasterKeyProvider):
    """
    A master key provider that uses static keys.

    Intended for unit testing.

    """
    @property
    def provider_id(self) -> str:
        return "static"

    def _get_raw_key(self, key_id) -> WrappingKey:
        return WrappingKey(
            wrapping_algorithm=WrappingAlgorithm.AES_256_GCM_IV12_TAG16_NO_PADDING,
            wrapping_key=urandom(32),
            wrapping_key_type=EncryptionKeyType.SYMMETRIC,
        )
