"""
Implement application-layer encryption using the aws-encryption-sdk.

"""
from typing import Mapping, Sequence, Tuple, Union

from aws_encryption_sdk import decrypt, encrypt
from aws_encryption_sdk.materials_managers.base import CryptoMaterialsManager


class SingleTenantEncryptor:
    """
    A single tenant encryptor.

    """
    def __init__(self, materials_manager: CryptoMaterialsManager):
        self.materials_manager = materials_manager

    def __contains__(self, encryption_context_key: str) -> bool:
        return True

    def encrypt(self,
                encryption_context_key: str,
                plaintext: str) -> Tuple[bytes, Sequence[str]]:
        """
        Encrypt a plaintext string value.

        The return value will include *both* the resulting binary ciphertext and the
        master key ids used for encryption. In the likely case that the encryptor was initialized
        with master key aliases, these master key ids returned will represent the unaliased key.

        """
        encryption_context = dict(
            microcosm=encryption_context_key,
        )

        cyphertext, header = encrypt(
            source=plaintext,
            materials_manager=self.materials_manager,
            encryption_context=encryption_context,
        )

        key_ids = [
            self.unpack_key_id(encrypted_data_key.key_provider)
            for encrypted_data_key in header.encrypted_data_keys
        ]
        return cyphertext, key_ids

    def decrypt(self, encryption_context_key: str, ciphertext: bytes) -> str:
        plaintext, header = decrypt(
            source=ciphertext,
            materials_manager=self.materials_manager,
        )
        return plaintext.decode("utf-8")

    def unpack_key_id(self, key_provider):
        key_info = key_provider.key_info
        try:
            # KMS case: the wrapped key id *is* the key id
            return key_info.decode("utf-8")
        except UnicodeDecodeError:
            # static case: the wrapped key id is the key id followed by two four byte integers (tags)
            # followed by a twelve byte initialization vectors (IV)
            #
            # see: aws_encryption_sdk.internal.formatting.serialize:serialize_wrapped_key
            return key_info[:-(4 + 4 + 12)].decode("utf-8")


class MultiTenantEncryptor:

    def __init__(self,
                 encryptors: Mapping[str, SingleTenantEncryptor],
                 default_key="default"):
        self.encryptors = encryptors
        self.default_key = default_key

    def __contains__(self, encryption_context_key: str) -> bool:
        return encryption_context_key in self.encryptors or self.default_key in self.encryptors

    def __getitem__(self, encryption_context_key: str) -> SingleTenantEncryptor:
        try:
            return self.encryptors[encryption_context_key]
        except KeyError:
            return self.encryptors[self.default_key]

    def encrypt(self, encryption_context_key: str, plaintext: str) -> Tuple[bytes, Sequence[str]]:
        encryptor = self[encryption_context_key]
        return encryptor.encrypt(encryption_context_key, plaintext)

    def decrypt(self, encryption_context_key: str, ciphertext: bytes) -> str:
        encryptor = self[encryption_context_key]
        return encryptor.decrypt(encryption_context_key, ciphertext)


Encryptor = Union[
    SingleTenantEncryptor,
    MultiTenantEncryptor,
]
