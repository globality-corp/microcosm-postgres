from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import ContextManager, Iterator, Protocol, TypeAlias

from microcosm_postgres.encryption.encryptor import SingleTenantEncryptor


class Encryptor(Protocol):
    def encrypt(self, value: str) -> bytes | None:
        """Encrypt a value.

        Return None if the value should not be encrypted.
        """
        ...

    def decrypt(self, value: bytes) -> str:
        """Decrypt a value key identified from the ciphertext."""
        ...


class PlainTextEncryptor(Encryptor):
    def encrypt(self, value: str) -> bytes | None:
        return None

    def decrypt(self, value: bytes) -> str:
        return value.decode()


EncryptorContext: TypeAlias = "tuple[str, SingleTenantEncryptor]"


class AwsKmsEncryptor(Encryptor):
    _encryptor_context: ContextVar[EncryptorContext] = ContextVar("_encryptor_context")

    class EncryptorNotBound(Exception):
        ...

    @property
    def encryptor_context(self) -> tuple[str, SingleTenantEncryptor] | None:
        return self._encryptor_context.get(None)

    @classmethod
    def set_encryptor_context(
        cls,
        context: str,
        encryptor: SingleTenantEncryptor,
    ) -> ContextManager[None]:
        """
        Hook to set the encryptor for the current context.

        Usage:
            Either, set the context at the start of the request and forget:
            ```python
                AwsKmsEncryptor.set_encryptor("context", encryptor)
            ```

            Or, set the context in a scope to ensure reset:
            ```python
                with AwsKmsEncryptor.set_encryptor("context", encryptor):
                    # use it
                    ...
            ```
        """
        token = cls._encryptor_context.set((context, encryptor))

        @contextmanager
        def _token_wrapper() -> Iterator[None]:
            try:
                yield
            finally:
                cls._encryptor_context.reset(token)

        return _token_wrapper()

    def encrypt(self, value: str) -> bytes | None:
        if self.encryptor_context is None:
            return None

        context, encryptor = self.encryptor_context
        return encryptor.encrypt(context, value)[0]

    def decrypt(self, value: bytes) -> str:
        if self.encryptor_context is None:
            raise self.EncryptorNotBound()

        context, encryptor = self.encryptor_context
        return encryptor.decrypt(context, value)
