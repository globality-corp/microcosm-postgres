from contextlib import contextmanager
from contextvars import ContextVar
from typing import ContextManager, Iterator, Protocol
from uuid import UUID

from microcosm.api import binding

from microcosm_postgres.encryption.encryptor import MultiTenantEncryptor


class Encryptor(Protocol):
    def encrypt(self, client_id: UUID, value: str) -> bytes:
        ...

    def decrypt(self, client_id: UUID, value: bytes) -> str:
        ...

    def should_encrypt(self, client_id: UUID) -> bool:
        ...


class PlainTextEncryptor(Encryptor):
    def encrypt(self, client_id: UUID, value: str) -> bytes:
        return value.encode()

    def decrypt(self, client_id: UUID, value: bytes) -> str:
        return value.decode()

    def should_encrypt(self, client_id: UUID) -> bool:
        return False


class AwsKmsEncryptor(Encryptor):
    _encryptor: ContextVar[MultiTenantEncryptor] = ContextVar("_encryptor")

    class NotBound(Exception):
        ...

    @property
    def encryptor(self) -> MultiTenantEncryptor:
        try:
            return self._encryptor.get()
        except LookupError:
            raise AwsKmsEncryptor.NotBound("No encryptor bound to context")

    @binding("configure_aws_kms_encryptor")
    def configure_aws_kms_encryptor(cls, graph) -> None:
        cls.set_encryptor(graph.multi_tenant_encryptor)

    @classmethod
    def set_encryptor(cls, encryptor: MultiTenantEncryptor) -> ContextManager[None]:
        """
        Hook to set the encryptor for the current context.

        Usage:
            Either, set the context at the start of the request and forget:
            ```python
                AwsKmsEncryptor.set_encryptor(encryptor)
            ```

            Or, set the context in a scope to ensure reset:
            ```python
                with AwsKmsEncryptor.set_encryptor(encryptor):
                    # use it
                    ...
            ```
        """
        token = cls._encryptor.set(encryptor)

        @contextmanager
        def _token_wrapper() -> Iterator[None]:
            try:
                yield
            finally:
                cls._encryptor.reset(token)

        return _token_wrapper()

    def encrypt(self, client_id: UUID, value: str) -> bytes:
        return self.encryptor.encrypt(str(client_id), value)[0]

    def decrypt(self, client_id: UUID, value: bytes) -> str:
        return self.encryptor.decrypt(str(client_id), value)

    def should_encrypt(self, client_id: UUID) -> bool:
        return str(client_id) in self.encryptor
