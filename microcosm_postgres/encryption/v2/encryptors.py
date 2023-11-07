from __future__ import annotations

from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from typing import (
    Any,
    ContextManager,
    Iterator,
    Literal,
    Protocol,
    TypeAlias,
    overload,
)

from aws_encryption_sdk.exceptions import DecryptKeyError
from microcosm.object_graph import ObjectGraph

from microcosm_postgres.constants import X_REQUEST_CLIENT_HEADER
from microcosm_postgres.encryption.constants import ENCRYPTION_V2_DEFAULT_KEY
from microcosm_postgres.encryption.encryptor import MultiTenantEncryptor, SingleTenantEncryptor
from microcosm_postgres.encryption.v2.beacons import BeaconHashAlgorithm
from microcosm_postgres.encryption.v2.errors import DecryptionError


class Encryptor(Protocol):
    def should_encrypt(self) -> bool:
        ...

    def encrypt(self, value: str) -> bytes | None:
        """Encrypt a value.

        Return None if the value should not be encrypted.
        """
        ...

    def decrypt(self, value: bytes) -> str | None:
        """Decrypt a value key identified from the ciphertext."""
        ...

    @overload
    def beacon(
        self,
        value: str,
        use_array: Literal[False],
        algorithm: BeaconHashAlgorithm | None = None,
    ) -> str:
        ...

    @overload
    def beacon(
        self,
        value: list[str],
        use_array: Literal[True],
        algorithm: BeaconHashAlgorithm | None = None,
    ) -> list[str]:
        ...

    def beacon(
        self,
        value: str | list[str],
        use_array: bool = False,
        algorithm: BeaconHashAlgorithm | None = None,
    ) -> list[str] | str:
        """Hash value using the beacon key."""
        ...


class PlainTextEncryptor(Encryptor, Protocol):
    def should_encrypt(self) -> bool:
        return False

    def encrypt(self, value: str) -> bytes | None:
        return None

    def decrypt(self, value: bytes) -> str | None:
        return value.decode()


EncryptorContext: TypeAlias = "tuple[str, SingleTenantEncryptor] | None"


class AwsKmsEncryptor(Encryptor):
    _encryptor_context: ContextVar[EncryptorContext] = ContextVar("_encryptor_context")

    class EncryptorNotBound(Exception):
        status_code = 403

    class BeaconKeyNotSet(Exception):
        status_code = 403

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

    @classmethod
    def set_context_from_graph(cls, graph: ObjectGraph) -> ContextManager[None]:
        encryptors: MultiTenantEncryptor = graph.multi_tenant_encryptor

        def normalise(opaque: dict[str, Any]) -> dict[str, Any]:
            return {k.lower(): v for k, v in opaque.items()}

        client_id = normalise(graph.request_context()).get(X_REQUEST_CLIENT_HEADER)
        if client_id is None or client_id not in encryptors.encryptors:
            # Then we return back the default encryptor
            default_encryptor = encryptors[ENCRYPTION_V2_DEFAULT_KEY]
            if default_encryptor is None:
                return nullcontext()
            return cls.set_encryptor_context(
                ENCRYPTION_V2_DEFAULT_KEY, default_encryptor
            )

        client_encryptor = encryptors[client_id]
        if client_encryptor is None:
            return nullcontext()
        return cls.set_encryptor_context(client_id, client_encryptor)

    @classmethod
    def register_flask_context(cls, graph: ObjectGraph) -> None:
        graph.use("multi_tenant_encryptor")

        @graph.flask.before_request
        def _register_encryptor():
            cls.set_context_from_graph(graph)

        @graph.flask.after_request
        def _reset_encryptor(response):
            cls._encryptor_context.set(None)
            return response

    def should_encrypt(self) -> bool:
        if self.encryptor_context is None:
            return False

        _, encryptor = self.encryptor_context
        return encryptor.encrypting_materials_manager is not None

    def encrypt(self, value: str) -> bytes | None:
        if not self.should_encrypt():
            return None

        assert self.encryptor_context is not None
        context, encryptor = self.encryptor_context

        # Note that the encryptor may return back None
        encrypted = encryptor.encrypt(context, value)
        if encrypted is None:
            return None
        else:
            return encrypted[0]

    def decrypt(self, value: bytes) -> str | None:
        if self.encryptor_context is None:
            raise self.EncryptorNotBound("Decryption context is not set")

        context, encryptor = self.encryptor_context
        try:
            return encryptor.decrypt(context, value)
        except DecryptKeyError as e:
            raise DecryptionError() from e

    @overload
    def beacon(
        self,
        value: str,
        use_array: Literal[False],
        algorithm: BeaconHashAlgorithm | None = None,
    ) -> str:
        ...

    @overload
    def beacon(
        self,
        value: list[str],
        use_array: Literal[True],
        algorithm: BeaconHashAlgorithm | None = None,
    ) -> list[str]:
        ...

    def beacon(
        self,
        value: str | list[str],
        use_array: bool = False,
        algorithm: BeaconHashAlgorithm | None = None,
    ) -> list[str] | str:
        if self.encryptor_context is None:
            raise self.EncryptorNotBound()

        _, encryptor = self.encryptor_context
        if use_array:
            assert isinstance(value, list)
            _beacon = [encryptor.beacon(v, algorithm=algorithm) for v in value]
            # Filter out the None values
            _beacon = [v for v in _beacon if v is not None]

        else:
            assert isinstance(value, str)
            _beacon = encryptor.beacon(value, algorithm=algorithm)  # type: ignore[assignment]

        if _beacon is None:
            raise self.BeaconKeyNotSet()

        return _beacon  # type: ignore[return-value]
