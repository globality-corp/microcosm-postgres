from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    TypeVar,
    overload,
)

from sqlalchemy import Column, LargeBinary
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped

from .encoders import Encoder
from .encryptors import Encryptor


T = TypeVar("T")


NOT_SET = object()


class encryption(hybrid_property, Generic[T]):
    @overload
    def __init__(
        self,
        key: str,
        encryptor: Encryptor,
        encoder: Encoder[T],
        *,
        column_type: Any = NOT_SET,
    ):
        ...

    @overload
    def __init__(
        self,
        key: str,
        encryptor: Encryptor,
        encoder: Encoder[T],
        *,
        default: T | Callable[[], T],
        column_type: Any = NOT_SET,
    ):
        ...

    def __init__(
        self,
        key: str,
        encryptor: Encryptor,
        encoder: Encoder[T],
        *,
        column_type: Any = NOT_SET,
        default: Any = NOT_SET,
    ):
        self.default = default
        self.key = key
        self.encryptor = encryptor
        self.encoder = encoder
        self.column_type = encoder.sa_type if column_type is NOT_SET else column_type

        encrypted_field = f"{key}_encrypted"
        unencrypted_field = f"{key}_unencrypted"

        def _prop(self):
            encrypted = getattr(self, encrypted_field)

            if encrypted is None:
                return getattr(self, unencrypted_field)

            return encoder.decode(encryptor.decrypt(encrypted))

        def _prop_setter(self, value) -> None:
            encrypted = encryptor.encrypt(encoder.encode(value))
            if encrypted is None:
                setattr(self, unencrypted_field, value)
                setattr(self, encrypted_field, None)
                return

            setattr(self, encrypted_field, encrypted)
            setattr(self, unencrypted_field, None)

        def _prop_expression(cls):
            return getattr(cls, unencrypted_field)

        super().__init__(_prop, _prop_setter, expr=_prop_expression)

    if TYPE_CHECKING:

        def __get__(self, instance: Any, owner: Any) -> T:
            ...

        def __set__(self, instance: Any, value: T) -> None:
            ...

    def encrypted(self) -> Mapped:
        if self.default is NOT_SET:
            return Column(self.key + "_encrypted", LargeBinary, nullable=True)

        return Column(
            self.key + "_encrypted",
            LargeBinary,
            nullable=True,
            default=(
                lambda: (
                    self.encryptor.encrypt(
                        self.encoder.encode(
                            self.default() if callable(self.default) else self.default
                        )
                    )
                    if self.encryptor.should_encrypt()
                    else None
                )
            ),
        )

    def unencrypted(self, **kwargs: Any) -> Mapped:
        if self.default is NOT_SET:
            return Column(self.key, self.column_type, nullable=True, **kwargs)

        return Column(
            self.key,
            self.column_type,
            nullable=True,
            default=lambda: (
                None
                if self.encryptor.should_encrypt()
                else (self.default() if callable(self.default) else self.default)
            ),
            **kwargs,
        )
