from typing import (
    Any,
    Generic,
    TypeVar,
    overload,
)

from sqlalchemy import ColumnElement, LargeBinary
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column

from .encoders import Encoder
from .encryptors import Encryptor


T = TypeVar("T")


NOT_SET = object()


class encryption_property(hybrid_property[T], Generic[T]):
    @overload
    def __init__(
        self,
        key: str,
        column_type: Any,
        encryptor: Encryptor,
        encoder: Encoder[T],
    ):
        ...

    @overload
    def __init__(
        self,
        key: str,
        column_type: Any,
        encryptor: Encryptor,
        encoder: Encoder[T],
        *,
        default: T,
    ):
        ...

    def __init__(
        self,
        key: str,
        column_type: Any,
        encryptor: Encryptor,
        encoder: Encoder[T],
        *,
        default: Any = NOT_SET,
    ):
        self.default = default
        self.key = key
        self.encryptor = encryptor
        self.encoder = encoder
        self.column_type = column_type

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

    def encrypted(self) -> Mapped[bytes | None]:
        if self.default is NOT_SET:
            return mapped_column(
                self.key + "_encrypted", LargeBinary, nullable=True
            )
        return mapped_column(
            self.key + "_encrypted",
            LargeBinary,
            nullable=True,
            default=lambda: self.encryptor.encrypt(self.encoder.encode(self.default)),
        )

    def unencrypted(self, **kwargs) -> Mapped[T | None]:
        if self.default is NOT_SET:
            return mapped_column(self.key, self.column_type, nullable=True, **kwargs)
        return mapped_column(
            self.key,
            self.column_type,
            nullable=True,
            default=self.default,
            **kwargs,
        )


def encryption(
    key: str, encryptor: Encryptor, encoder: Encoder[T]
) -> hybrid_property[T]:
    """
    Switches between encrypted and plaintext values based on the client_id.

    Queries on the encryption field will only be performed on the unencrypted rows.
    """
    encrypted_field = f"{key}_encrypted"
    unencrypted_field = f"{key}_unencrypted"

    @hybrid_property
    def _prop(self) -> T:
        encrypted = getattr(self, encrypted_field)

        if encrypted is None:
            return getattr(self, unencrypted_field)

        return encoder.decode(encryptor.decrypt(encrypted))

    @_prop.inplace.setter
    def _prop_setter(self, value: T) -> None:
        encrypted = encryptor.encrypt(encoder.encode(value))
        if encrypted is None:
            setattr(self, unencrypted_field, value)
            setattr(self, encrypted_field, None)
            return

        setattr(self, encrypted_field, encrypted)
        setattr(self, unencrypted_field, None)

    @_prop.inplace.expression
    def _prop_expression(cls) -> ColumnElement[T]:
        return getattr(cls, unencrypted_field)

    return _prop
