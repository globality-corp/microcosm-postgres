from typing import TypeVar

from sqlalchemy import ColumnElement
from sqlalchemy.ext.hybrid import hybrid_property

from .encoders import Encoder
from .encryptors import Encryptor


T = TypeVar("T")


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
            return

        setattr(self, encrypted_field, encrypted)

    @_prop.inplace.expression
    def _prop_expression(cls) -> ColumnElement[T]:
        return getattr(cls, unencrypted_field)

    return _prop
