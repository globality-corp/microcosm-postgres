from contextvars import ContextVar
from typing import TypeVar
from uuid import UUID

from sqlalchemy import ColumnElement
from sqlalchemy.ext.hybrid import hybrid_property

from .encoders import Encoder
from .encryptors import Encryptor


T = TypeVar("T")
CLIENT_ID: ContextVar[UUID | None] = ContextVar("CLIENT_ID", default=None)


def encryption(
    key: str,
    encryptor: Encryptor,
    encoder: Encoder[T],
    client_id_field: str = "client_id",
) -> hybrid_property[T]:
    """
    Switches between encrypted and plaintext values based on the client_id.

    Queries on the encryption field will only be performed on the unencrypted rows.
    """

    @hybrid_property
    def _prop(self) -> T:
        client_id: UUID | None = getattr(self, client_id_field)
        if client_id is not None and encryptor.should_encrypt(client_id):
            return encoder.decode(
                encryptor.decrypt(
                    client_id,
                    getattr(self, f"{key}_encrypted"),
                )
            )

        return getattr(self, f"{key}_unencrypted")

    @_prop.inplace.setter
    def _prop_setter(self, value: T) -> None:
        client_id: UUID | None = getattr(self, client_id_field)
        if client_id is None:
            setattr(self, client_id_field, client_id := CLIENT_ID.get())

        if client_id is not None and encryptor.should_encrypt(client_id):
            setattr(
                self,
                f"{key}_encrypted",
                encryptor.encrypt(client_id, encoder.encode(value)),
            )
            return

        setattr(self, f"{key}_unencrypted", value)

    @_prop.inplace.expression
    def _prop_expression(cls) -> ColumnElement[T]:
        return getattr(cls, f"{key}_unencrypted")

    return _prop
