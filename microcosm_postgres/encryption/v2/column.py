from sqlalchemy.ext.hybrid import hybrid_property

from .encoders import Encoder
from .encryptors import Encryptor


def encryption(key: str, encryptor: Encryptor, encoder: Encoder) -> hybrid_property:
    """
    Switches between encrypted and plaintext values based on the client_id.

    Queries on the encryption field will only be performed on the unencrypted rows.
    """
    encrypted_field = f"{key}_encrypted"
    unencrypted_field = f"{key}_unencrypted"

    @hybrid_property
    def _prop(self):
        encrypted = getattr(self, encrypted_field)

        if encrypted is None:
            return getattr(self, unencrypted_field)

        return encoder.decode(encryptor.decrypt(encrypted))

    @_prop.setter
    def _prop_setter(self, value) -> None:
        encrypted = encryptor.encrypt(encoder.encode(value))
        if encrypted is None:
            setattr(self, unencrypted_field, value)
            return

        setattr(self, encrypted_field, encrypted)

    @_prop_setter.expression
    def _prop_expression(cls):
        return getattr(cls, unencrypted_field)

    return _prop_setter
