from json import dumps, loads
from typing import Sequence, Tuple

from microcosm.api import binding
from sqlalchemy import CheckConstraint, Column, ForeignKey, String
from sqlalchemy.orm import relationship
from sqlalchemy_utils import JSONType, UUIDType

from microcosm_postgres.models import EntityMixin, Model
from microcosm_postgres.store import Store
from microcosm_postgres.encryption.models import EncryptableMixin, EncryptedMixin


class JsonEncrypted(EntityMixin, EncryptedMixin, Model):
    __tablename__ = "json_encrypted"


class JsonEncryptable(EntityMixin, EncryptableMixin, Model):
    """
    A model for conditionally-encrypted plaintext.

    """
    __tablename__ = "json_encryptable"

    # key used for encryption context
    key = Column(String, nullable=False)
    # value is not encrypted
    value = Column(JSONType, nullable=False)
    # foreign key to encrypted data
    json_encrypted_id = Column(UUIDType, ForeignKey("json_encrypted.id"), nullable=True)
    # load encrypted relationship automatically
    encrypted = relationship(JsonEncrypted, lazy="joined")

    __table_args__ = (
        CheckConstraint(
            name="value_or_json_encrypted_is_not_null",
            sqltext="value::text != 'null' OR json_encrypted_id IS NOT NULL",
        ),
        CheckConstraint(
            name="value_or_json_encrypted_is_null",
            sqltext="value::text = 'null' OR json_encrypted_id IS NULL",
        ),
    )

    @property
    def ciphertext(self) -> Tuple[bytes, Sequence[str]]:
        return (self.encrypted.ciphertext, self.encrypted.key_ids)

    @ciphertext.setter
    def ciphertext(self, value: Tuple[bytes, Sequence[str]]) -> None:
        ciphertext, key_ids = value
        self.encrypted = JsonEncrypted(
            ciphertext=ciphertext,
            key_ids=key_ids,
        )

    @classmethod
    def plaintext_to_str(cls, plaintext_object) -> str:
        return dumps(plaintext_object)

    @classmethod
    def str_to_plaintext(cls, text: str) -> object:
        return loads(text)


@binding("json_encrypted_store")
class JsonEncryptedStore(Store):

    def __init__(self, graph):
        super().__init__(graph, JsonEncrypted)


@binding("json_encryptable_store")
class JsonEncryptableStore(Store):

    def __init__(self, graph):
        super().__init__(graph, JsonEncryptable)
