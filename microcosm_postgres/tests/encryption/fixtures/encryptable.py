from typing import Sequence, Tuple

from microcosm.api import binding
from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKey,
    String,
)
from sqlalchemy.orm import relationship
from sqlalchemy_utils import UUIDType

from microcosm_postgres.encryption.models import EncryptableMixin, EncryptedMixin
from microcosm_postgres.encryption.store import EncryptableStore
from microcosm_postgres.models import EntityMixin, Model
from microcosm_postgres.store import Store


class Encrypted(EntityMixin, EncryptedMixin, Model):
    __tablename__ = "encrypted"


class Encryptable(EntityMixin, EncryptableMixin, Model):
    """
    A model for conditionally-encrypted plaintext.

    """
    __tablename__ = "encryptable"

    # key used for encryption context
    key = Column(String, nullable=False)
    # value is not encrypted
    value = Column(String, nullable=True)
    # foreign key to encrypted data
    encrypted_id = Column(UUIDType, ForeignKey("encrypted.id"), nullable=True)
    # load and update encrypted relationship automatically
    encrypted = relationship(
        Encrypted,
        lazy="joined",
    )

    __table_args__ = (
        CheckConstraint(
            name="value_or_encrypted_is_not_null",
            sqltext="value IS NOT NULL OR encrypted_id IS NOT NULL",
        ),
        CheckConstraint(
            name="value_or_encrypted_is_null",
            sqltext="value IS NULL OR encrypted_id IS NULL",
        ),
    )

    @property
    def ciphertext(self) -> Tuple[bytes, Sequence[str]]:
        return (self.encrypted.ciphertext, self.encrypted.key_ids)

    @ciphertext.setter
    def ciphertext(self, value: Tuple[bytes, Sequence[str]]) -> None:
        ciphertext, key_ids = value
        self.encrypted = Encrypted(
            ciphertext=ciphertext,
            key_ids=key_ids,
        )


@binding("encrypted_store")
class EncryptedStore(Store):

    def __init__(self, graph):
        super().__init__(graph, Encrypted)


@binding("encryptable_store")
class EncryptableModelStore(EncryptableStore):

    def __init__(self, graph):
        super().__init__(graph, Encryptable, graph.encrypted_store)
