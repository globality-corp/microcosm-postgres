from typing import Optional, Sequence, Tuple

from microcosm.api import binding
from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKey,
    String,
)
from sqlalchemy.orm import relationship

from microcosm_postgres.encryption.models import EncryptableMixin, EncryptedMixin
from microcosm_postgres.encryption.store import EncryptableStore
from microcosm_postgres.models import EntityMixin, Model
from microcosm_postgres.sqlalchemy_utils import UUIDType
from microcosm_postgres.store import Store


class NullableEncrypted(EntityMixin, EncryptedMixin, Model):
    __tablename__ = "nullable_encrypted"


class NullableEncryptable(EntityMixin, EncryptableMixin, Model):
    """
    A model for conditionally-encrypted plaintext.

    """
    __tablename__ = "nullable_encryptable"

    # key used for encryption context
    key = Column(String, nullable=False)
    # value is not encrypted
    value = Column(String, nullable=True)
    # foreign key to encrypted data
    encrypted_id = Column(UUIDType, ForeignKey("nullable_encrypted.id"), nullable=True)
    # load and update encrypted relationship automatically
    encrypted = relationship(
        NullableEncrypted,
        lazy="joined",
    )

    __table_args__ = (
        CheckConstraint(
            name="value_or_encrypted_is_null",
            sqltext="value IS NULL OR encrypted_id IS NULL",
        ),
    )

    @property
    def ciphertext(self) -> Optional[Tuple[bytes, Sequence[str]]]:
        if not self.encrypted:
            return None
        return (self.encrypted.ciphertext, self.encrypted.key_ids)

    @ciphertext.setter
    def ciphertext(self, value: Tuple[bytes, Sequence[str]]) -> None:
        ciphertext, key_ids = value
        self.encrypted = NullableEncrypted(
            ciphertext=ciphertext,
            key_ids=key_ids,
        )


@binding("nullable_encrypted_store")
class NullableEncryptedStore(Store):

    def __init__(self, graph):
        super().__init__(graph, NullableEncrypted)


@binding("nullable_encryptable_store")
class ENullablencryptableModelStore(EncryptableStore):

    def __init__(self, graph):
        super().__init__(graph, NullableEncryptable, graph.nullable_encrypted_store)
