from typing import Sequence, Tuple

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


class Parent(EntityMixin, Model):
    __tablename__ = "parent"

    name = Column(String)

    __mapper_args__ = {
        "polymorphic_identity": "parent",
        "polymorphic_on": name,
    }


class SubEncrypted(EntityMixin, EncryptedMixin, Model):
    __tablename__ = "sub_encrypted"


class SubEncryptable(Parent, EncryptableMixin):
    """
    A model for conditionally-encrypted plaintext.

    """
    __tablename__ = "sub_encryptable"

    id = Column(UUIDType, ForeignKey("parent.id"), primary_key=True)
    # key used for encryption context
    key = Column(String, nullable=False)
    # value is not encrypted
    value = Column(String, nullable=True)
    # foreign key to encrypted data
    sub_encrypted_id = Column(UUIDType, ForeignKey("sub_encrypted.id"), nullable=True)
    # load and update encrypted relationship automatically
    sub_encrypted = relationship(
        SubEncrypted,
        lazy="joined",
    )

    __mapper_args__ = {
        "polymorphic_identity": "sub",
    }

    __table_args__ = (
        CheckConstraint(
            name="value_or_encrypted_is_not_null",
            sqltext="value IS NOT NULL OR sub_encrypted_id IS NOT NULL",
        ),
        CheckConstraint(
            name="value_or_encrypted_is_null",
            sqltext="value IS NULL OR sub_encrypted_id IS NULL",
        ),
    )
    __encrypted_identifier__ = "sub_encrypted_id"

    @property
    def ciphertext(self) -> Tuple[bytes, Sequence[str]]:
        return (self.sub_encrypted.ciphertext, self.sub_encrypted.key_ids)

    @ciphertext.setter
    def ciphertext(self, value: Tuple[bytes, Sequence[str]]) -> None:
        ciphertext, key_ids = value
        self.sub_encrypted = SubEncrypted(
            ciphertext=ciphertext,
            key_ids=key_ids,
        )


@binding("sub_encrypted_store")
class SubEncryptedStore(Store):

    def __init__(self, graph):
        super().__init__(graph, SubEncrypted)


@binding("sub_encryptable_store")
class SubEncryptableModelStore(EncryptableStore):

    def __init__(self, graph):
        super().__init__(graph, SubEncryptable, graph.sub_encrypted_store)


@binding("parent_store")
class ParentStore(Store):

    def __init__(self, graph):
        super().__init__(graph, Parent)
