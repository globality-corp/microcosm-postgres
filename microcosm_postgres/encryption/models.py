"""
Encryption-related models.

"""
from typing import Sequence, Tuple

from sqlalchemy import Column, LargeBinary, String
from sqlalchemy.event import contains, listen, remove
from sqlalchemy.dialects.postgresql import ARRAY

from microcosm_postgres.encryption.encryptor import Encryptor


def on_init(target: "EncryptableMixin", args, kwargs):
    """
    Intercept SQLAlchemy's instance init event.

    SQLALchemy allows callback to intercept ORM instance init functions. The calling arguments
    will be an empty instance of the `target` model, plus the arguments passed to `__init__`.

    The `kwargs` dictionary is mutable (which is why it is not passed as `**kwargs`). We leverage
    this callback to conditionally remove the `__plaintext__` value and set the `ciphertext` property.

    """
    encryptor = target.__encryptor__

    # encryption context may be nullable
    try:
        encryption_context_key = str(kwargs[target.__encryption_context_key__])
    except KeyError:
        return

    # do not encrypt targets that are not configured for it
    if encryption_context_key not in encryptor:
        return

    plaintext = kwargs.pop(target.__plaintext__)
    ciphertext, key_ids = encryptor.encrypt(encryption_context_key, plaintext)
    target.ciphertext = (ciphertext, key_ids)


def on_load(target: "EncryptableMixin", context):
    """
    Intercept SQLAlchemy's instance load event.

    """
    encryptor = target.__encryptor__

    # encryption context may be nullable
    if target.encryption_context_key is None:
        return

    encryption_context_key = str(target.encryption_context_key)

    # do not decrypt targets that are not configured for it
    if encryption_context_key not in encryptor:
        return

    ciphertext, key_ids = target.ciphertext
    target.plaintext = encryptor.decrypt(encryption_context_key, ciphertext)


class EncryptableMixin:
    """
    A (conditionally) encryptable model.

    Using SQLAlchemy ORM events to intercept instance construction and loading to
    enforce encryption (if appropriate for the `encryption_context_key`). Should be
    combined with database constraints to enforce that the instance is *either* encrypted
    or un-encrypted, but *not* both.

    Must define:

     -  An `encryption_context_key` property (defaults to `self.key`)
     -  A settable, `plaintext` property (defaults to `self.value`)
     -  A settable, `ciphertext` property (not defaulted)

    """
    __encryptor__ = None
    __encryption_context_key__ = "key"
    __plaintext__ = "value"

    @property
    def encryption_context_key(self) -> str:
        return getattr(self, self.__encryption_context_key__)

    @property
    def plaintext(self) -> str:
        return getattr(self, self.__plaintext__)

    @plaintext.setter
    def plaintext(self, value: str) -> None:
        return setattr(self, self.__plaintext__, value)

    @property
    def ciphertext(self) -> Tuple[bytes, Sequence[str]]:
        raise NotImplementedError("Encryptable must implement `ciphertext` property")

    @ciphertext.setter
    def ciphertext(self, value: Tuple[bytes, Sequence[str]]) -> None:
        raise NotImplementedError("Encryptable must implement `ciphertext` property")

    @classmethod
    def register(cls, encryptor: Encryptor):
        """
        Register this encryptable with an encryptor.

        Instances of this encryptor will be encrypted on initialization and decrypted on load.

        """
        # save the current encryptor statically
        cls.__encryptor__ = encryptor

        # NB: we cannot use the before_insert listener in conjunction with a foreign key relationship
        # for encrypted data; SQLAlchemy will warn about using 'related attribute set' operation so
        # late in its insert/flush process.
        listeners = dict(
            init=on_init,
            load=on_load,
        )

        for name, func in listeners.items():
            # If we initialize the graph multiple times (as in many unit testing scenarios),
            # we will accumulate listener functions -- with unpredictable results. As protection,
            # we need to remove existing listeners before adding new ones; this solution only
            # works if the id (e.g. memory address) of the listener does not change, which means
            # they cannot be closures around the `encryptor` reference.
            #
            # Hence the `__encryptor__` hack above...
            if contains(cls, name, func):
                remove(cls, name, func)
            listen(cls, name, func)


class EncryptedMixin:
    """
    A mixin that in include ciphertext and an array of key ids.

    """
    # save the encrypted data as unindexed binary
    ciphertext = Column(LargeBinary, nullable=False)
    # save the encryption key ids in an indexed column for future re-keying
    key_ids = Column(ARRAY(String), nullable=False, index=True)
