from typing import (
    Any,
    Callable,
    Generic,
    TypedDict,
    TypeVar,
    cast,
    overload,
)

from sqlalchemy import ColumnElement, LargeBinary, String
from sqlalchemy.ext.hybrid import Comparator, hybrid_property
from sqlalchemy.orm import InstrumentedAttribute, Mapped, mapped_column
from sqlalchemy.sql.operators import in_op

from .encoders import Encoder
from .encryptors import Encryptor


T = TypeVar("T")


NOT_SET = object()


class BeaconComparator(Comparator):
    def __init__(
        self,
        val,
        encoder_fn: Callable | None = None,
        beacon_fn: Callable[[str], str] | None = None,
        encrypt_fn: Callable[[str], bytes | None] | None = None,
        beacon_val: Any = None,
    ):
        self.val = val

        # Note that we only store beacon val when we are instantiating as part of
        # the sqlalchemy model setup
        self.beacon_val = beacon_val

        self.encoder_fn = encoder_fn
        self.beacon_fn = beacon_fn
        self.encrypt_fn = encrypt_fn

    def operate(self, op: Callable, other: Any = NOT_SET, **kwargs: Any) -> ColumnElement[Any]:  # type: ignore[override]  # noqa: E501
        # This first condition might happen when we are doing an order by ACS / DESC
        if other is NOT_SET:
            if self._check_if_should_use_beacon():
                return op(self.beacon_val)
            else:
                return op(self.val)
        elif op == in_op:
            # e.g in_([1,2,3])
            # So we beaconise each of the values in the list
            # e.g [1,2,3] -> [beacon(1), beacon(2), beacon(3)]
            if self._check_if_should_use_beacon():
                return op(self.beacon_val, [self._beaconise(v) for v in other], **kwargs)
            else:
                return op(self.val, other, **kwargs)
        return op(self.val, other, **kwargs)

    def __clause_element__(self):
        if self._check_if_should_use_beacon():
            return self.beacon_val
        else:
            return self.val

    def __str__(self):
        return self.val

    def __eq__(self, other: Any) -> ColumnElement[bool]:  # type: ignore[override]  # noqa: E501
        if not self._check_if_should_use_beacon():
            return self.val == other

        return self.__clause_element__() == self._beaconise(other)

    def _beaconise(self, value):
        encoded = self.encoder_fn(value)
        beaconised = self.beacon_fn(encoded)
        return beaconised

    def _check_if_should_use_beacon(self):
        # If the encrypt_fn returns `None` we know not use the beacon
        # and just use the normal flow (without beacons)
        if self.encrypt_fn is None:
            return False

        test_val_to_encrypt = "test"
        encrypted_val = self.encrypt_fn(test_val_to_encrypt)
        return encrypted_val is not None

    key = 'beacon'


class BeaconNotDefinedError(Exception):
    pass


class EncryptionV2ColumnInfo(TypedDict, total=False):
    encryption_v2_key: str
    encryption_v2_encrypted: bool
    encryption_v2_unencrypted: bool
    encryption_v2_beacon: bool


class encryption(hybrid_property[T], Generic[T]):
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

        # This is used in the store auto filters
        self.name = key

        beacon_field = f"{key}_beacon"
        encrypted_field = f"{key}_encrypted"
        unencrypted_field = f"{key}_unencrypted"

        # Shortcuts to the relevant functions used throughout the hybrid
        encrypt_fn = encryptor.encrypt
        decrypt_fn = encryptor.decrypt
        beacon_fn = encryptor.beacon
        encoder_fn = encoder.encode
        decoder_fn = encoder.decode

        def _prop(self):
            encrypted = getattr(self, encrypted_field)

            if encrypted is None:
                return getattr(self, unencrypted_field)

            return decoder_fn(decrypt_fn(encrypted))

        def _prop_setter(self, value) -> None:
            encoded = encoder_fn(value)
            encrypted = encrypt_fn(encoded)
            if encrypted is None:
                setattr(self, unencrypted_field, value)
                setattr(self, encrypted_field, None)
                if hasattr(self, beacon_field):
                    setattr(self, beacon_field, None)
                return

            setattr(self, encrypted_field, encrypted)
            setattr(self, unencrypted_field, None)
            if hasattr(self, beacon_field):
                setattr(self, beacon_field, beacon_fn(encoded))

        def _prop_comparator(cls) -> Comparator[T] | InstrumentedAttribute:
            if beacon := getattr(cls, beacon_field, None):
                return BeaconComparator(
                    val=getattr(cls, unencrypted_field),
                    beacon_val=beacon,
                    encoder_fn=encoder_fn,
                    beacon_fn=beacon_fn,
                    encrypt_fn=encrypt_fn,
                )

            return getattr(cls, unencrypted_field)

        super().__init__(
            _prop,
            _prop_setter,
            custom_comparator=cast(Comparator[T] | None, _prop_comparator),
        )

    def encrypted(self) -> Mapped[bytes | None]:
        info = EncryptionV2ColumnInfo(
            encryption_v2_key=self.key,
            encryption_v2_encrypted=True,
        )

        if self.default is NOT_SET:
            return mapped_column(self.key + "_encrypted", LargeBinary, nullable=True, info=cast(dict, info))

        return mapped_column(
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
            info=cast(dict, info),
        )

    def unencrypted(self, **kwargs: Any) -> Mapped[T | None]:
        info = {
            **kwargs.pop("info", {}),
            **EncryptionV2ColumnInfo(
                encryption_v2_key=self.key,
                encryption_v2_unencrypted=True,
            )
        }

        if self.default is NOT_SET:
            return mapped_column(self.key, self.column_type, nullable=True, info=info, **kwargs)

        return mapped_column(
            self.key,
            self.column_type,
            nullable=True,
            default=lambda: (
                None
                if self.encryptor.should_encrypt()
                else (self.default() if callable(self.default) else self.default)
            ),
            info=info,
            **kwargs,
        )

    def beacon(self, **kwargs: Any) -> Mapped[str | None]:
        return mapped_column(
            String,
            nullable=True,
            info=cast(dict, EncryptionV2ColumnInfo(
                encryption_v2_key=self.key,
                encryption_v2_beacon=True,
            )),
            **kwargs,
        )
