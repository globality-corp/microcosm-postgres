from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    TypedDict,
    TypeVar,
    Union,
    cast,
    overload,
)

from sqlalchemy import Column, LargeBinary, String
from sqlalchemy.ext.hybrid import Comparator, hybrid_property
from sqlalchemy.orm import InstrumentedAttribute, Mapped
from sqlalchemy.sql.operators import in_op

from .encoders import Encoder
from .encryptors import Encryptor


T = TypeVar("T")


NOT_SET = object()


class BeaconComparator(Comparator):
    def __init__(
        self,
        val,
        encoder_fn: Union[Callable, None] = None,
        beacon_fn: Union[Callable[[str], str], None] = None,
        encrypt_fn: Union[Callable[[str], Union[bytes, None]], None] = None,
        beacon_val: Any = None,
    ):
        self.val = val

        # Note that we only store beacon val when we are instantiating as part of
        # the sqlalchemy model setup
        self.beacon_val = beacon_val

        self.encoder_fn = encoder_fn
        self.beacon_fn = beacon_fn
        self.encrypt_fn = encrypt_fn

    def operate(self, op: Callable, other: Any = NOT_SET, **kwargs: Any):  # type: ignore[override]  # noqa: E501
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
        return self.beacon_val

    def __str__(self):
        return self.val

    def __eq__(self, other: Any):  # type: ignore[override]  # noqa: E501
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


class encryption(hybrid_property, Generic[T]):
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
        default: Union[T, Callable[[], T]],
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

        def _prop(self):
            encrypted = getattr(self, encrypted_field)

            if encrypted is None:
                return getattr(self, unencrypted_field)

            return encoder.decode(encryptor.decrypt(encrypted))

        def _prop_setter(self, value) -> None:
            encoded = encoder.encode(value)
            encrypted = encryptor.encrypt(encoded)
            if encrypted is None:
                setattr(self, unencrypted_field, value)
                setattr(self, encrypted_field, None)
                if hasattr(self, beacon_field):
                    setattr(self, beacon_field, None)
                return

            setattr(self, encrypted_field, encrypted)
            setattr(self, unencrypted_field, None)
            if hasattr(self, beacon_field):
                setattr(self, beacon_field, encryptor.beacon(encoded))

        def _prop_comparator(cls) -> Union[Comparator, InstrumentedAttribute]:
            if beacon := getattr(cls, beacon_field, None):
                return BeaconComparator(
                    val=getattr(cls, unencrypted_field),
                    beacon_val=beacon,
                    encoder_fn=encoder.encode,
                    beacon_fn=encryptor.beacon,
                    encrypt_fn=encryptor.encrypt,
                )

            return getattr(cls, unencrypted_field)

        super().__init__(
            _prop,
            _prop_setter,
            custom_comparator=cast(Union[Comparator, None], _prop_comparator),
        )

    if TYPE_CHECKING:

        def __get__(self, instance: Any, owner: Any) -> T:
            ...

        def __set__(self, instance: Any, value: T) -> None:
            ...

    def encrypted(self) -> Mapped:
        info = EncryptionV2ColumnInfo(
            encryption_v2_key=self.key,
            encryption_v2_encrypted=True,
        )

        if self.default is NOT_SET:
            return Column(self.key + "_encrypted", LargeBinary, nullable=True, info=cast(dict, info))

        return Column(
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

    def unencrypted(self, **kwargs: Any) -> Mapped:
        info = {
            **kwargs.pop("info", {}),
            **EncryptionV2ColumnInfo(
                encryption_v2_key=self.key,
                encryption_v2_unencrypted=True,
            )
        }

        if self.default is NOT_SET:
            return Column(self.key, self.column_type, nullable=True, info=info, **kwargs)

        return Column(
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

    def beacon(self, **kwargs: Any) -> Mapped[Union[str, None]]:
        return Column(
            String,
            nullable=True,
            info=cast(dict, EncryptionV2ColumnInfo(
                encryption_v2_key=self.key,
                encryption_v2_beacon=True,
            )),
            **kwargs,
        )
