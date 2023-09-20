from typing import (
    Any,
    Callable,
    Generic,
    TypeVar,
    overload,
)

from sqlalchemy import LargeBinary, String, ColumnElement
from sqlalchemy.ext.hybrid import hybrid_property, Comparator
from sqlalchemy.orm import Mapped, mapped_column

from .encoders import Encoder
from .encryptors import Encryptor


T = TypeVar("T")


NOT_SET = object()


def beaconise(word):
    """
    Mock implementation of beaconisation.
    """
    return f"{word} beaconised"


class BeaconComparator(Comparator[str]):
    def __init__(self, val, beacon_val: Any = None):
        if isinstance(val, str):
            self.val = beaconise(val)
        elif isinstance(val, BeaconComparator):
            self.val = val.val
        else:
            self.val = val

        # Note that we only store beacon val when we are instantiating as part of
        # the sqlalchemy model setup
        self.beacon_val = beacon_val

    def operate(self, op, other: Any = NOT_SET, **kwargs: Any):
        # This first condition might happen when we are doing an order by ACS / DESC
        if other is NOT_SET:
            return op(self.beacon_val)
        if not isinstance(other, BeaconComparator):
            other = BeaconComparator(other)
        return op(self.val, other.val, **kwargs)

    def __clause_element__(self):
        return self.beacon_val

    def __str__(self):
        return self.val

    def __eq__(self, other: Any) -> ColumnElement[bool]:  # type: ignore[override]  # noqa: E501
        # Here we would beaconise the "other" value
        return self.__clause_element__() == beaconise(other)

    key = 'beacon'


class BeaconNotDefinedError(Exception):
    pass


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

        beacon_field = f"{key}_beacon"
        encrypted_field = f"{key}_encrypted"
        unencrypted_field = f"{key}_unencrypted"

        def _prop(self):
            encrypted = getattr(self, encrypted_field)

            if encrypted is None:
                return getattr(self, unencrypted_field)

            return encoder.decode(encryptor.decrypt(encrypted))

        def _prop_setter(self, value) -> None:
            encrypted = encryptor.encrypt(encoder.encode(value))
            if encrypted is None:
                setattr(self, unencrypted_field, value)
                setattr(self, encrypted_field, None)
                return

            if hasattr(self, beacon_field):
                setattr(self, beacon_field, beaconise(value))

            setattr(self, encrypted_field, encrypted)
            setattr(self, unencrypted_field, None)

        def _prop_comparator(cls):
            if beacon := getattr(cls, beacon_field, None):
                return BeaconComparator(getattr(cls, unencrypted_field), beacon)

            return getattr(cls, unencrypted_field)

        super().__init__(_prop, _prop_setter, custom_comparator=_prop_comparator)

    def encrypted(self) -> Mapped[bytes | None]:
        if self.default is NOT_SET:
            return mapped_column(self.key + "_encrypted", LargeBinary, nullable=True)

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
        )

    def unencrypted(self, **kwargs: Any) -> Mapped[T | None]:
        if self.default is NOT_SET:
            return mapped_column(self.key, self.column_type, nullable=True, **kwargs)

        return mapped_column(
            self.key,
            self.column_type,
            nullable=True,
            default=lambda: (
                None
                if self.encryptor.should_encrypt()
                else (self.default() if callable(self.default) else self.default)
            ),
            **kwargs,
        )

    def beacon(self, **kwargs: Any):
        # TODO - assume all encrypted fields need to use beacons
        return mapped_column(String, nullable=True, **kwargs)
