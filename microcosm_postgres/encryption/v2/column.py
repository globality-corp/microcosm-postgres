from collections.abc import Callable
from typing import (
    Any,
    Generic,
    TypedDict,
    TypeVar,
    cast,
    overload,
)

from sqlalchemy import ColumnElement, LargeBinary, String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql.operators import CONTAINED_BY, CONTAINS, OVERLAP
from sqlalchemy.ext.hybrid import Comparator, hybrid_property
from sqlalchemy.orm import InstrumentedAttribute, Mapped, mapped_column
from sqlalchemy.sql.operators import in_op

from microcosm_postgres.encryption.v2.beacons import BeaconHashAlgorithm
from microcosm_postgres.encryption.v2.errors import DecryptionError

from .encoders import ArrayEncoder, Encoder, Nullable
from .encryptors import Encryptor


T = TypeVar("T")


NOT_SET = object()


class BeaconComparator(Comparator):
    def __init__(
        self,
        val,
        encoder_fn: Callable,
        beacon_fn: Callable,
        encrypt_fn: Callable[[str], bytes | None],
        beacon_val: Any = None,
        beacon_algorithm: BeaconHashAlgorithm | None = None,
    ):
        self.val = val

        # Note that we only store beacon val when we are instantiating as part of
        # the sqlalchemy model setup
        self.beacon_val = beacon_val

        self.encoder_fn = encoder_fn
        self.beacon_fn = beacon_fn
        self.encrypt_fn = encrypt_fn
        self.beacon_algorithm = beacon_algorithm

    def operate(
        self, op: Callable, other: Any = NOT_SET, **kwargs: Any
    ) -> ColumnElement[Any]:  # type: ignore[override]  # noqa: E501
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
                return op(
                    self.beacon_val,
                    self._beaconise(other, use_array=isinstance(other, list)),
                    **kwargs,
                )
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

    def _beaconise(self, value: Any, use_array: bool = False) -> Any:
        if use_array:
            return [self._beaconise(v) for v in value]

        encoded = self.encoder_fn(value)
        beaconised = self.beacon_fn(encoded, algorithm=self.beacon_algorithm)
        return beaconised

    def _check_if_should_use_beacon(self):
        # If the encrypt_fn returns `None` we know not use the beacon
        # and just use the normal flow (without beacons)
        if self.encrypt_fn is None:
            return False

        test_val_to_encrypt = "test"
        encrypted_val = self.encrypt_fn(test_val_to_encrypt)
        return encrypted_val is not None

    key = "beacon"


class BeaconArrayComparator(ARRAY.Comparator):
    def __init__(
        self,
        val,
        array_beaconiser: "ArrayBeaconsiser",
        encrypt_fn: Callable[[str], bytes | None],
        beacon_val: Any = None,
        beacon_algorithm: BeaconHashAlgorithm | None = None,
    ):
        self.val = val

        # Note that we only store beacon val when we are instantiating as part of
        # the sqlalchemy model setup
        self.beacon_val = beacon_val
        self.array_beaconiser = array_beaconiser
        self.encrypt_fn = encrypt_fn
        self.beacon_algorithm = beacon_algorithm

    def operate(
        self, op: Callable, other: Any = NOT_SET, **kwargs: Any
    ) -> ColumnElement[Any]:
        if not self._check_if_should_use_beacon():
            return op(self.val, other, **kwargs)

        if op == CONTAINS:
            return op(self.beacon_val, self._beaconise(other), **kwargs)

        elif op == OVERLAP:
            return op(self.beacon_val, self._beaconise(other), **kwargs)

        elif op == CONTAINED_BY:
            return op(self.beacon_val, self._beaconise(other), **kwargs)

        return op(self.val, other, **kwargs)

    def __eq__(self, other: Any) -> ColumnElement[bool]:  # type: ignore[override]  # noqa: E501
        if not self._check_if_should_use_beacon():
            return self.val == other

        return self.__clause_element__() == self._beaconise(other)

    def __clause_element__(self):
        if self._check_if_should_use_beacon():
            return self.beacon_val
        return self.val

    def __str__(self):
        return self.val

    def _beaconise(self, value: Any) -> Any:
        return self.array_beaconiser(value, beacon_algorithm=self.beacon_algorithm)

    def _check_if_should_use_beacon(self):
        # If the encrypt_fn returns `None` we know not use the beacon
        # and just use the normal flow (without beacons)
        if self.encrypt_fn is None:
            return False

        test_val_to_encrypt = "test"
        encrypted_val = self.encrypt_fn(test_val_to_encrypt)
        return encrypted_val is not None


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
        use_beacon_array: bool = False,
        beacon_algorithm: BeaconHashAlgorithm | None = None,
    ): ...

    @overload
    def __init__(
        self,
        key: str,
        encryptor: Encryptor,
        encoder: Encoder[T],
        *,
        default: T | Callable[[], T],
        column_type: Any = NOT_SET,
        use_beacon_array: bool = False,
        beacon_algorithm: BeaconHashAlgorithm | None = None,
    ): ...

    def __init__(
        self,
        key: str,
        encryptor: Encryptor,
        encoder: Encoder[T],
        *,
        column_type: Any = NOT_SET,
        default: Any = NOT_SET,
        use_beacon_array: bool = False,
        beacon_algorithm: BeaconHashAlgorithm | None = None,
    ):
        self.default = default
        self.key = key
        self.encryptor = encryptor
        self.encoder = encoder
        self.column_type = encoder.sa_type if column_type is NOT_SET else column_type
        self.use_beacon_array = use_beacon_array
        self.beacon_algorithm = beacon_algorithm

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

        array_beaconiser = (
            ArrayBeaconsiser(encoder, encryptor) if self.use_beacon_array else None
        )
        
        def _prop(self):
            encrypted = getattr(self, encrypted_field)

            if encrypted is None:
                return getattr(self, unencrypted_field)
            try:
                return decoder_fn(decrypt_fn(encrypted))
            except DecryptionError:
                return encoder.redacted_value

        def _prop_setter(self, value) -> None:
            # We ignore the type - should come back as a string
            # as we don't explicitly say use_array=True inside the encoder
            # Typing needs to be updated in all encoder functions to support
            # properly
            encoded = encoder_fn(value)  # type: ignore[arg-type]
            encrypted = encrypt_fn(encoded)  # type: ignore[arg-type]
            if encrypted is None:
                setattr(self, unencrypted_field, value)
                setattr(self, encrypted_field, None)
                if hasattr(self, beacon_field):
                    setattr(self, beacon_field, None)
                return

            setattr(self, encrypted_field, encrypted)
            setattr(self, unencrypted_field, None)
            if hasattr(self, beacon_field):
                if array_beaconiser:
                    setattr(
                        self,
                        beacon_field,
                        array_beaconiser(value, beacon_algorithm=beacon_algorithm),
                    )
                else:
                    setattr(
                        self,
                        beacon_field,
                        beacon_fn(encoded, algorithm=beacon_algorithm),
                    )

        def _prop_comparator(cls) -> Comparator[T] | InstrumentedAttribute:
            if beacon := getattr(cls, beacon_field, None):
                if array_beaconiser:
                    return BeaconArrayComparator(  # type: ignore[return-value]
                        val=getattr(cls, unencrypted_field),
                        beacon_val=beacon,
                        array_beaconiser=array_beaconiser,
                        encrypt_fn=encrypt_fn,
                        beacon_algorithm=beacon_algorithm,
                    )
                return BeaconComparator(
                    val=getattr(cls, unencrypted_field),
                    beacon_val=beacon,
                    encoder_fn=encoder_fn,
                    beacon_fn=beacon_fn,
                    encrypt_fn=encrypt_fn,
                    beacon_algorithm=beacon_algorithm,
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
            return mapped_column(
                self.key + "_encrypted",
                LargeBinary,
                nullable=True,
                info=cast(dict, info),
            )

        return mapped_column(
            self.key + "_encrypted",
            LargeBinary,
            nullable=True,
            default=(
                lambda: (
                    self.encryptor.encrypt(
                        cast(
                            str,
                            self.encoder.encode(
                                self.default()
                                if callable(self.default)
                                else self.default
                            ),
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
            ),
        }

        if self.default is NOT_SET:
            return mapped_column(
                self.key, self.column_type, nullable=True, info=info, **kwargs
            )

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

    def beacon(self, **kwargs: Any) -> Mapped[str | None] | Mapped[list[str] | None]:
        column_type = ARRAY(String()) if self.use_beacon_array else String()
        return mapped_column(
            column_type,  # type: ignore[arg-type]
            nullable=True,
            info=cast(
                dict,
                EncryptionV2ColumnInfo(
                    encryption_v2_key=self.key,
                    encryption_v2_beacon=True,
                ),
            ),
            **kwargs,
        )


class ArrayBeaconsiser:
    def __init__(self, encoder: Encoder, encryptor: Encryptor) -> None:
        match encoder:
            case Nullable(inner_encoder=ArrayEncoder(element_encoder=element_encoder)):
                self.element_encoder = element_encoder
            case ArrayEncoder(element_encoder=element_encoder):
                self.element_encoder = element_encoder

            case _:
                raise ValueError(
                    "use_beacon_array can only be used with ArrayEncoder or Nullable(ArrayEncoder)"
                )

        self.encryptor = encryptor

    def __call__(
        self, value: list[T] | None, beacon_algorithm: BeaconHashAlgorithm | None = None
    ) -> list[str] | str:
        if value is None:
            return self.encryptor.beacon(
                Nullable.null_encoded, algorithm=beacon_algorithm
            )

        return [
            self.encryptor.beacon(
                self.element_encoder.encode(v),
                algorithm=beacon_algorithm,
            )
            for v in value
        ]
