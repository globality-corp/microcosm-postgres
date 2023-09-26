import json
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import (
    Any,
    Callable,
    Generic,
    Literal,
    ParamSpec,
    Protocol,
    TypeAlias,
    TypeVar,
    overload,
)

import sqlalchemy
from sqlalchemy.dialects.postgresql import ARRAY, JSONB


T = TypeVar("T")
JSONType: TypeAlias = (
    "dict[str, JSONType] | list[JSONType] | str | int | float | bool | None"
)


class Encoder(Protocol[T]):
    sa_type: Any

    class EncodeException(Exception):
        status_code = 400

    class DecodeException(Exception):
        status_code = 400

    def encode(self, value: T, **kwargs) -> list[str] | str:
        ...

    def decode(self, value: str, **kwargs) -> T:
        ...


P = ParamSpec("P")
R = TypeVar("R")

# The original function takes any parameters and returns any type
OriginalFunc = Callable[P, R]

# The decorated function has the same signature as the original
DecoratedFunc = Callable[P, R]


def encode_exception_wrapper(func: OriginalFunc[P, R]) -> DecoratedFunc[P, R]:
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            raise Encoder.EncodeException(f"Error encoding value: {e}")

    return wrapper


def decode_exception_wrapper(func: OriginalFunc[P, R]) -> DecoratedFunc[P, R]:
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            raise Encoder.DecodeException(f"Error decoding value: {e}")

    return wrapper


class StringEncoder(Encoder[Any]):
    sa_type = sqlalchemy.String

    @encode_exception_wrapper
    def encode(self, value: Any, **kwargs) -> str:
        return str(value)

    @decode_exception_wrapper
    def decode(self, value: str, **kwargs) -> Any:
        return value


class TextEncoder(Encoder[Any]):
    sa_type = sqlalchemy.Text

    @encode_exception_wrapper
    def encode(self, value: Any, **kwargs) -> str:
        return str(value)

    @decode_exception_wrapper
    def decode(self, value: str, **kwargs) -> Any:
        return value


class IntEncoder(Encoder[int]):
    sa_type = sqlalchemy.Integer

    @encode_exception_wrapper
    def encode(self, value: int, **kwargs) -> str:
        return str(value)

    @decode_exception_wrapper
    def decode(self, value: str, **kwargs) -> int:
        return int(value)


class DecimalEncoder(Encoder[Decimal]):
    sa_type = sqlalchemy.Numeric(asdecimal=True)

    @encode_exception_wrapper
    def encode(self, value: Decimal, **kwargs) -> str:
        return str(value)

    @decode_exception_wrapper
    def decode(self, value: str, **kwargs) -> Decimal:
        return Decimal(value)


class DatetimeEncoder(Encoder[datetime]):
    sa_type = sqlalchemy.DateTime(timezone=True)

    @encode_exception_wrapper
    def encode(self, value: datetime, **kwargs) -> str:
        return value.isoformat()

    @decode_exception_wrapper
    def decode(self, value: str, **kwargs) -> datetime:
        return datetime.fromisoformat(value)


class ArrayEncoder(Encoder[list[T]], Generic[T]):
    def __init__(self, element_encoder: Encoder[T]):
        self.element_encoder = element_encoder
        self.sa_type = ARRAY(element_encoder.sa_type)

    @overload  # type: ignore[override]
    def encode(self, value: list[T], keep_as_array: Literal[True], **kwargs) -> list[str]:
        ...

    @overload
    def encode(self, value: list[T], keep_as_array: Literal[False], **kwargs) -> str:
        ...

    @encode_exception_wrapper
    def encode(self, value: list[T], keep_as_array: bool = False, **kwargs) -> list[str] | str:
        raw = [self.element_encoder.encode(element) for element in value]
        if keep_as_array:
            assert isinstance(raw, list)
            assert isinstance(raw[0], str)
            return raw  # type: ignore[return-value]
        else:
            return json.dumps(raw)

    @decode_exception_wrapper
    def decode(self, value: str, **kwargs) -> list[T]:
        return [self.element_encoder.decode(v) for v in json.loads(value)]


class JSONEncoder(Encoder[JSONType]):
    sa_type = JSONB(none_as_null=True)

    @encode_exception_wrapper
    def encode(self, value: JSONType, **kwargs) -> str:
        return json.dumps(value)

    @decode_exception_wrapper
    def decode(self, value: str, **kwargs) -> JSONType:
        return json.loads(value)


class Nullable(Encoder[T | None], Generic[T]):
    def __init__(self, inner_encoder: Encoder[T]) -> None:
        self.inner_encoder = inner_encoder
        # Nullable encoder does not affect the sa_type
        self.sa_type = inner_encoder.sa_type

    @encode_exception_wrapper
    def encode(self, value: T | None, keep_as_array: bool = False, **kwargs) -> str | list[str]:
        if value is None:
            return json.dumps(value)

        if keep_as_array:
            return self.inner_encoder.encode(value, keep_as_array=keep_as_array)
        else:
            return json.dumps(self.inner_encoder.encode(value))

    @decode_exception_wrapper
    def decode(self, value: str, **kwargs) -> T | None:
        if (loaded_value := json.loads(value)) is None:
            return None

        return self.inner_encoder.decode(loaded_value)


E = TypeVar("E", bound=Enum)


class EnumEncoder(Encoder[E], Generic[E]):
    """
    Encodes and decodes an enum by its name.

    """
    sa_type = sqlalchemy.String

    def __init__(self, enum: type[E]):
        self._enum = enum

    @encode_exception_wrapper
    def encode(self, value: E, **kwargs) -> str:
        return value.name

    @decode_exception_wrapper
    def decode(self, value: str, **kwargs) -> E:
        return self._enum[value]
