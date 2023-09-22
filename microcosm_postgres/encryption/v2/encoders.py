import json
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import (
    Any,
    Generic,
    Protocol,
    TypeVar,
    Union,
)

import sqlalchemy
from sqlalchemy.dialects.postgresql import JSONB
from typing_extensions import TypeAlias


T = TypeVar("T")
JSONType: TypeAlias = (
    "Union[dict[str, JSONType], list[JSONType], str, int, float, bool, None]"
)


class Encoder(Protocol[T]):
    sa_type: Any

    def encode(self, value: T) -> str:
        ...

    def decode(self, value: str) -> T:
        ...


class StringEncoder(Encoder[str]):
    sa_type = sqlalchemy.String

    def encode(self, value: str) -> str:
        return value

    def decode(self, value: str) -> str:
        return value


class IntEncoder(Encoder[int]):
    sa_type = sqlalchemy.Integer

    def encode(self, value: int) -> str:
        return str(value)

    def decode(self, value: str) -> int:
        return int(value)


class DecimalEncoder(Encoder[Decimal]):
    sa_type = sqlalchemy.Numeric(asdecimal=True)

    def encode(self, value: Decimal) -> str:
        return str(value)

    def decode(self, value: str) -> Decimal:
        return Decimal(value)


class DatetimeEncoder(Encoder[datetime]):
    sa_type = sqlalchemy.DateTime(timezone=True)

    def encode(self, value: datetime) -> str:
        return value.isoformat()

    def decode(self, value: str) -> datetime:
        return datetime.fromisoformat(value)


class ArrayEncoder(Encoder["list[T]"], Generic[T]):
    def __init__(self, element_encoder: Encoder[T]):
        self.element_encoder = element_encoder
        self.sa_type = sqlalchemy.ARRAY(element_encoder.sa_type)

    def encode(self, value: "list[T]") -> str:
        return json.dumps([self.element_encoder.encode(element) for element in value])

    def decode(self, value: str) -> "list[T]":
        return [self.element_encoder.decode(v) for v in json.loads(value)]


class JSONEncoder(Encoder[JSONType]):
    sa_type = JSONB(none_as_null=True)

    def encode(self, value: JSONType) -> str:
        return json.dumps(value)

    def decode(self, value: str) -> JSONType:
        return json.loads(value)


class Nullable(Encoder["Union[T, None]"], Generic[T]):
    def __init__(self, inner_encoder: Encoder[T]) -> None:
        self.inner_encoder = inner_encoder
        # Nullable encoder does not affect the sa_type
        self.sa_type = inner_encoder.sa_type

    def encode(self, value: "Union[T, None]") -> str:
        if value is None:
            return json.dumps(value)

        return json.dumps(self.inner_encoder.encode(value))

    def decode(self, value: str) -> "Union[T, None]":
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

    def encode(self, value: E) -> str:
        return value.name

    def decode(self, value: str) -> E:
        return self._enum[value]
