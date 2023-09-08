import json
from decimal import Decimal
from typing import (
    Generic,
    Protocol,
    TypeAlias,
    TypeVar,
)


T = TypeVar("T")
JSONType: TypeAlias = (
    "dict[str, JSONType] | list[JSONType] | str | int | float | bool | None"
)


class Encoder(Protocol[T]):
    def encode(self, value: T) -> str:
        ...

    def decode(self, value: str) -> T:
        ...


class StringEncoder(Encoder[str]):
    def encode(self, value: str) -> str:
        return value

    def decode(self, value: str) -> str:
        return value


class IntEncoder(Encoder[int]):
    def encode(self, value: int) -> str:
        return str(value)

    def decode(self, value: str) -> int:
        return int(value)


class DecimalEncoder(Encoder[Decimal]):
    def encode(self, value: Decimal) -> str:
        return str(value)

    def decode(self, value: str) -> Decimal:
        return Decimal(value)


class ArrayEncoder(Encoder[list[T]], Generic[T]):
    def __init__(self, element_encoder: Encoder[T]):
        self.element_encoder = element_encoder

    def encode(self, value: list[T]) -> str:
        return json.dumps([self.element_encoder.encode(element) for element in value])

    def decode(self, value: str) -> list[T]:
        return [self.element_encoder.decode(v) for v in json.loads(value)]


class JSONEncoder(Encoder[JSONType]):
    def encode(self, value: JSONType) -> str:
        return json.dumps(value)

    def decode(self, value: str) -> JSONType:
        return json.loads(value)


class Nullable(Encoder[T | None], Generic[T]):
    def __init__(self, inner_encoder: Encoder[T]) -> None:
        self.inner_encoder = inner_encoder

    def encode(self, value: T | None) -> str:
        if value is None:
            return json.dumps(value)

        return json.dumps(self.inner_encoder.encode(value))

    def decode(self, value: str) -> T | None:
        if (loaded_value := json.loads(value)) is None:
            return None

        return self.inner_encoder.decode(loaded_value)
