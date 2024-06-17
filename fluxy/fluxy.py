from abc import ABC, abstractmethod
from dataclasses import dataclass, fields
from datetime import datetime, timedelta
from enum import Enum
from functools import reduce
from typing import Callable, List, Optional, Protocol, TypeVar, cast, overload


def dedent(val: str) -> str:
    lines = val.split("\n")
    return "\n".join(line.strip() for line in lines)


class Operation(Protocol):
    def to_flux(self) -> str:
        ...


@dataclass
class PartialPipe:
    initial: "From"


@dataclass
class Pipe:
    initial: "From"
    range: "Range | RangeOffset"
    operations: "List[Operation]"

    def to_flux(self) -> str:
        fluxes = (op.to_flux() for op in [self.initial, self.range, *self.operations])
        return "\n|> ".join(fluxes)


@overload
def pipe(first: "From") -> PartialPipe:
    ...


@overload
def pipe(first: PartialPipe) -> PartialPipe:
    ...


@overload
def pipe(first: Pipe) -> Pipe:
    ...


@overload
def pipe(
    first: "PartialPipe | From", second: "Range | RangeOffset", *ops: "Operation"
) -> Pipe:
    ...


@overload
def pipe(first: Pipe, *ops: "Operation") -> Pipe:
    ...


def pipe(
    first: "From | Pipe | PartialPipe",
    second: "Range | RangeOffset | Operation | None" = None,
    *ops: "Operation",
) -> Pipe | PartialPipe:
    match (first, second):
        case (From(_) as from_bucket, None):
            return PartialPipe(from_bucket)
        case (From(_) as from_bucket, Range(_) | RangeOffset(_) as range):
            return Pipe(from_bucket, range, list(ops))
        case (PartialPipe(_) as pipe, None):
            return pipe
        case (PartialPipe(_) as pipe, Range(_) | RangeOffset(_) as range):
            return Pipe(pipe.initial, range, list(ops))
        case (Pipe(_) as pipe, None):
            return pipe
        case (Pipe(_) as pipe, op):
            return Pipe(
                pipe.initial, pipe.range, [*pipe.operations, cast(Operation, op), *ops]
            )
        case _:
            raise Exception("invalid types")


Query = Pipe


@dataclass
class From:
    bucket: str

    def to_flux(self) -> str:
        return f'from(bucket: "{self.bucket}")'


@dataclass
class PipedFunction(ABC):
    def to_flux(self) -> str:
        arguments = [
            f"{PipedFunction._snakecase_to_camelcase(field.name)}: {PipedFunction._to_flux(getattr(self, field.name))}"
            for field in fields(self.__class__)
        ]
        return dedent(f"""{self.function}({", ".join(arguments)})""")

    @property
    def function(self) -> str:
        name = self.__class__.__name__
        return name[0:1].lower() + name[1:]

    @staticmethod
    def _snakecase_to_camelcase(val: str) -> str:
        upper_camel_case = "".join(part.capitalize() for part in val.split("_"))
        return upper_camel_case[0:1].lower() + upper_camel_case[1:]

    @staticmethod
    def _list_to_flux(list):
        return "[" + ", ".join(f'"{key}"' for key in list) + "]"

    @staticmethod
    def _timedelta_to_flux(every: timedelta) -> str:
        def _part(value: int, unit: str) -> str:
            return f"{value}{unit}" if value != 0 else ""

        return "".join(
            [
                _part(every.days, "d"),
                _part(every.seconds, "s"),
                _part(every.microseconds, "us"),
            ]
        )

    @staticmethod
    def _to_flux(value: list[str] | bool | str | timedelta):
        match value:
            case str():
                return f'"{value}"'
            case bool():
                return bool_to_flux(value)
            case list():
                return PipedFunction._list_to_flux(value)
            case timedelta():
                return PipedFunction._timedelta_to_flux(value)
            case datetime():
                return value.isoformat()
            case Enum():
                return value.value


@dataclass
class Range(PipedFunction):
    start: datetime
    stop: datetime


@dataclass
class RangeOffset(PipedFunction):
    start: timedelta

    @property
    def function(self) -> str:
        return "range"


@dataclass
class LiteralStringExpression:
    value: str

    def to_flux(self):
        return f'"{self.value}"'


Expression = LiteralStringExpression


def parse_expression(value):
    match value:
        case str():
            return LiteralStringExpression(value)
        case _:
            raise Exception(f"didn't recognize expression {value}")


@dataclass
class Clause(ABC):
    @staticmethod
    def parse(val: "Clausable") -> "Clause":
        match val:
            case bool():
                return LiteralClause(val)
            case _:
                return val

    def should_parenthesize(self, other: "Clause") -> bool:
        return self.precedence < other.precedence

    def ensure_precedence(self, other: "Clause") -> str:
        return (
            f"({other.to_flux()})"
            if self.should_parenthesize(other)
            else other.to_flux()
        )

    def __or__(self, other: "Clausable") -> "Clause":
        return BinaryClause(BinaryOperation.OR, self, Clause.parse(other))

    def __and__(self, other: "Clausable") -> "Clause":
        return BinaryClause(BinaryOperation.AND, self, Clause.parse(other))

    @abstractmethod
    def to_flux(self) -> str:
        pass

    @property
    @abstractmethod
    def precedence(self) -> int:
        pass


Clausable = Clause | bool


def bool_to_flux(val: bool) -> str:
    return "true" if val else "false"


@dataclass
class LiteralClause(Clause):
    literal: bool

    def to_flux(self) -> str:
        return bool_to_flux(self.literal)

    @property
    def precedence(self) -> int:
        return 0


class BinaryOperation(Enum):
    OR = "or"
    AND = "and"


@dataclass
class BinaryClause(Clause):
    operation: BinaryOperation
    left: "Clause"
    right: "Clause"

    def to_flux(self) -> str:
        return f"{self.ensure_precedence(self.left)} {self.operation.value} {self.ensure_precedence(self.right)}"

    @property
    def precedence(self) -> int:
        match self.operation:
            case BinaryOperation.AND:
                return 9
            case BinaryOperation.OR:
                return 10


class ComparisonOperation(Enum):
    EQ = "=="
    NE = "!="


@dataclass
class ComparisonClause(Clause):
    operation: ComparisonOperation
    name: str
    expression: Expression

    @property
    def precedence(self) -> int:
        return 7

    def to_flux(self) -> str:
        return (
            f"""r["{self.name}"] {self.operation.value} {self.expression.to_flux()}"""
        )


@dataclass
class Field:
    name: str

    def __eq__(self, other) -> ComparisonClause:
        return ComparisonClause(
            ComparisonOperation.EQ, self.name, parse_expression(other)
        )

    def __ne__(self, other) -> ComparisonClause:
        return ComparisonClause(
            ComparisonOperation.NE, self.name, parse_expression(other)
        )


class Row:
    def __getattr__(self, name):
        return Field(name)

    def __getitem__(self, name):
        return Field(name)


@dataclass
class Filter:
    clause: Clause

    def to_flux(self) -> str:
        return dedent(f"""filter(fn: (r) => {self.clause.to_flux()})""")


@dataclass
class Pivot(PipedFunction):
    row_key: list[str]
    column_key: list[str]
    value_column: str


@dataclass
class AggregateWindow(PipedFunction):
    every: timedelta
    fn: "WindowOperation"
    create_empty: bool


@dataclass
class Drop(PipedFunction):
    columns: list[str]


@dataclass
class Keep(PipedFunction):
    columns: list[str]


@dataclass
class Map:
    fn: str

    def to_flux(self) -> str:
        return f"map(fn: {self.fn})"


@dataclass
class Sum:
    column: str

    def to_flux(self) -> str:
        return f'sum(column: "{self.column}")'


@dataclass
class Mean:
    column: str

    def to_flux(self) -> str:
        return f'mean(column: "{self.column}")'


class Last:
    def to_flux(self) -> str:
        return "last()"


@dataclass
class Limit:
    n: int
    offset: int

    def to_flux(self) -> str:
        return f"limit(n: {self.n}, offset: {self.offset})"


class Order(Enum):
    DESC = "desc"
    ASC = "asc"


@dataclass
class Sort:
    columns: list[str]
    sort_order: Order

    def to_flux(self) -> str:
        columns_string = '", "'.join(self.columns)
        return f'sort(columns: ["{columns_string}"], desc: {"true" if self.sort_order == Order.DESC else "false"})'


@dataclass
class Literal:
    expression: str

    def to_flux(self) -> str:
        return self.expression


def from_bucket(bucket: str) -> From:
    return From(bucket)


@overload
def range(start: timedelta) -> RangeOffset:
    ...


@overload
def range(start: datetime, stop: datetime) -> Range:
    ...


def range(
    start: timedelta | datetime, stop: Optional[datetime] = None
) -> Range | RangeOffset:
    match start:
        case timedelta():
            return RangeOffset(start)
        case datetime() if stop is not None:
            return Range(start, stop)
        case _:
            raise Exception("invalid types")


ranged = range

FilterCallback = Callable[[Row], Clausable]


def filter(callback: FilterCallback) -> Filter:
    return Filter(Clause.parse(callback(Row())))


def pivot(row_key: list[str], column_key: list[str], value_column: str) -> Pivot:
    return Pivot(row_key, column_key, value_column)


def conform(value: dict[str, str]) -> FilterCallback:
    return lambda row: reduce(
        lambda acc, cur: acc & cur, (row[key] == value for key, value in value.items())
    )


T = TypeVar("T")


def any(test: Callable[[T], FilterCallback], values: list[T]) -> FilterCallback:
    return lambda row: reduce(
        lambda acc, cur: acc | cur, (Clause.parse(test(value)(row)) for value in values)
    )


some = any


class WindowOperation(Enum):
    MEAN = "mean"
    LAST = "last"
    SUM = "sum"


def aggregate_window(
    every: timedelta, fn: WindowOperation, create_empty: bool
) -> AggregateWindow:
    return AggregateWindow(every, fn, create_empty)


def drop(columns: list[str]) -> Drop:
    return Drop(columns)


def keep(columns: list[str]) -> Keep:
    return Keep(columns)


def map(function: str) -> Map:
    return Map(function)


def sum(column: str) -> Sum:
    return Sum(column)


def last() -> Last:
    return Last()


def mean(column: str) -> Mean:
    return Mean(column)


def limit(n: int, offset: int = 0):
    return Limit(n, offset)


def sort(columns: list[str] = ["_value"], sort_order: Order = Order.ASC):
    return Sort(columns, sort_order)


def literal(expression: str):
    return Literal(expression)
