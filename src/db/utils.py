from math import ceil
from typing import TypeVar, Sequence, Any, overload, Literal

import psycopg

T = TypeVar('T')


@overload
def execute_values(cur: psycopg.Cursor[T],
                   sql: str,
                   values: Sequence[Sequence[Any]],
                   cols_count: int = None,
                   template: str = None,
                   page_size=100,
                   fetch: Literal[False] = False) -> None: ...


@overload
def execute_values(cur: psycopg.Cursor[T],
                   sql: str,
                   values: Sequence[Sequence[Any]],
                   cols_count: int = None,
                   template: str = None,
                   page_size=100,
                   fetch: Literal[True] = True) -> list[T]: ...


@overload
def execute_values(cur: psycopg.Cursor[T],
                   sql: str,
                   values: Sequence[Sequence[Any]],
                   cols_count: int = None,
                   template: str = None,
                   page_size=100,
                   fetch: bool = False) -> list[T] | None: ...


def execute_values(cur: psycopg.Cursor[T],
                   sql: str,
                   values: Sequence[Sequence[Any]],
                   cols_count: int = None,
                   template: str = None,
                   page_size=100,
                   fetch: bool = False) -> list[T] | None:
    """
    Execute multiple values in a VALUES statement.
    Should work like execute_values in psycopg2

    Args:
        cur: Cursor object
        sql: The sql statement with a single "VALUES %s"
        cols_count: How many items each of the values has. Calculated from the first value if not given.
        values: List of values
        template: Template string to use for values. e.g. "(%s, %s, 10)"
        page_size: How many values to process per execute statement.
        fetch: Whether to fetch the results or not.
    """
    if not values:
        return [] if fetch else None

    batches = ceil(len(values) / page_size)
    prepare = batches > 3
    if template is None:
        if cols_count is None:
            cols_count = len(values[0])

        template = f'({",".join(["%s"] * cols_count)})'

    result: list[T] = []
    for batch in range(batches):
        batch_values = values[batch * page_size:batch * page_size + page_size]
        template_list = ','.join(template for _ in range(len(batch_values)))

        args = [val for group in batch_values for val in group]
        cur.execute(sql % template_list, args,
                    prepare=prepare)
        if fetch:
            result.extend(cur.fetchall())

    if fetch:
        return result

