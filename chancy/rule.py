"""
Utilities for creating dynamic rules that can be used when configuring the
conditions of a Plugin.
"""

from typing import Any
from psycopg import sql


class SQLAble:
    def to_sql(self) -> sql.Composable:
        raise NotImplementedError


class Rule(SQLAble):
    def __init__(self, field: str):
        self.field = field

    def __eq__(self, other: Any) -> "Condition":
        return Condition(self.to_sql(), "=", other)

    def __ne__(self, other: Any) -> "Condition":
        return Condition(self.to_sql(), "!=", other)

    def __lt__(self, other: Any) -> "Condition":
        return Condition(self.to_sql(), "<", other)

    def __le__(self, other: Any) -> "Condition":
        return Condition(self.to_sql(), "<=", other)

    def __gt__(self, other: Any) -> "Condition":
        return Condition(self.to_sql(), ">", other)

    def __ge__(self, other: Any) -> "Condition":
        return Condition(self.to_sql(), ">=", other)

    def __or__(self, other: "Condition") -> "OrCondition":
        return OrCondition(self, other)

    def __and__(self, other: "Condition") -> "AndCondition":
        return AndCondition(self, other)

    def contains(self, value: str) -> "Condition":
        """
        String contains a lowercase string.
        """
        return Condition(self.to_sql(), "ILIKE", f"%{value}%")

    def to_sql(self) -> sql.Composable:
        return sql.Identifier(self.field)


class Condition(SQLAble):
    def __init__(self, field: sql.Composable, op: str, value: Any):
        self.field = field
        self.op = op
        self.value = value

    def __or__(self, other: "Condition") -> "OrCondition":
        return OrCondition(self, other)

    def __and__(self, other: "Condition") -> "AndCondition":
        return AndCondition(self, other)

    def to_sql(self) -> sql.Composable:
        return sql.SQL("{field} {op} {value}").format(
            field=self.field,
            op=sql.SQL(self.op),
            value=sql.Literal(self.value),
        )


class OrCondition(SQLAble):
    def __init__(self, left: SQLAble, right: SQLAble):
        self.left = left
        self.right = right

    def __or__(self, other: SQLAble) -> "OrCondition":
        return OrCondition(self, other)

    def __and__(self, other: SQLAble) -> "AndCondition":
        return AndCondition(self, other)

    def to_sql(self) -> sql.Composable:
        return sql.SQL("({left}) OR ({right})").format(
            left=self.left.to_sql(), right=self.right.to_sql()
        )


class AndCondition(SQLAble):
    def __init__(self, left: SQLAble, right: SQLAble):
        self.left = left
        self.right = right

    def __or__(self, other: Condition) -> OrCondition:
        return OrCondition(self, other)

    def __and__(self, other: Condition) -> "AndCondition":
        return AndCondition(self, other)

    def to_sql(self) -> sql.Composable:
        return sql.SQL("({left}) AND ({right})").format(
            left=self.left.to_sql(), right=self.right.to_sql()
        )


class JobRules:
    """
    A collection of rules that can be used to filter the main job table.
    """
    
    class All(Rule):
        def __init__(self):
            super().__init__("all")
            
        def to_sql(self) -> sql.Composable:
            return sql.SQL("TRUE")

    class Age(Rule):
        def __init__(self):
            super().__init__("age")

        def to_sql(self) -> sql.Composable:
            return sql.SQL("EXTRACT(EPOCH FROM (NOW() - created_at))")

    class Queue(Rule):
        def __init__(self):
            super().__init__("queue")

    class Job(Rule):
        def __init__(self):
            super().__init__("func")

    class State(Rule):
        def __init__(self):
            super().__init__("state")

    class CreatedAt(Rule):
        def __init__(self):
            super().__init__("created_at")

    class ScheduledAt(Rule):
        def __init__(self):
            super().__init__("scheduled_at")

    class ID(Rule):
        def __init__(self):
            super().__init__("id")
