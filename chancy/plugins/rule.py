import abc
from typing import Union

from psycopg import sql

OpT = Union["Conditional", "Operator"]


class SQLAble(abc.ABC):
    @abc.abstractmethod
    def to_sql(self) -> sql.Composed:
        """
        Convert this object to an SQL expression.
        """


class Operator(SQLAble, abc.ABC):
    def __init__(self, left: OpT, right: OpT):
        self.left = left
        self.right = right

    def __or__(self, other: OpT) -> "Or":
        return Or(self, other)

    def __add__(self, other: OpT) -> "And":
        return And(self, other)


class Or(Operator):
    def to_sql(self) -> sql.Composed:
        return sql.SQL("({left}) OR ({right})").format(
            left=self.left.to_sql(), right=self.right.to_sql()
        )


class And(Operator):
    def to_sql(self) -> sql.Composed:
        return sql.SQL("({left}) AND ({right})").format(
            left=self.left.to_sql(), right=self.right.to_sql()
        )


class Conditional(SQLAble, abc.ABC):
    """
    A conditional is a rule that can be applied to a pruner.
    """

    def __or__(self, other: OpT) -> Or:
        return Or(self, other)

    def __add__(self, other: OpT) -> And:
        return And(self, other)


class QueueRule(Conditional):
    """
    A conditional that matches jobs based on their queue.
    """

    def __init__(self, queue: str):
        self.queue = queue

    def to_sql(self) -> sql.Composed:
        return sql.SQL("queue = {queue}").format(queue=sql.Literal(self.queue))


class AgeRule(Conditional):
    """
    A conditional that matches jobs based on their age.
    """

    def __init__(self, age: int):
        self.age = age

    def to_sql(self) -> sql.Composed:
        return sql.SQL(
            "scheduled_at < NOW() - INTERVAL '{age} seconds'"
        ).format(age=sql.Literal(self.age))


class JobRule(Conditional):
    """
    A conditional that matches jobs based on their job name.
    """

    def __init__(self, job: str):
        self.job = job

    def to_sql(self) -> sql.Composed:
        return sql.SQL("payload->>'func' = {job}").format(
            job=sql.Literal(self.job)
        )


RuleT = Union[SQLAble, list[SQLAble]]
