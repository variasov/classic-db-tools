from typing import Any, cast

from classic.criteria import (
    And,
    CriteriaNode,
    Invert,
    Or,
    ReturnsFalse,
    ReturnsTrue,
    Xor,
)
from jinja2 import Environment


def traverse(
    criteria: CriteriaNode[Any],
    translators: Any,  # объект с макросами из Jinja
) -> str:
    """
    Транслирует дерево критериев в SQL-условие.

    Каждый вложенный узел и каждый лист оборачивается в скобки, поэтому
    приоритет операций всегда соответствует структуре дерева.
    """
    if isinstance(criteria, And):
        if not criteria.nested:
            return "1=1"
        return " AND ".join(
            f"({traverse(nested, translators)})" for nested in criteria.nested
        )
    if isinstance(criteria, Or):
        if not criteria.nested:
            return "1=0"
        return " OR ".join(
            f"({traverse(nested, translators)})" for nested in criteria.nested
        )
    if isinstance(criteria, Xor):
        # XOR есть не во всех СУБД (нет в PostgreSQL и SQLite),
        # поэтому раскрываем: (A AND NOT B) OR (NOT A AND B).
        # Каждое вхождение рендерится заново: для позиционных
        # paramstyle у каждого плейсхолдера должно быть своё значение.
        left = criteria.left
        right = criteria.right
        return (
            f"((({traverse(left, translators)}) AND NOT "
            f"({traverse(right, translators)})) OR "
            f"(NOT ({traverse(left, translators)}) AND "
            f"({traverse(right, translators)})))"
        )
    if isinstance(criteria, Invert):
        return f"NOT ({traverse(criteria.nested, translators)})"
    if isinstance(criteria, ReturnsTrue):
        return "1=1"
    if isinstance(criteria, ReturnsFalse):
        return "1=0"
    name = criteria.__class__.__name__
    macro = getattr(translators, name, None)
    if not callable(macro):
        raise LookupError(
            f'No macro "{name}" for criteria {name} in translators'
        )
    return cast("str", macro(criteria))


def contains(
    criteria: CriteriaNode[Any],
    *translators: Any,  # имена или объекты-макросы из Jinja
) -> bool:
    if isinstance(criteria, (And, Or)):
        return any(contains(nested, *translators) for nested in criteria.nested)
    if isinstance(criteria, Xor):
        return contains(criteria.left, *translators) or contains(
            criteria.right, *translators
        )
    if isinstance(criteria, Invert):
        return contains(criteria.nested, *translators)
    for translator in translators:
        name = translator if isinstance(translator, str) else translator.name
        if name == criteria.__class__.__name__:
            return True
    return False


def register_criteria_macro(env: Environment) -> None:
    # jinja2 типизирует globals узким объединением, поэтому приводим к Any
    globals_ = cast("dict[str, Any]", env.globals)
    globals_.update(
        {
            "traverse": traverse,
            "contains": contains,
        }
    )
