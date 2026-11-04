from typing import Any

from jinja2 import Environment

from classic.criteria import (
    And, CriteriaNode, Invert, Or, ReturnsFalse, ReturnsTrue, Xor,
)


def traverse(criteria: CriteriaNode[Any], translators) -> str:
    """
    Транслирует дерево критериев в SQL-условие.

    Каждый вложенный узел и каждый лист оборачивается в скобки, поэтому
    приоритет операций всегда соответствует структуре дерева.
    """
    if isinstance(criteria, And):
        if not criteria.nested:
            return '1=1'
        return ' AND '.join(
            '({})'.format(traverse(nested, translators))
            for nested in criteria.nested
        )
    elif isinstance(criteria, Or):
        if not criteria.nested:
            return '1=0'
        return ' OR '.join(
            '({})'.format(traverse(nested, translators))
            for nested in criteria.nested
        )
    elif isinstance(criteria, Xor):
        # XOR есть не во всех СУБД (нет в PostgreSQL и SQLite),
        # поэтому раскрываем: (A AND NOT B) OR (NOT A AND B).
        left = traverse(criteria.left, translators)
        right = traverse(criteria.right, translators)
        return (
            '((({0}) AND NOT ({1})) OR (NOT ({0}) AND ({1})))'
        ).format(left, right)
    elif isinstance(criteria, Invert):
        return 'NOT ({})'.format(traverse(criteria.nested, translators))
    elif isinstance(criteria, ReturnsTrue):
        return '1=1'
    elif isinstance(criteria, ReturnsFalse):
        return '1=0'
    else:
        name = criteria.__class__.__name__
        macro = getattr(translators, name, None)
        if not callable(macro):
            raise LookupError(
                'No macro "{}" for criteria {} in translators'.format(
                    name, name,
                )
            )
        return macro(criteria)


def contains(criteria: CriteriaNode[Any], *translators) -> bool:
    if isinstance(criteria, (And, Or)):
        return any((
            contains(nested, *translators)
            for nested in criteria.nested
        ))
    elif isinstance(criteria, Xor):
        return (
            contains(criteria.left, *translators) or
            contains(criteria.right, *translators)
        )
    elif isinstance(criteria, Invert):
        return contains(criteria.nested, *translators)
    else:
        for translator in translators:
            if isinstance(translator, str):
                name = translator
            else:
                name = translator.name
            if name == criteria.__class__.__name__:
                return True
        return False


def register_criteria_macro(env: Environment):
    env.globals.update({
        'traverse': traverse,
        'contains': contains,
    })
