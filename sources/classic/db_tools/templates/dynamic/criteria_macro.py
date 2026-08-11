from typing import Any

from jinja2 import Environment

from classic.criteria import CriteriaNode, And, Or, Xor, Invert


def traverse(criteria: CriteriaNode[Any], translators) -> str:
    if isinstance(criteria, And):
        return ' AND '.join((
            traverse(nested, translators)
            for nested in criteria.nested
        ))
    elif isinstance(criteria, Or):
        return ' OR '.join((
            traverse(nested, translators)
            for nested in criteria.nested
        ))
    elif isinstance(criteria, Xor):
        return ' XOR '.join((
            traverse(criteria.left, translators),
            traverse(criteria.right, translators),
        ))
    elif isinstance(criteria, Invert):
        return 'NOT {}'.format(traverse(criteria.nested, translators))
    else:
        macro = translators.__dict__[criteria.__class__.__name__]
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
