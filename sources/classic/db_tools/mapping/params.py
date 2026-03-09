from dataclasses import dataclass, field
import inspect
from typing import (
    Any, Type, Callable, Tuple, Dict,
    Union, get_args, get_origin, List, Set,
)

from frozendict import frozendict


Factory = Union[Type[Any], Callable[[Any, ...], Any]]


@dataclass(frozen=True, init=False)
class Relationship:
    target: str

    def __init__(self, target: str):
        object.__setattr__(self, 'target', target.lower())


@dataclass(frozen=True, init=False)
class Assign(Relationship):
    pass


@dataclass(frozen=True, init=False)
class Append(Relationship):
    pass


@dataclass(frozen=True, init=False)
class Add(Relationship):
    pass


@dataclass(frozen=True)
class Parameter:
    factory: Factory

    def _parse_relationships(self, rels: Dict[str, Relationship]) -> None:
        try:
            signature = inspect.signature(self.factory)
        except ValueError:
            object.__setattr__(self, 'relationships', frozendict(rels))
            return

        new_rels = {}
        for name_, sign_param in signature.parameters.items():
            # User parameters have priority
            if rel := rels.get(name_):
                new_rels[name_] = rel
                continue

            # Trying to create relationship automatically
            annot = sign_param.annotation
            origin = get_origin(annot)
            args = get_args(annot)
            if origin is None:
                new_rels[name_] = Assign(annot.__name__.lower())
            elif issubclass(origin, List):
                new_rels[name_] = Append(args[0].__name__.lower())
            elif issubclass(origin, Set):
                new_rels[name_] = Add(args[0].__name__.lower())

        object.__setattr__(self, 'relationships', frozendict(new_rels))


@dataclass(frozen=True, init=False)
class Entity(Parameter):
    id: Union[str, Tuple[str, ...]]
    relationships: Dict[str, Relationship] = field(default_factory=frozendict)

    def __init__(
        self,
        factory: Factory,
        id_: Union[str, Tuple[str, ...]],
        /,
        **relationships: Relationship,
    ) -> None:
        # Set cls
        object.__setattr__(self, 'factory', factory)

        # Set ID
        object.__setattr__(
            self, 'id',
            (id_.lower(),)
            if isinstance(id_, str) else
            tuple((_.lower() for _ in id_))
        )

        # Set relationships
        self._parse_relationships(relationships)


@dataclass(frozen=True, init=False)
class Value(Parameter):
    reduce_none: bool
    relationships: Dict[str, Relationship] = field(default_factory=frozendict)

    def __init__(
        self,
        factory: Factory,
        reduce_none: bool = True,
        /,
        **relationships: Relationship,
    ) -> None:
        # Set cls
        object.__setattr__(self, 'factory', factory)

        # Set ID
        object.__setattr__(self, 'reduce_none', reduce_none)

        # Set relationships
        self._parse_relationships(relationships)


@dataclass(frozen=True, init=False)
class Mapper:
    params: frozendict[str, Parameter] = field(default_factory=frozendict)

    def __init__(self, **params: Parameter) -> None:
        object.__setattr__(self, 'params', frozendict((
            (key.lower(), value)
            for key, value in params.items()
        )))
