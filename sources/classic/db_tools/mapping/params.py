from dataclasses import dataclass, field
from typing import Tuple, Optional, Dict, Union

from frozendict import frozendict

from .types import Target, Class


@dataclass(frozen=True)
class Relationship:
    target: Target

    @property
    def target_name(self):
        if isinstance(self.target, str):
            return self.target.lower()
        return self.target.__name__.lower()


@dataclass(frozen=True)
class Assign(Relationship):
    pass


@dataclass(frozen=True)
class Append(Relationship):
    pass


@dataclass(frozen=True)
class Add(Relationship):
    pass


@dataclass(frozen=True)
class Parameter:
    pass


@dataclass(frozen=True)
class Entity(Parameter):
    id: Union[str, Tuple[str, ...]]
    cls: Optional[Class] = None
    prefix: Optional[str] = None
    relationships: Dict[str, Relationship] = field(default_factory=frozendict)


@dataclass(frozen=True)
class Value(Parameter):
    reduce_none: bool = False
    cls: Optional[Class] = None
    prefix: str = None
    relationships: Dict[str, Relationship] = field(default_factory=frozendict)
