from dataclasses import dataclass
import inspect
from typing import (
    List, Optional, Tuple, Dict, Set, Callable, Any,
    get_args, get_origin,
)

from frozendict import frozendict

from .params import Relationship, Value, Entity, Parameter, Append, Add, Assign
from .types import Class, Accessor, Target


@dataclass(frozen=True)
class Mapper:
    name: str
    cls: Class
    prefix: str
    relationships: Dict[str, Relationship]

    @staticmethod
    def for_param(param: Parameter):
        if isinstance(param, Entity):
            return EntityMapper
        if isinstance(param, Value):
            return ValueMapper
        else:
            raise NotImplemented

    @property
    def accessor(self) -> Accessor:
        return 'item' if issubclass(self.cls, dict) else 'attr'

    @property
    def id_name(self) -> str:
        return self.name + '_id'

    @property
    def id_map_name(self) -> str:
        return self.name + '_map'

    @property
    def func_name(self) -> str:
        return f'map_{self.name}'

    @staticmethod
    def resolve(params: Dict[Target, Parameter]) -> Dict[str, 'Mapper']:
        mappers = {}
        for target, param in params.items():
            if isinstance(target, str):
                mapper_cls = Mapper.for_param(param)
                name = target.lower()
                mapper = mapper_cls.from_parameter(param, None, name, params)

            elif inspect.isclass(target):
                mapper_cls = Mapper.for_param(param)
                name = target.__name__.lower()
                mapper = mapper_cls.from_parameter(param, target, name, params)
            else:
                raise NotImplemented

            mappers[name] = mapper

        return frozendict(mappers)

    @classmethod
    def _parse_relationships(
        cls,
        param: Parameter,
        factory: Callable[[Any, ...], Any],
        config,
    ):
        try:
            signature = inspect.signature(factory)
        except ValueError:
            return param.relationships

        relationships = dict(param.relationships)
        for name_, sign_param in signature.parameters.items():
            rel = param.relationships.get(name_)
            if rel:
                relationships[name_] = rel
                continue

            annot = sign_param.annotation
            origin = get_origin(annot)
            args = get_args(annot)
            if origin is None:
                if annot in config:
                    relationships[name_] = Assign(annot)
            elif issubclass(origin, List):
                relationships[name_] = Append(args[0])
            elif issubclass(origin, Set):
                relationships[name_] = Add(args[0])
        return relationships


@dataclass(frozen=True)
class EntityMapper(Mapper):
    id: Tuple[str, ...]

    @classmethod
    def from_parameter(
        cls,
        entity: Entity,
        cls_: Optional[Class],
        name: Optional[str],
        mapper: Dict[Target, Parameter],
    ) -> 'EntityMapper':
        factory = cls_ or entity.cls
        relationships = cls._parse_relationships(entity, factory, mapper)
        prefix = entity.prefix or name or factory.__name__
        name = name or factory.__name__
        id_ = (entity.id,) if isinstance(entity.id, str) else entity.id
        return cls(
            id=id_,
            cls=factory,
            name=name.lower(),
            prefix=prefix,
            relationships=frozendict(relationships),
        )


@dataclass(frozen=True)
class ValueMapper(Mapper):
    reduce_none: bool

    @classmethod
    def from_parameter(
        cls,
        value: Value,
        cls_: Optional[Class],
        name: Optional[str],
        mapper: Dict[Target, Parameter],
    ) -> 'ValueMapper':
        factory = cls_ or value.cls
        relationships = cls._parse_relationships(value, factory, mapper)
        prefix = value.prefix or name or factory.__name__
        name = name or factory.__name__
        return cls(
            reduce_none=value.reduce_none,
            cls=factory,
            name=name.lower(),
            prefix=prefix,
            relationships=frozendict(relationships),
        )
