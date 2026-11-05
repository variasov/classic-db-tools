import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import (
    Any,
    TypeAlias,
    get_args,
    get_origin,
    get_type_hints,
)

from frozendict import frozendict

Factory: TypeAlias = Callable[..., Any]


@dataclass(frozen=True, init=False)
class Relationship:
    target: str

    def __init__(self, target: Any) -> None:
        if isinstance(target, str):
            name = target
        elif isinstance(target, type):
            name = target.__name__
        else:
            name = target.__class__.__name__
        object.__setattr__(self, "target", name.lower())


@dataclass(frozen=True, init=False)
class Assign(Relationship):
    """
    Указывает, что target необходимо присвоить полю объекта.
    """


@dataclass(frozen=True, init=False)
class Append(Relationship):
    """
    Указывает, что target необходимо добавить в список в поле объекта.
    """


@dataclass(frozen=True, init=False)
class Add(Relationship):
    """
    Указывает, что target необходимо добавить в множество в поле объекта.
    """


@dataclass(frozen=True)
class Parameter:
    factory: Factory
    relationships: dict[str, Relationship]

    def _parse_relationships(self, rels: dict[str, Relationship]) -> None:
        try:
            signature = inspect.signature(self.factory)
        except (TypeError, ValueError):
            object.__setattr__(self, "relationships", frozendict(rels))
            return

        hints = _type_hints(self.factory)

        new_rels: dict[str, Relationship] = {}
        for name_, sign_param in signature.parameters.items():
            # *args/**kw в отношения не превращаются
            if sign_param.kind in (
                inspect.Parameter.VAR_KEYWORD,
                inspect.Parameter.VAR_POSITIONAL,
            ):
                continue

            # User parameters have priority
            if rel := rels.get(name_):
                new_rels[name_] = rel
                continue

            # Trying to create relationship automatically
            annot = hints.get(name_, sign_param.annotation)
            if annot is None or annot is inspect.Parameter.empty:
                continue
            origin = get_origin(annot)
            args = get_args(annot)
            try:
                if origin is None:
                    if name := _annot_name(annot):
                        new_rels[name_] = Assign(name)
                elif issubclass(origin, list):
                    if args and (name := _annot_name(args[0])):
                        new_rels[name_] = Append(name)
                elif (
                    issubclass(origin, set)
                    and args
                    and (name := _annot_name(args[0]))
                ):
                    new_rels[name_] = Add(name)
            except TypeError:
                continue

        # Заданные вручную отношения для полей вне сигнатуры фабрики
        # сохраняются: они применяются после конструирования объекта
        # (например, к полям с default_factory у dataclass).
        for name_ in set(rels) - set(new_rels):
            new_rels[name_] = rels[name_]

        object.__setattr__(self, "relationships", frozendict(new_rels))


def _annot_name(annot: Any) -> str | None:
    """Имя типа из аннотации для авто-отношения.

    Строковая аннотация (from __future__ import annotations) сама
    является именем типа.
    """
    if isinstance(annot, str):
        return annot.lower()
    name = getattr(annot, "__name__", None)
    return name.lower() if name else None


def _get_hints(target: Any) -> dict[str, Any]:
    try:
        return get_type_hints(target)
    except (NameError, TypeError, AttributeError, SyntaxError):
        return {}


def _type_hints(factory: Factory) -> dict[str, Any]:
    """Разрешает строковые аннотации фабрики; при неудаче — пусто.

    Для классов пробует и аннотации класса (поля dataclass), и
    аннотации __init__: у dataclass __init__ синтезирован и его
    __globals__ не содержат пользовательских типов.
    """
    if inspect.isclass(factory):
        targets: tuple[Any, ...] = (factory.__init__, factory)
    else:
        targets = (factory,)
    hints: dict[str, Any] = {}
    for target in targets:
        hints.update(_get_hints(target))
    return hints


@dataclass(frozen=True, init=False)
class Entity(Parameter):
    """
    Принимает фабрику для объекта и одно или несколько названий поле объекта,
    из которых складывается id объекта.

    Применение Entity указывает мапперу, что объекты, возвращаемые фабрикой,
    имеют ID, складываемый из полей объекта, указанных во втором параметре.

    Такие объекты маппер будет сопоставлять при парсинге с помощью словаря,
    что приведет к тому, что на каждый ID будет встречаться только один объект
    в результате.
    """

    id: str | tuple[str, ...]

    def __init__(
        self,
        factory: Factory,
        id_: str | tuple[str, ...],
        /,
        **relationships: Relationship,
    ) -> None:
        # Set cls
        object.__setattr__(self, "factory", factory)

        # Set ID
        if isinstance(id_, str):
            id_ = (id_.lower(),)
        elif isinstance(id_, tuple):  # pyright: ignore[reportUnnecessaryIsInstance]
            # runtime-проверка сохранена: id_ может прийти не по аннотации
            id_ = tuple(_.lower() for _ in id_)

        object.__setattr__(self, "id", id_)

        # Set relationships
        self._parse_relationships(relationships)


@dataclass(frozen=True, init=False)
class Value(Parameter):
    """
    Принимает фабрику для объекта и bool-параметр, указывающий,
    может ли объект иметь None в каждом поле,
    или же вместо объекта с None следует вернуть None.

    Применение Valuee указывает мапперу, что объекты, возвращаемые фабрикой,
    не имеют никакого ID, и отличаются друг от друга всеми полями.

    Такие объекты маппер будет инстанцировать на каждую строку,
    не переиспользуя инстанцированные ранее объекты, что приведет к тому,
    что в результате запроса могут встречаться одинаковые объекты.
    """

    reduce_none: bool

    def __init__(
        self,
        factory: Factory,
        reduce_none: bool = True,
        /,
        **relationships: Relationship,
    ) -> None:
        # Set cls
        object.__setattr__(self, "factory", factory)

        # Set ID
        object.__setattr__(self, "reduce_none", reduce_none)

        # Set relationships
        self._parse_relationships(relationships)


Mapping: TypeAlias = frozendict[str, Parameter]


def create_mapping(**params: Parameter) -> Mapping:
    return frozendict(((key.lower(), value) for key, value in params.items()))
