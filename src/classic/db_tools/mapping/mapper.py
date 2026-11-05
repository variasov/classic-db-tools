import inspect
import keyword
import re
import threading
from collections import defaultdict
from typing import (
    Any,
    NamedTuple,
    cast,
)

from classic.db_tools.dbapi import Cursor

from .params import (
    Add,
    Append,
    Assign,
    Entity,
    Mapping,
    Parameter,
    Relationship,
    Value,
)
from .types import Accessor, MapperFunc

# Aliases for readability
Prefix = str
Field = str
Column = str

# Tuple positions for fields_to_columns
INDEX = 0
COLUMN = 1

# Число частей имени колонки prefix__field
COLUMN_PARTS = 2


class Mapper:
    def __init__(self, mapping: Mapping) -> None:
        self._cache: dict[
            tuple[str, Mapping, tuple[str, ...]],
            MapperFunc[Any],
        ] = {}
        self._lock = threading.RLock()
        self._mapping = mapping
        self._compile_mapper_func = compile_mapper_func

    def func_for_cursor(
        self,
        cursor: Cursor[Any],
        mapping: Mapping,
        result: str,
    ) -> MapperFunc[Any]:
        columns = tuple(column[0] for column in cursor.description)
        key = (result, mapping or self._mapping, columns)

        with self._lock:
            mapper = self._cache.get(key)
            if not mapper:
                mapper = self._compile_mapper_func(*key)
                self._cache[key] = mapper

        return mapper


class ResultParam(NamedTuple):
    prefix: Prefix
    param: Parameter


class FactoryCallPlan(NamedTuple):
    """План вызова фабрики: какие поля подать позиционно, какие по имени.

    Позиционная передача заметно дешевле kwargs на каждом вызове,
    поэтому поля, идущие подряд с начала сигнатуры фабрики, подаются
    позиционно, остальные — по имени.
    """

    positional: tuple[Field, ...]
    keyword: tuple[Field, ...]


def plan_factory_call(
    param: Parameter,
    fields: tuple[Field, ...],
    ctor_fields: frozenset[Field] = frozenset(),
) -> FactoryCallPlan:
    """Строит план вызова фабрики по её сигнатуре.

    Откатывается к полной kwargs-передаче, когда сигнатура недоступна
    (dict-фабрики, C-функции без сигнатуры) или поле не позиционное.

    ctor_fields — поля, поставляемые не из колонок строки, а из собранных
    объектов-отношений (Assign, передаваемый аргументом конструктора).
    Они учитываются в порядке сигнатуры наравне с полями колонок.
    """
    factory = param.factory
    if isinstance(factory, type) and issubclass(factory, dict):
        return FactoryCallPlan((), fields)
    try:
        signature = inspect.signature(cast("Any", factory))
    except (TypeError, ValueError):
        return FactoryCallPlan((), fields)

    supplied = frozenset(fields) | ctor_fields
    named = {
        name
        for name, p in signature.parameters.items()
        if p.kind is not inspect.Parameter.VAR_KEYWORD
    }
    positional: list[Field] = []
    keyword_: list[Field] = []
    accept_positional = True
    for name, p in signature.parameters.items():
        if p.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            break
        if name in supplied:
            if accept_positional and p.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                positional.append(name)
            else:
                keyword_.append(name)
        else:
            # Пропуск параметра: последующие поля позиционно не подать.
            accept_positional = False
    # Поля вне сигнатуры (уйдут в **kw или дадут TypeError, как и раньше).
    keyword_.extend(f for f in fields if f not in named)
    return FactoryCallPlan(tuple(positional), tuple(keyword_))


def _ctor_assign_rels(
    param: Parameter,
    rels: dict[Field, Relationship],
) -> dict[Field, Relationship]:
    """Assign-отношения, которые можно передать аргументами конструктора.

    Отбираются отношения, чьё поле присутствует в сигнатуре фабрики:
    такой объект-значение собирается до вызова фабрики и подаётся в неё,
    что позволяет конструировать обязательные поля-VO и frozen-агрегаты.
    Остальные Assign применяются присваиванием после конструирования.
    """
    assigns = {f: r for f, r in rels.items() if isinstance(r, Assign)}
    if not assigns:
        return {}
    factory = param.factory
    if isinstance(factory, type) and issubclass(factory, dict):
        return {}
    try:
        signature = inspect.signature(cast("Any", factory))
    except (TypeError, ValueError):
        return {}
    params = {
        name
        for name, p in signature.parameters.items()
        if p.kind
        not in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        )
    }
    return {f: r for f, r in assigns.items() if f in params}


class Context:
    mapper: dict[Prefix, Parameter]
    rels: dict[Prefix, dict[Field, Relationship]]
    ctor_rels: dict[Prefix, dict[Field, Relationship]]
    result: ResultParam
    columns: tuple[Column, ...] | None
    fields_to_columns: dict[Prefix, dict[Field, tuple[int, Column]]]
    factory_plans: dict[Prefix, FactoryCallPlan]

    def __init__(
        self,
        result: str,
        mapping: Mapping,
        columns: tuple[Column, ...],
    ) -> None:
        self.mapper = {}
        self.rels = defaultdict(dict)
        self.result = ResultParam(result, mapping[result])
        self.columns = None
        self.fields_to_columns = defaultdict(dict)
        self.factory_plans = {}
        self.parse_columns(columns, mapping)
        self.ctor_rels = {
            prefix: _ctor_assign_rels(param, self.rels.get(prefix, {}))
            for prefix, param in self.mapper.items()
        }
        for prefix, fields in self.fields_to_columns.items():
            self.factory_plans[prefix] = plan_factory_call(
                self.mapper[prefix],
                tuple(fields),
                frozenset(self.ctor_rels.get(prefix, {})),
            )

    @staticmethod
    def accessor(param: Parameter) -> Accessor:
        # Только dict-подобные фабрики дают item-доступ; для функций
        # и прочих классов отношения применяются через атрибуты.
        if isinstance(param.factory, type) and issubclass(param.factory, dict):
            return "item"
        return "attr"

    def parse_columns(
        self,
        columns: tuple[Column, ...],
        mapping: Mapping,
    ) -> None:
        self.columns = columns
        for index_, column in enumerate(columns):
            parts = column.lower().split("__")
            if len(parts) == 1:
                # Колонка без префикса (COUNT(*) OVER() и т.п.):
                # в маппинге не участвует.
                continue
            if len(parts) != COLUMN_PARTS:
                raise ValueError(
                    f"Column {column} are not contains name of cls "
                    f"and name of field, concatenated with __"
                )
            prefix, field_name = parts

            param = mapping.get(prefix)
            if not param:
                raise ValueError(f"Prefix {prefix} not found in mapper")

            if prefix not in self.mapper:
                self.mapper[prefix] = param

                for prefix_, param_ in mapping.items():
                    for rel_field, rel in param_.relationships.items():
                        if (
                            rel.target == prefix
                            and rel not in self.rels[prefix]
                        ):
                            self.rels[prefix_][rel_field] = rel

            self.fields_to_columns[prefix][field_name] = index_, column


# Имена, занятые самим сгенерированным кодом
RESERVED = {
    "row",
    "rows",
    "row_",
    "obj",
    "last_obj",
    "obj_with_rels",
    "result",
    "set",
    "map",
    "id",
}


def safe_ident(name: str, used: set[str]) -> str:
    """Превращает префикс/поле в валидный и уникальный идентификатор."""
    ident = re.sub(r"\W", "_", name)
    if not ident or ident[0].isdigit():
        ident = "_" + ident
    if keyword.iskeyword(ident):
        ident += "_"
    base = ident
    i = 2
    while ident in used:
        ident = f"{base}_{i}"
        i += 1
    used.add(ident)
    return ident


def is_ident(name: str) -> bool:
    return name.isidentifier() and not keyword.iskeyword(name)


class StringCodegen:
    """Генератор исходного кода функции-маппера по контексту запроса.

    Фабрики прокидываются в глобальные имена модуля как f0..fN,
    что устраняет коллизии по __name__ и конфликты с builtins.
    """

    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self.used: set[str] = set(RESERVED)
        # префикс -> (имя map-функции, имя переменной объекта, имя idmap)
        self.names: dict[Prefix, tuple[str, str, str]] = {}
        for prefix in ctx.mapper:
            base = safe_ident(prefix, self.used)
            self.names[prefix] = (
                f"map_{base}",
                safe_ident(f"{prefix}_obj", self.used),
                safe_ident(f"{prefix}_map", self.used),
            )
        # имена дедуп-множеств для (prefix, rel_field)
        self.relset_names: dict[tuple[Prefix, Field], str] = {}
        for prefix, rels in ctx.rels.items():
            for field, rel in rels.items():
                if isinstance(rel, (Append, Add)):
                    self.relset_names[(prefix, field)] = safe_ident(
                        f"{prefix}_{field}_seen",
                        self.used,
                    )
        # имена переменных для Assign-отношений, подаваемых в конструктор
        self.relvar_names: dict[tuple[Prefix, Field], str] = {}
        for prefix, crels in ctx.ctor_rels.items():
            for field in crels:
                self.relvar_names[(prefix, field)] = safe_ident(
                    f"{prefix}_{field}_arg",
                    self.used,
                )
        # фабрики -> глобальные имена f0..fN
        self.factory_names: dict[int, str] = {}
        self.namespace: dict[str, Any] = {}
        for i, param in enumerate(ctx.mapper.values()):
            fname = f"f{i}"
            self.factory_names[id(param.factory)] = fname
            self.namespace[fname] = param.factory
        self.lines: list[str] = []

    def emit(self, indent: int, line: str) -> None:
        self.lines.append("    " * indent + line)

    def factory_ref(self, param: Parameter) -> str:
        return self.factory_names[id(param.factory)]

    def factory_call(self, prefix: Prefix, param: Parameter) -> str:
        plan = self.ctx.factory_plans[prefix]
        fields = self.ctx.fields_to_columns[prefix]
        ctor = self.ctx.ctor_rels.get(prefix, {})
        ref = self.factory_ref(param)
        args = [
            self.relvar_names[(prefix, f)]
            if f in ctor
            else f"row[{fields[f][INDEX]}]"
            for f in plan.positional
        ]
        kw = plan.keyword
        if all(is_ident(f) for f in kw):
            args += [
                f"{f}={self.relvar_names[(prefix, f)]}"
                if f in ctor
                else f"{f}=row[{fields[f][INDEX]}]"
                for f in kw
            ]
        elif kw:
            pairs = ", ".join(
                f"{f!r}: {self.relvar_names[(prefix, f)]}"
                if f in ctor
                else f"{f!r}: row[{fields[f][INDEX]}]"
                for f in kw
            )
            args.append(f"**{{{pairs}}}")
        return f"{ref}({', '.join(args)})"

    def render_ctor_args(self, prefix: Prefix, indent: int) -> None:
        """Собирает объекты-значения для аргументов конструктора."""
        for field, rel in self.ctx.ctor_rels.get(prefix, {}).items():
            right_mapfn, _, _ = self.names[rel.target]
            var = self.relvar_names[(prefix, field)]
            if isinstance(self.ctx.mapper[rel.target], Entity):
                self.emit(indent, f"{var}, _ = {right_mapfn}(row)")
            else:
                self.emit(indent, f"{var} = {right_mapfn}(row)")

    def render_none_check(self, columns: list[int]) -> str:
        parts = [f"row[{c}] is None" for c in columns]
        if len(parts) == 1:
            return parts[0]
        return "(" + " and ".join(parts) + ")"

    def render_rel(
        self,
        prefix: Prefix,
        field: Field,
        rel: Relationship,
        accessor: Accessor,
        indent: int,
    ) -> None:
        ctx = self.ctx
        right: Prefix = rel.target
        right_param = ctx.mapper[right]
        right_mapfn, right_var, _ = self.names[right]
        _, left_var, _ = self.names[prefix]

        if isinstance(rel, Assign):
            if field in self.ctx.ctor_rels.get(prefix, {}):
                # Отношение уже передано аргументом конструктора.
                return
            attr_form = accessor == "attr" and is_ident(field)
            if isinstance(right_param, Entity):
                self.emit(indent, f"{right_var}, _ = {right_mapfn}(row)")
                if attr_form:
                    self.emit(indent, f"{left_var}.{field} = {right_var}")
                elif accessor == "attr":
                    self.emit(
                        indent,
                        f"setattr({left_var}, {field!r}, {right_var})",
                    )
                else:
                    self.emit(indent, f"{left_var}[{field!r}] = {right_var}")
            elif attr_form:
                self.emit(indent, f"{left_var}.{field} = {right_mapfn}(row)")
            elif accessor == "attr":
                self.emit(
                    indent,
                    f"setattr({left_var}, {field!r}, {right_mapfn}(row))",
                )
            else:
                self.emit(
                    indent,
                    f"{left_var}[{field!r}] = {right_mapfn}(row)",
                )
            return

        # Append / Add
        method = "append" if isinstance(rel, Append) else "add"
        if accessor == "item":
            default = "[]" if isinstance(rel, Append) else "set()"
            self.emit(indent, f"if {field!r} not in {left_var}:")
            self.emit(indent + 1, f"{left_var}[{field!r}] = {default}")
            coll = f"{left_var}[{field!r}]"
        elif is_ident(field):
            coll = f"{left_var}.{field}"
        else:
            coll = f"getattr({left_var}, {field!r})"

        relset = self.relset_names[(prefix, field)]
        if isinstance(right_param, Entity):
            right_id = f"{right_var}_id"
            self.emit(indent, f"{right_var}, {right_id} = {right_mapfn}(row)")
            self.emit(
                indent,
                f"if {right_var} is not None and {right_id} not in {relset}:",
            )
            self.emit(indent + 1, f"{relset}.add({right_id})")
            self.emit(indent + 1, f"{coll}.{method}({right_var})")
        else:
            self.emit(indent, f"{right_var} = {right_mapfn}(row)")
            self.emit(indent, f"if {right_var} is not None:")
            self.emit(indent + 1, f"{coll}.{method}({right_var})")

    def render_entity(
        self,
        prefix: Prefix,
        entity: Entity,
        indent: int,
    ) -> None:
        ctx = self.ctx
        mapfn, var, idmap = self.names[prefix]
        fields = ctx.fields_to_columns[prefix]
        id_columns = [
            cols[INDEX] for f, cols in fields.items() if f in entity.id
        ]

        self.emit(indent, f"def {mapfn}(row):")
        i = indent + 1
        self.emit(i, f"if {self.render_none_check(id_columns)}:")
        self.emit(i + 1, "return (None, None)")
        if len(entity.id) == 1:
            id_col = fields[entity.id[0]][INDEX]
            self.emit(i, f"{var}_id = row[{id_col}]")
        else:
            cols = ", ".join(f"row[{fields[f][INDEX]}]" for f in entity.id)
            self.emit(i, f"{var}_id = ({cols},)")
        self.emit(i, f"obj_with_rels = {idmap}.get({var}_id)")
        self.emit(i, "if obj_with_rels is None:")
        self.render_ctor_args(prefix, i + 1)
        self.emit(i + 1, f"{var} = {self.factory_call(prefix, entity)}")
        rel_sets = [
            self.relset_names[(prefix, f_)]
            for f_, r_ in ctx.rels[prefix].items()
            if isinstance(r_, (Append, Add))
        ]
        for relset in rel_sets:
            self.emit(i + 1, f"{relset} = set()")
        stored = ", ".join([var, *rel_sets])
        self.emit(i + 1, f"{idmap}[{var}_id] = ({stored},)")
        self.emit(i, "else:")
        self.emit(i + 1, f"({stored},) = obj_with_rels")
        for field, rel in ctx.rels[prefix].items():
            self.render_rel(prefix, field, rel, ctx.accessor(entity), i)
        self.emit(i, f"return ({var}, {var}_id)")

    def render_value(
        self,
        prefix: Prefix,
        value: Value,
        indent: int,
    ) -> None:
        ctx = self.ctx
        mapfn, var, _ = self.names[prefix]
        fields = ctx.fields_to_columns[prefix]
        columns = [cols[INDEX] for cols in fields.values()]

        self.emit(indent, f"def {mapfn}(row):")
        i = indent + 1
        if value.reduce_none:
            self.emit(i, f"if {self.render_none_check(columns)}:")
            self.emit(i + 1, "return None")
        self.render_ctor_args(prefix, i)
        self.emit(i, f"{var} = {self.factory_call(prefix, value)}")
        for field, rel in ctx.rels[prefix].items():
            self.render_rel(prefix, field, rel, ctx.accessor(value), i)
        self.emit(i, f"return {var}")

    def render_cycle(self, indent: int) -> None:
        ctx = self.ctx
        mapfn, _, _ = self.names[ctx.result.prefix]
        self.emit(indent, "last_obj = None")
        self.emit(indent, "for row_ in rows:")
        if isinstance(ctx.result.param, Entity):
            self.emit(indent + 1, f"obj, _ = {mapfn}(row_)")
        else:
            self.emit(indent + 1, f"obj = {mapfn}(row_)")
        self.emit(indent + 1, "if last_obj is not obj:")
        self.emit(indent + 2, "if last_obj is not None:")
        self.emit(indent + 3, "yield last_obj")
        self.emit(indent + 2, "last_obj = obj")
        self.emit(indent, "if last_obj is not None:")
        self.emit(indent + 1, "yield last_obj")

    def build(self) -> str:
        ctx = self.ctx
        header = ", ".join(
            f"{self.factory_ref(param)}="
            f"{getattr(param.factory, '__name__', '<factory>')}"
            for param in ctx.mapper.values()
        )
        self.emit(0, f"# factories: {header}")
        self.emit(0, "def mapper_func(rows):")
        for prefix, param in ctx.mapper.items():
            if isinstance(param, Entity):
                _, _, idmap = self.names[prefix]
                self.emit(1, f"{idmap} = {{}}")
        for prefix, param in ctx.mapper.items():
            if isinstance(param, Entity):
                self.render_entity(prefix, param, 1)
            elif isinstance(param, Value):
                self.render_value(prefix, param, 1)
            else:
                raise NotImplementedError
            self.emit(0, "")
        self.render_cycle(1)
        return "\n".join(self.lines) + "\n"


def compile_mapper_func(
    result: str,
    mapper: Mapping,
    columns: tuple[Column, ...],
) -> MapperFunc[Any]:
    ctx = Context(result, mapper, columns)
    codegen = StringCodegen(ctx)
    source = codegen.build()

    code = compile(source, "<mapper>", "exec")
    exec(code, codegen.namespace)

    func: Any = codegen.namespace["mapper_func"]

    # Add source code to func for easy debugging
    func.sources = lambda: source

    return cast("MapperFunc[Any]", func)
