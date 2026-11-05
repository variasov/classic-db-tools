from jinja2.ext import Extension
from jinja2.lexer import Token


class AutoBind(Extension):
    """
    Расширение для Jinja, автоматически добавляющее
    фильтр bind к каждой переменной в шаблоне.
    """

    def extract_param_name(self, tokens):
        name = ''
        for token in tokens:
            if token.test('variable_begin'):
                continue
            elif token.test('name'):
                name += token.value
            elif token.test('dot'):
                name += token.value
            else:
                break
        return name

    BRACKETS_OPEN = ('lparen', 'lbracket', 'lbrace')
    BRACKETS_CLOSE = ('rparen', 'rbracket', 'rbrace')

    def last_filter(self, tokens):
        """
        Имя последнего фильтра верхнего уровня (после последнего pipe
        на нулевой вложенности скобок) или None. После имени фильтра
        допустим только вызов с аргументами в скобках.
        """
        depth = 0
        last_pipe = None
        for idx, token in enumerate(tokens):
            if token.type in self.BRACKETS_OPEN:
                depth += 1
            elif token.type in self.BRACKETS_CLOSE:
                depth -= 1
            elif depth == 0 and token.test('pipe'):
                last_pipe = idx
        if last_pipe is None or last_pipe + 1 >= len(tokens):
            return None
        name = tokens[last_pipe + 1]
        if not name.test('name'):
            return None
        rest = tokens[last_pipe + 2:]
        if rest and not rest[0].test('lparen'):
            return None
        return name.value

    def filter_stream(self, stream):
        """
        We convert
        {{ some.variable | filter1 | filter 2 }}
            to
        {{ ( some.variable | filter1 | filter 2 ) | bind }}

        ... for all variable declarations in the template

        Note the extra ( and ). We want the | bind to apply to the entire value,
        not just the last value.
        The parentheses are mostly redundant, except in expressions like
        {{ '%' ~ myval ~ '%' }}

        This function is called by jinja2 immediately
        after the lexing stage, but before the parser is called.
        """
        while not stream.eos:
            token = next(stream)
            if token.test('variable_begin'):
                var_expr = []
                while not token.test('variable_end'):
                    var_expr.append(token)
                    token = next(stream)
                variable_end = token

                lineno = var_expr[-1].lineno

                # don't bind twice
                if self.last_filter(var_expr) not in (
                    'bind', 'inclause', 'sqlsafe',
                ):
                    param_name = self.extract_param_name(var_expr)

                    var_expr.insert(1, Token(lineno, 'lparen', '('))
                    var_expr.append(Token(lineno, 'rparen', ')'))
                    var_expr.append(Token(lineno, 'pipe', '|'))
                    var_expr.append(Token(lineno, 'name', 'autobind'))
                    var_expr.append(Token(lineno, 'lparen', '('))
                    var_expr.append(Token(lineno, 'string', param_name))
                    var_expr.append(Token(lineno, 'rparen', ')'))

                var_expr.append(variable_end)
                for token in var_expr:
                    yield token
            else:
                yield token
