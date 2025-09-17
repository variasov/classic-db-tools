from jinja2.ext import Extension
from jinja2.lexer import Token


class AutoBind(Extension):

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
        if not name:
            name = 'bind#0'
        return name

    def filter_stream(self, stream):
        """
        We convert
        {{ some.variable | filter1 | filter 2}}
            to
        {{ ( some.variable | filter1 | filter 2 ) | bind}}

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

                last_token = var_expr[-1]
                lineno = last_token.lineno

                # don't bind twice
                if (
                        not last_token.test('name') or
                        last_token.value not in ('bind', 'inclause', 'sqlsafe')
                ):
                    param_name = self.extract_param_name(var_expr)

                    var_expr.insert(1, Token(lineno, 'lparen', '('))
                    var_expr.append(Token(lineno, 'rparen', ')'))
                    var_expr.append(Token(lineno, 'pipe', '|'))
                    var_expr.append(Token(lineno, 'name', 'bind'))
                    var_expr.append(Token(lineno, 'lparen', '('))
                    var_expr.append(Token(lineno, 'string', param_name))
                    var_expr.append(Token(lineno, 'rparen', ')'))

                var_expr.append(variable_end)
                for token in var_expr:
                    yield token
            else:
                yield token
