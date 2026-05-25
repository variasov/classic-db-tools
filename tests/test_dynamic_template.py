from classic.db_tools import Engine


def test_default_identifier_quote(engine: Engine, ddl):
    assert engine.dynamic_templates.identifier_quote_char == '"'
