from classic.db_tools import Engine


def test_from_file(engine: Engine):
    assert engine.from_file('test_render.sql').scalar() == 'rendered'


def test_from_str(engine: Engine):
    assert engine.from_str("SELECT 'rendered'").scalar() == 'rendered'
