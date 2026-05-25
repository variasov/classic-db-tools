# from classic.db_tools import Engine, ConnectionPool
#
# from .conftest import create_pool
#
#
# # # Autocommit
# # engine.query("SELECT 'rendered'").scalar()
# # engine.query("SELECT 'rendered'").scalar()
# #
# # # One conn for all queries
# # with engine.conn():
# #     engine.query("SELECT 'rendered'").scalar()
# #     engine.query("SELECT 'rendered'").scalar()
# #
# # # One transaction
# # with engine.transaction():
# #     engine.query("SELECT 'rendered'").scalar()
# #     engine.query("SELECT 'rendered'").scalar()
# #
# # # 2 transaction
# # with engine.transaction() as tx:
# #     engine.query("SELECT 'rendered'").scalar()
# #     engine.query("SELECT 'rendered'").scalar()
# #     tx.commit()
# #     engine.query("SELECT 'rendered'").scalar()
# #
# # # Nested transactions
# # with engine.transaction(commit=True):
# #     engine.query("SELECT 'rendered'").scalar()
# #     with engine.transaction():
# #         engine.query("SELECT 'rendered'").scalar()
# #
# #
# # class SomeUseCase:
# #     engine: Engine
# #
# #     @engine.conn()
# #     def run1(self):
# #         self.engine.query("SELECT 'rendered'").scalar()
# #         self.engine.query("SELECT 'rendered'").scalar()
# #
# #     @engine.transaction(level='READ COMMITED')
# #     def run2(self):
# #         self.engine.query("SELECT 'rendered'").scalar()
# #         self.engine.query("SELECT 'rendered'").scalar()
# #         self.engine.current.tx.commit()
# #         self.engine.query("SELECT 'rendered'").scalar()
# #
# #     @engine.transaction(level='SERIALIZABLE')
# #     def run3(self):
# #         self.engine.query("SELECT 'rendered'").scalar()
# #         self.engine.query("SELECT 'rendered'").scalar()
# #         self.engine.current.tx.commit()
# #         self.run2()
# #
# # class Repo:
# #
# #     def some_method(self):
# #         self.engine.query("SELECT 'rendered'").scalar()
# #
# #
# # engine = Engine()
# # repo = Repo(engine)
# # repo.some_method = engine.conn(repo.some_method)
# # repo.some_method = engine.transaction(repo.some_method)
#
#
# def test_engine_commits(conn_pool: ConnectionPool) -> None:
#     engine = Engine(conn_pool)
#     engine.query(
#         'DROP TABLE IF EXISTS example'
#     ).execute()
#     engine.query(
#         'CREATE TABLE example(a int, b int)'
#     ).execute()
#     engine.query(
#         'INSERT INTO example(a, b) '
#         'VALUES (1, 2), (2, 3), (3, 4)'
#     ).execute()
#
#     assert engine.query(
#         'SELECT * FROM example'
#     ).all() == [(1, 2), (2, 3), (3, 4)]
#
#     engine.query(
#         'DROP TABLE example'
#     ).execute()
#
#
# def test_engine_with_autocommit() -> None:
#     conn_pool = create_pool(dict(autocommit=True), dict(limit=1))
#     engine = Engine(conn_pool)
#     with engine.conn():
#         engine.query(
#             'DROP TABLE IF EXISTS example'
#         ).execute()
#         engine.query(
#             'CREATE TABLE example(a int, b int)'
#         ).execute()
#         engine.query(
#             'INSERT INTO example(a, b) '
#             'VALUES (1, 2), (2, 3), (3, 4)'
#         ).execute()
#
#     assert engine.query(
#         'SELECT * FROM example'
#     ).all() == [(1, 2), (2, 3), (3, 4)]
#
#     engine.query('DROP TABLE example').execute()
#
#
# def test_engine_with_autocommit_and_tx() -> None:
#     conn_pool = create_pool(dict(autocommit=True), dict(limit=1))
#     engine = Engine(psycopg.connect, '')
#
#     with engine.conn():
#         with engine.transaction():
#             engine.query('DROP TABLE IF EXISTS example').execute()
#             engine.query('CREATE TABLE example(a int, b int)').execute()
#             engine.query(
#                 'INSERT INTO example(a, b) '
#                 'VALUES (1, 2), (2, 3), (3, 4)'
#             ).execute()
#
#         with engine.transaction(commit=False):
#             assert engine.query(
#                 'SELECT * FROM example'
#             ).all() == [(1, 2), (2, 3), (3, 4)]
#
#         with engine.transaction():
#             engine.query('DROP TABLE example').execute()
#
#
# def test_engine_with_autocommit_and_tx_commit_false() -> None:
#     conn_pool = create_pool(dict(autocommit=True), dict(limit=1))
#     engine = Engine(conn_pool)
#
#     with engine.conn():
#         engine.query(
#             'DROP TABLE IF EXISTS example'
#         ).execute()
#         engine.query(
#             'CREATE TABLE example(a int, b int)'
#         ).execute()
#         with engine.transaction(commit=False):
#             engine.query(
#                 'INSERT INTO example(a, b) '
#                 'VALUES (1, 2), (2, 3), (3, 4)'
#             ).execute()
#
#         with engine.transaction(commit=False):
#             assert engine.query(
#                 'SELECT * FROM example'
#             ).all() == []
#
#         @engine.transaction(commit=False)
#         def do_something():
#             engine.query(
#                 'INSERT INTO example(a, b) '
#                 'VALUES (1, 2), (2, 3), (3, 4)'
#             ).execute()
#
#         do_something()
#
#         with engine.transaction():
#             engine.query(
#                 'DROP TABLE example'
#             ).execute()
#
#
# def test_engine_with_autocommit_and_tx_commit_false_() -> None:
#     conn_pool = create_pool(dict(autocommit=True), dict(limit=1))
#     engine = Engine(conn_pool)
#
#     engine.query(
#         'DROP TABLE IF EXISTS example'
#     ).execute()
#     engine.query(
#         'CREATE TABLE example(a int, b int)'
#     ).execute()
#     with engine.transaction(commit=False):
#         engine.query(
#             'INSERT INTO example(a, b) '
#             'VALUES (1, 2), (2, 3), (3, 4)'
#         ).execute()
#
#     with engine.transaction(commit=False):
#         assert engine.query(
#             'SELECT * FROM example'
#         ).all() == []
#
#     with engine.transaction():
#         engine.query(
#             'DROP TABLE example'
#         ).execute()
