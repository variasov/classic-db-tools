validators = {}


def validator(conn_type, cls=None):
    def validator(cls):
        validators[conn_type] = cls
        return cls

    if cls:
        validators[conn_type] = cls

    return validator


class ConnectionValidator:
    def validate(self, conn):
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
        except Exception:
            return False
        return True

    def before_release(self, conn):
        try:
            conn.rollback()
        except Exception:
            return False
        return self.validate(conn)


try:
    from psycopg2 import extensions as psycopg2_ext
    from psycopg2.extensions import connection as psycopg2_conn
except ImportError:
    pass
else:

    @validator(psycopg2_conn)
    class Psycopg2ConnectionValidator(ConnectionValidator):
        def validate(self, conn):
            try:
                cur = conn.cursor()
                cur.execute('SELECT 1')
            except Exception:
                return False
            return True

        def before_release(self, conn):
            if conn.closed:
                return False
            status = conn.info.transaction_status
            if status == psycopg2_ext.TRANSACTION_STATUS_UNKNOWN:
                return False
            elif status != psycopg2_ext.TRANSACTION_STATUS_IDLE:
                conn.rollback()
                return True
            return True


try:
    from psycopg.connection import Connection as psycopg3_conn
    from psycopg.pq import TransactionStatus as Psycopg3TransactionStatus
except ImportError as e:
    pass
else:

    @validator(psycopg3_conn)
    class Psycopg3ConnectionValidator(ConnectionValidator):
        def validate(self, conn):
            try:
                cur = conn.cursor()
                cur.execute('SELECT 1')
            except Exception:
                return False
            return True

        def before_release(self, conn):
            if conn.closed:
                return False
            status = conn.info.transaction_status
            if status == Psycopg3TransactionStatus.UNKNOWN:
                return False
            elif status != Psycopg3TransactionStatus.IDLE:
                conn.rollback()
                return True
            return True


class MysqlConnectionValidator(ConnectionValidator):
    def validate(self, conn):
        try:
            conn.ping()
        except Exception:
            return False
        return True


try:
    from pymysql.connections import Connection as pymysql_conn
except ImportError:
    pass
else:
    validator(pymysql_conn, MysqlConnectionValidator)


try:
    from MySQLdb.connections import Connection as mysqldb_conn
except ImportError:
    pass
else:
    validator(mysqldb_conn, MysqlConnectionValidator)


try:
    from pymssql import Connection as pymssql_conn
except ImportError:
    pass
else:
    @validator(pymssql_conn)
    class PyMSSQLConnectionValidator(ConnectionValidator):

        def validate(self, conn):
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 AS [1]")
                cursor.fetchone()
                cursor.close()
            except Exception:
                return False
            return True


try:
    from oracledb import Connection as oracledb_conn
except ImportError:
    pass
else:
    @validator(oracledb_conn)
    class OracleConnectionValidator(ConnectionValidator):

        def validate(self, conn):
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                cursor.fetchone()
                cursor.close()
            except Exception:
                return False
            return True


try:
    from cx_Oracle import Connection as cx_oracle_conn
except ImportError:
    pass
else:
    @validator(cx_oracle_conn)
    class OracleConnectionValidator(ConnectionValidator):

        def validate(self, conn):
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                cursor.fetchone()
                cursor.close()
            except Exception:
                return False
            return True
