
from contextlib import contextmanager
from psycopg2.pool import ThreadedConnectionPool
from dataclasses import make_dataclass

def register_db_block(dsn):
    db_pool = ThreadedConnectionPool(minconn=2, maxconn=10, dsn=dsn)

    @contextmanager
    def db_block():#变量conn是建立好的连接对象
        conn = db_pool.getconn()# # 从池取得一个连接

        #最后，提交commit或回滚rollback所有操作。
        # 在打开游标时，事务自动开启
        # 当遇到某些例外情况时，回滚操作，抛出异常
        # 正常执行完毕，最后提交事务
        try:
            with conn.cursor() as cur:
                yield RecordCursor(cur)
                conn.commit()
        except:
            conn.rollback()
            raise
        finally:
            db_pool.putconn(conn)
    # 将连接放回连接池
    return db_block

#执行SQL命令或者进行SQL查询都需要创建一个“游标cursor”对象，游标用来标记SQL执行或查询的位置。
#执行SQL命令则直接调用游标对象的execute方法，SQL语句则作为字符串传给execute方法。
class RecordCursor:
    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, query, vars=None):
        self._cursor.execute(query, vars)

    def __iter__(self):
        field_names = [d[0] for d in self._cursor.description]
        self._dataclass = make_dataclass("Rec", field_names)
        return self

    def __next__(self):

        record = self._cursor.__next__()
        record = self._dataclass(*record)

        return record

    def fetch_first(self):
        '''读取第一条数据'''

        try:
            return iter(self).__next__()
        except StopIteration:
            return None
