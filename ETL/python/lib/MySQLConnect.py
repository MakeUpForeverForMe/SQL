from pymysql import connect, cursors
from pprint import pprint


class MySQLConnect:
    def __init__(self, host='localhost', port=3306, user='root', passwd='', db='etl', charset='utf8'):
        # 建立连接
        self.conn = connect(host=host, port=port, db=db,
                            user=user, passwd=passwd, charset=charset)
        # 创建游标，操作设置为字典类型
        self.cur = self.conn.cursor(cursor=cursors.DictCursor)

    def __enter__(self):
        # 返回游标
        return self.cur

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 提交数据库并执行
        self.conn.commit()
        # 关闭游标
        self.cur.close()
        # 关闭数据库连接
        self.conn.close()


if __name__ == '__main__':
    with MySQLConnect(host='10.83.16.22', passwd='INikGPLun*8v') as con:
        con.execute('select * from STRUCT_BASE')
        data = con.fetchall
        pprint(data)
