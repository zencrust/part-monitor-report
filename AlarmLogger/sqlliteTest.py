
import sqlite3
from sqlite3 import Error
import logging
import datetime
from contextlib import contextmanager


table_def_file = '''CREATE TABLE IF NOT EXISTS data (
    id        INTEGER  PRIMARY KEY,
    name      text  NOT NULL,
    start_time DATETIME NOT NULL,
    duration  REAL     NOT NULL,
    comments  text
);'''


write_def = '''INSERT INTO data(name,start_time,duration, comments)
            VALUES(?,?,?,?);'''

read_data = '''SELECT
 * FROM data
 ORDER BY start_time DESC
 LIMIT ? OFFSET ?;'''


@contextmanager
def LogFile(file_path):
    try:
        conn = sqlite3.connect(file_path)
        _create_table(conn, table_def_file)
        print(sqlite3.version)
        yield conn
    except Error as e:
        logging.critical(e)
    finally:
        conn.close()


def _create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        logging.critical(e)


def write(conn, data):
    cur = conn.cursor()
    cur.execute(write_def, data)
    conn.commit()
    return cur.lastrowid


def read(conn, cond):
    cur = conn.cursor()
    cur.execute(read_data, cond)
    x = cur.fetchall()
    return x


if __name__ == '__main__':
    with LogFile("pythonsqlite.db") as f:
        x = (datetime.datetime.now(),)
        # data1 = ("Station1", datetime.datetime.now(), 56, "aaa")
        # data2 = ("Station2", datetime.datetime.now(), 56, "aaa")
        # write(f, data1)
        # write(f, data2)
        d = read(f, (2,3))
        print(d)
