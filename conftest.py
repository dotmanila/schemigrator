#!/usr/bin/env python3

import logging
import mysql.connector
import pytest
import schemigrate

opts = None
logger = None

sql_single_pk = ("""
CREATE TABLE IF NOT EXISTS single_pk (
    autonum INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    c CHAR(1) NOT NULL,
    n INT NOT NULL,
    KEY (n)
) ENGINE=INNODB
""")

sql_multi_pk = ("""
CREATE TABLE IF NOT EXISTS multi_pk (
    autonum INT UNSIGNED NOT NULL AUTO_INCREMENT,
    c CHAR(1) NOT NULL,
    n INT NOT NULL,
    PRIMARY KEY (autonum, c),
    KEY (n)
) ENGINE=INNODB
""")

mysql_params_global = {'host': '127.0.0.1', 
                       'user': 'schemigrator', 
                       'passwd': 'schemigrator'}


class MySQLControl(object):
    def __init__(self, params):
        self.conn = mysql.connector.connect(**params, auth_plugin='mysql_native_password', autocommit=True, use_pure=True)

    def query(self, sql, args=None):
        results = None
        cursor = mysql.connector.cursor.MySQLCursorDict(self.conn)
        cursor.execute(sql, args)
        
        if cursor.with_rows:
            results = cursor.fetchall()

        self.rowcount = cursor.rowcount

        cursor.close()
        return results

    def scalar(self, sql, column):
        results = self.query(sql)
        if len(results) == 0:
            return None
        return results[0][column]

def mysql_conn(port, db=None):
    params = mysql_params_global

    if db is not None:
        params['db'] = db
    mysql_ctl_src = MySQLControl(params)

    return mysql_ctl_src

def init():
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.NullHandler())

    opts = schemigrate.SchemigrateOptionParser('test')
    opts.dst_dsn = dict(mysql_params_global)
    opts.dst_dsn['port'] = 13301

    opts.src_dsn = dict(mysql_params_global)
    opts.src_dsn['port'] = 13300
    opts.src_dsn['db'] = 'single_pk'

    opts.replicas_dsn = []
    for port in [13302, 13303]:
        params = dict(mysql_params_global)
        params['port'] = port
        opts.replicas_dsn.append(params)

    opts.bucket = 'single_pk'
    opts.chunk_size_repl = 10
    opts.chunk_size_copy = 100
    opts.max_lag = 10
    schemigrator = schemigrate.Schemigrate(opts, logger)

    params = dict(mysql_params_global)
    params['port'] = 13300
    mysql_ctl_src = MySQLControl(params)
    mysql_ctl_src.query('CREATE DATABASE IF NOT EXISTS single_pk')
    mysql_ctl_src.query('CREATE DATABASE IF NOT EXISTS multi_pk')
    mysql_ctl_src.query('USE single_pk')
    mysql_ctl_src.query(sql_single_pk)
    mysql_ctl_src.query('USE multi_pk')
    mysql_ctl_src.query(sql_multi_pk)
    mysql_ctl_src = None

    params['port'] = 13301
    params['db'] = None
    mysql_ctl_dst = MySQLControl(params)
    mysql_ctl_dst.query('DROP DATABASE IF EXISTS single_pk')
    mysql_ctl_dst.query('DROP DATABASE IF EXISTS multi_pk')
    mysql_ctl_dst = None

    return opts, logger, schemigrator

@pytest.fixture
def mysql_src_conn():
    return mysql_conn(13300, db=None)

@pytest.fixture
def mysql_dst_conn():
    return mysql_conn(13301, db=None)


if __name__ == "__main__":
    opts, logger, schemigrator = init()
    print(schemigrator.opts.src_dsn)