#!/usr/bin/env python3

import mysql.connector
import pytest
import schemigrate
import conftest

opts, logger, schemigrator = conftest.init()


def test_setup_bootstrap(monkeypatch, mysql_dst_conn):
    with pytest.raises(KeyError) as excinfo:
        assert schemigrator.opts.dst_dsn['db']

    assert schemigrator.opts.src_dsn['db'] == 'single_pk'
    assert schemigrator.opts.bucket == 'single_pk'

    monkeypatch.setitem(schemigrator.opts.dst_dsn, 'db', schemigrator.opts.bucket)
    with pytest.raises(mysql.connector.errors.ProgrammingError) as excinfo:
        assert schemigrator.setup_bootstrap()
    assert 'Unknown database' in str(excinfo.value)

    sql = ("""SELECT SCHEMA_NAME AS t FROM INFORMATION_SCHEMA.SCHEMATA 
           WHERE SCHEMA_NAME = 'single_pk'""")
    
    monkeypatch.delitem(schemigrator.opts.dst_dsn, 'db', raising=False)

    assert schemigrator.setup_bootstrap() is None
    assert mysql_dst_conn.scalar(sql, 't') == 'single_pk'


def test_list_bucket_tables():
    assert schemigrator.list_bucket_tables() == ['single_pk']
    assert schemigrator.setup_metadata_tables(['single_pk']) is None


def test_is_table_copy_complete():
    assert schemigrator.is_table_copy_complete('non_existent_table') == 0
    assert schemigrator.is_table_copy_complete('single_pk') == 0


def test_list_incomplete_tables():
    assert schemigrator.list_incomplete_tables() == [{'tbl': 'single_pk', 'status': 0}]


def test_setup_metadata_tables(mysql_dst_conn):
    tables = schemigrator.list_bucket_tables()
    assert schemigrator.setup_metadata_tables(tables) is None

    sql = ("""SELECT TABLE_NAME AS t FROM INFORMATION_SCHEMA.TABLES 
           WHERE TABLE_SCHEMA = 'single_pk' AND TABLE_NAME = '%s'""")
    assert mysql_dst_conn.scalar(sql % 'schemigrator_checksums', 't') == 'schemigrator_checksums'
    assert mysql_dst_conn.scalar(sql % 'schemigrator_binlog_status', 't') == 'schemigrator_binlog_status'
    assert mysql_dst_conn.scalar(sql % 'schemigrator_checkpoint', 't') == 'schemigrator_checkpoint'


def test_create_dst_tables():
    tables = schemigrator.list_bucket_tables()
    assert schemigrator.create_dst_tables(tables) is None
    assert schemigrator.list_bucket_tables(from_source=False) == ['single_pk']


def test_get_binlog_checkpoint(mysql_dst_conn_bucket):
    assert schemigrator.get_binlog_checkpoint() == (None, None)
    sql = """INSERT INTO schemigrator_binlog_status (bucket, fil, pos) VALUES ('%s', '%s', %d)"""
    mysql_dst_conn_bucket.query(sql % ('single_pk', 'fil0', 4))
    mysql_dst_conn_bucket.query(sql % ('multi_pk', 'fil2', 4))

    assert schemigrator.get_binlog_checkpoint() == ('fil0', 4)
    mysql_dst_conn_bucket.query('TRUNCATE TABLE schemigrator_binlog_status')


def test_master_status(mysql_src_conn_bucket):
    status = mysql_src_conn_bucket.row('SHOW MASTER STATUS')
    assert schemigrator.master_status() == (status['File'], status['Position'])


def test_get_binlog_coords(mysql_dst_conn_bucket):
    assert schemigrator.get_binlog_checkpoint() == (None, None)
    sql = """INSERT INTO schemigrator_binlog_status (bucket, fil, pos) VALUES ('%s', '%s', %d)"""
    mysql_dst_conn_bucket.query(sql % ('single_pk', 'fil0', 4))
    mysql_dst_conn_bucket.query(sql % ('multi_pk', 'fil2', 4))

    assert schemigrator.get_binlog_checkpoint() == ('fil0', 4)
    mysql_dst_conn_bucket.query('TRUNCATE TABLE schemigrator_binlog_status')


def test_copy_tables():
    assert schemigrator.copy_tables([]) is True


