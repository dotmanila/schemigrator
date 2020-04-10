#!/usr/bin/env python3

import mysql.connector
import pytest
import schemigrate
import conftest

opts, logger, schemigrator = conftest.init()
replclient = schemigrate.ReplicationClient(opts.src_dsn, opts.dst_dsn, opts.bucket, binlog_fil=None,
                                           binlog_pos=None, debug=True, pause_file=None, stop_file=None, 
                                           chunk_size=1000, replica_dsns=opts.replicas, max_lag=60, 
                                           checksum=True)

def test_sizeof_fmt():
    assert replclient.sizeof_fmt(1023) == '1023.0B'
    assert replclient.sizeof_fmt(1048576) == '1.0MiB'

def test_max_replica_lag(mysql_repl1_conn, mysql_repl2_conn):
    mysql_repl1_conn.query('START SLAVE')
    mysql_repl2_conn.query('START SLAVE')
    max_sbm, replica = replclient.max_replica_lag()
    assert max_sbm == 0
    mysql_repl1_conn.query('STOP SLAVE')
    mysql_repl2_conn.query('STOP SLAVE')
    max_sbm, replica = replclient.max_replica_lag()
    assert max_sbm == None
    mysql_repl2_conn.query('START SLAVE')
    max_sbm, replica = replclient.max_replica_lag()
    assert max_sbm == 0
    mysql_repl1_conn.query('START SLAVE')

def test_list_bucket_tables(monkeypatch):
    assert 'single_pk' in replclient.list_bucket_tables()
    monkeypatch.setattr(replclient, 'bucket', 'non_existent_db')
    assert replclient.list_bucket_tables() == False
    assert replclient.list_bucket_tables(from_source=True) == False

def test_get_table_primary_key(monkeypatch):
    assert replclient.get_table_primary_key('single_pk') == 'autonum'

    monkeypatch.setattr(replclient, 'bucket', 'non_existent_db')
    with pytest.raises(Exception) as excinfo:
        assert replclient.get_table_primary_key('single_pk')

    monkeypatch.setattr(replclient, 'bucket', 'multi_pk')
    with pytest.raises(Exception) as excinfo:
        assert replclient.get_table_primary_key('multi_pk')

def test_get_table_columns(monkeypatch):
    assert replclient.get_table_columns('single_pk') == True
    assert replclient.columns_str == {'single_pk': '`autonum`, `c`, `n`'}
    assert replclient.columns_arr == {'single_pk': ['autonum', 'c', 'n']}
    assert list(replclient.columns_dict['single_pk'][0].keys()).sort() == ['col', 'is_nullable', 'data_type'].sort()
    monkeypatch.setattr(replclient, 'bucket', 'non_existent_db')
    with pytest.raises(Exception) as excinfo:
        assert replclient.get_table_columns('non_existent_table')

def test_checkpoint_begin():
    assert isinstance(replclient.checkpoint_begin(), mysql.connector.cursor.MySQLCursorDict)

def test_halt_or_pause(tmpdir, monkeypatch):
    assert replclient.halt_or_pause() == True
    stop_file = tmpdir.join('schemigrator.stop')
    stop_file.write('x')
    monkeypatch.setattr(replclient, 'stop_file', str(stop_file.realpath()))
    assert replclient.halt_or_pause() == False
    monkeypatch.setattr(replclient, 'is_alive', False)
    assert replclient.halt_or_pause() == False

def test_setup_for_checksum(monkeypatch):
    monkeypatch.setitem(replclient.pkcols, 'single_pk', 'autonum')
    assert replclient.setup_for_checksum('single_pk') == True
    sql = ("COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) "
           "AS UNSIGNED)), 10, 16)), 0)") % ('`autonum`, CONVERT(`c` USING utf8mb4), `n`')
    assert replclient.row_checksum['single_pk'] == sql

def test_checkpoint_write(monkeypatch):
    cursor = mysql.connector.cursor.MySQLCursorDict(replclient.mysql_dst.conn)
    replclient.mysql_dst.query('CREATE DATABASE IF NOT EXISTS single_pk')
    replclient.mysql_dst.query('USE single_pk')
    replclient.mysql_dst.query(schemigrate.sql_schemigrator_binlog_status)

    with pytest.raises(mysql.connector.errors.IntegrityError) as excinfo:
        assert replclient.checkpoint_write(cursor)

    monkeypatch.setattr(replclient, 'checkpoint_next_binlog_fil', 'fff')
    monkeypatch.setattr(replclient, 'checkpoint_next_binlog_pos', 10000)
    assert replclient.checkpoint_write(cursor) == None
    replclient.mysql_dst.query('DROP DATABASE IF EXISTS single_pk')

def test_begin_apply_trx():
    assert replclient.begin_apply_trx() == None
    assert replclient.trx_open == True
    assert replclient.mysql_dst.conn.in_transaction == True
    assert replclient.mysql_dst.conn.rollback() == None

def test_insert(mysql_dst_conn, monkeypatch):
    cursor = mysql.connector.cursor.MySQLCursorDict(replclient.mysql_dst.conn)
    mysql_dst_conn.query('CREATE DATABASE IF NOT EXISTS single_pk')
    mysql_dst_conn.query('USE single_pk')
    mysql_dst_conn.query('DROP TABLE IF EXISTS single_pk')
    mysql_dst_conn.query(conftest.sql_single_pk)
    values = {'autonum': 1, 'c': 'a', 'n': 1}

    replclient.mysql_dst.query('USE single_pk')
    assert replclient.insert(cursor, 'single_pk', values) == None
    assert mysql_dst_conn.row('SELECT autonum, c, n FROM single_pk') == values
    mysql_dst_conn.query('DROP DATABASE IF EXISTS single_pk')






