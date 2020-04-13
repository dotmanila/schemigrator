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
    assert max_sbm is not None
    mysql_repl1_conn.query('STOP SLAVE')
    mysql_repl2_conn.query('STOP SLAVE')
    max_sbm, replica = replclient.max_replica_lag()
    assert max_sbm == None
    mysql_repl2_conn.query('START SLAVE')
    max_sbm, replica = replclient.max_replica_lag()
    assert max_sbm > -1
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
    assert replclient.trx_open == True

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

def test_checkpoint_write(mysql_dst_bootstrap, mysql_dst_teardown, monkeypatch):
    mysql_dst_bootstrap()
    replclient.mysql_dst.query('USE single_pk')
    cursor = mysql.connector.cursor.MySQLCursorDict(replclient.mysql_dst.conn)

    with pytest.raises(mysql.connector.errors.IntegrityError) as excinfo:
        assert replclient.checkpoint_write(cursor)

    monkeypatch.setattr(replclient, 'checkpoint_next_binlog_fil', 'fff')
    monkeypatch.setattr(replclient, 'checkpoint_next_binlog_pos', 10000)
    assert replclient.checkpoint_write(cursor) == None
    if replclient.mysql_dst.conn.in_transaction:
        replclient.mysql_dst.conn.rollback()
    mysql_dst_teardown()

def test_begin_apply_trx():
    assert replclient.begin_apply_trx() == None
    assert replclient.trx_open == True
    assert replclient.mysql_dst.conn.in_transaction == True
    assert replclient.mysql_dst.conn.rollback() == None

def test_insert(mysql_dst_bootstrap, mysql_dst_teardown, mysql_dst_conn, monkeypatch):
    cursor = mysql.connector.cursor.MySQLCursorDict(replclient.mysql_dst.conn)
    mysql_dst_bootstrap()
    values = {'autonum': 1, 'c': 'a', 'n': 1}

    replclient.mysql_dst.query('USE single_pk')
    assert replclient.insert(cursor, 'single_pk', values) == None
    assert mysql_dst_conn.row('SELECT autonum, c, n FROM single_pk') == values
    mysql_dst_teardown()

def test_checksum_chunk(mysql_dst_bootstrap, mysql_dst_teardown, mysql_dst_conn, monkeypatch):
    cursor = mysql.connector.cursor.MySQLCursorDict(replclient.mysql_dst.conn)
    mysql_dst_bootstrap()
    values = {
        'chunk': 1, 'lower_boundary': 1, 'upper_boundary': 1000,
        'tbl': 'single_pk', 'master_cnt': 1000, 'master_crc': 'aabbcc'
    }
    assert replclient.checksum_chunk(cursor, values) == True
    monkeypatch.setattr(replclient, 'checksum', False)
    assert replclient.checksum_chunk(cursor, values) == True
    sql = 'SELECT * FROM schemigrator_checksums WHERE lower_boundary = 1 and upper_boundary = 1000'
    checksum = mysql_dst_conn.row(sql)
    assert checksum['master_crc'] == 'aabbcc'
    assert checksum['this_crc'] != 'aabbcc'
    mysql_dst_teardown()

def test_checkpoint_end(mysql_dst_bootstrap, mysql_dst_teardown, monkeypatch):
    monkeypatch.setattr(replclient, 'checkpoint_next_binlog_fil', 'aaa')
    monkeypatch.setattr(replclient, 'checkpoint_next_binlog_pos', 100)
    mysql_dst_bootstrap()
    replclient.log_event_metrics(start=True)
    cursor = replclient.checkpoint_begin()

    assert replclient.checkpoint_end(cursor, force=True, fil='aaa', pos=120) == None
    assert replclient.trx_open == False
    assert replclient.mysql_dst.conn.in_transaction == False
    mysql_dst_teardown()

def test_log_event_metrics():
    assert replclient.log_event_metrics(start=True) == True
    assert replclient.metrics['events'] == 0
    assert replclient.log_event_metrics(binlog_fil='aaa', binlog_pos=120) == True



