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
replclient.connect_target()
replcursor = mysql.connector.cursor.MySQLCursorDict(replclient.mysql_applier.conn)


def test_sizeof_fmt():
    assert replclient.sizeof_fmt(1023) == '1023.0B'
    assert replclient.sizeof_fmt(1048576) == '1.0MiB'


def test_max_replica_lag(mysql_repl1_conn, mysql_repl2_conn):
    replclient.connect_replicas()
    mysql_repl1_conn.query('START SLAVE')
    mysql_repl2_conn.query('START SLAVE')
    max_sbm, replica = replclient.max_replica_lag()
    assert max_sbm is not None
    mysql_repl1_conn.query('STOP SLAVE')
    mysql_repl2_conn.query('STOP SLAVE')
    max_sbm, replica = replclient.max_replica_lag()
    assert max_sbm is None
    mysql_repl2_conn.query('START SLAVE')
    max_sbm, replica = replclient.max_replica_lag()
    assert max_sbm > -1
    mysql_repl1_conn.query('START SLAVE')


def test_list_bucket_tables(monkeypatch):
    assert 'single_pk' in replclient.list_bucket_tables()
    monkeypatch.setattr(replclient, 'bucket', 'non_existent_db')
    assert replclient.list_bucket_tables() is False
    assert replclient.list_bucket_tables(from_source=True) is False


def test_list_tables_status(mysql_dst_bootstrap, mysql_dst_teardown, mysql_dst_conn):
    mysql_dst_bootstrap()
    replclient.mysql_dst.query('USE single_pk')
    assert replclient.list_tables_status() == {'not_started': 0, 'in_progress': 0, 'checksum': 0,
                                               'complete': 0, 'error': 0}
    mysql_dst_conn.query('INSERT INTO schemigrator_checkpoint VALUES ("single_pk", 1, 2, 0, 0)')
    assert replclient.list_tables_status() == {'not_started': 1, 'in_progress': 0, 'checksum': 0,
                                               'complete': 0, 'error': 0}
    mysql_dst_conn.query('INSERT INTO schemigrator_checkpoint VALUES ("multi_pk", 1, 3, 0, 4)')
    assert replclient.list_tables_status() == {'not_started': 1, 'in_progress': 0, 'checksum': 0,
                                               'complete': 0, 'error': 1}
    mysql_dst_teardown()


def test_list_checksum_status(mysql_dst_bootstrap, mysql_dst_teardown):
    mysql_dst_bootstrap()
    replclient.mysql_dst.query('USE single_pk')
    replclient.mysql_src.query("TRUNCATE TABLE schemigrator_checksums")
    assert replclient.list_checksum_status() == -2
    sql = ('INSERT INTO schemigrator_checksums (db, tbl, chunk, master_crc, master_cnt) '
           'VALUES ("single_pk", "single_pk", 1, "aaa", 1)')
    replclient.mysql_src.query(sql)
    assert replclient.list_checksum_status() == -1
    replclient.mysql_dst.query(sql)
    assert replclient.list_checksum_status() == 1
    mysql_dst_teardown()


def test_list_copy_status(mysql_dst_bootstrap, mysql_dst_teardown):
    mysql_dst_bootstrap()
    assert replclient.list_copy_status() == 0
    mysql_dst_teardown()


def test_get_heartbeat(mysql_dst_bootstrap, mysql_dst_teardown):
    mysql_dst_bootstrap()
    assert replclient.get_heartbeat() >= 0.0
    assert replclient.src_server_id is not None
    mysql_dst_teardown()


def test_get_table_primary_key(monkeypatch):
    assert replclient.get_table_primary_key('single_pk') == 'autonum'

    monkeypatch.setattr(replclient, 'bucket', 'non_existent_db')
    with pytest.raises(Exception) as excinfo:
        assert replclient.get_table_primary_key('single_pk')

    monkeypatch.setattr(replclient, 'bucket', 'multi_pk')
    with pytest.raises(Exception) as excinfo:
        assert replclient.get_table_primary_key('multi_pk')


def test_get_table_columns(monkeypatch):
    assert replclient.get_table_columns('single_pk') is True
    assert replclient.columns_str == {'single_pk': '`autonum`, `c`, `n`'}
    assert replclient.columns_arr == {'single_pk': ['autonum', 'c', 'n']}
    assert list(replclient.columns_dict['single_pk'][0].keys()).sort() == ['col', 'is_nullable', 'data_type'].sort()
    monkeypatch.setattr(replclient, 'bucket', 'non_existent_db')
    with pytest.raises(Exception) as excinfo:
        assert replclient.get_table_columns('non_existent_table')


def test_checkpoint_begin():
    assert replclient.checkpoint_begin() is True
    assert replclient.trx_open is True


def test_halt_or_pause(tmpdir, monkeypatch):
    assert replclient.halt_or_pause() is True
    stop_file = tmpdir.join('schemigrator.stop')
    stop_file.write('x')
    monkeypatch.setattr(replclient, 'checkpoint_only', True)
    assert replclient.halt_or_pause() is False
    monkeypatch.setattr(replclient, 'stop_file', str(stop_file.realpath()))
    assert replclient.halt_or_pause() is False
    monkeypatch.setattr(replclient, 'is_alive', False)
    assert replclient.halt_or_pause() is False


def test_setup_for_checksum(monkeypatch):
    monkeypatch.setitem(replclient.pkcols, 'single_pk', 'autonum')
    assert replclient.setup_for_checksum('single_pk') is True


def test_checkpoint_write(mysql_dst_bootstrap, mysql_dst_teardown, monkeypatch):
    mysql_dst_bootstrap()
    replclient.mysql_applier.query('USE single_pk')

    with pytest.raises(mysql.connector.errors.IntegrityError) as excinfo:
        assert replclient.checkpoint_write()

    monkeypatch.setattr(replclient, 'checkpoint_next_binlog_fil', 'fff')
    monkeypatch.setattr(replclient, 'checkpoint_next_binlog_pos', 10000)
    assert replclient.checkpoint_write() is None
    if replclient.mysql_applier.conn.in_transaction:
        replclient.mysql_applier.conn.rollback()
    mysql_dst_teardown()


def test_checksum_chunk(monkeypatch):
    values = {'chunk': 1, 'lower_boundary': 1, 'upper_boundary': 20000,
              'master_cnt': 0, 'master_crc': 'aaa'}
    assert replclient.checksum_chunk(replcursor, values) == True
    monkeypatch.setattr(replclient, 'checkpoint', False)
    assert replclient.checksum_chunk(replcursor, values) == True
    monkeypatch.setattr(replclient, 'checkpoint', True)


def test_begin_apply_trx():
    assert replclient.begin_apply_trx() is True
    assert replclient.trx_open is True
    assert replclient.mysql_applier.conn.in_transaction is True
    assert replclient.mysql_applier.conn.rollback() is None


def test_compose_columns_and_values(monkeypatch, single_pk_columns):
    values = {'autonum': 1, 'c': 'a', 'n': 1}
    column_keys, set_keys, set_values = replclient.compose_columns_and_values(values, single_pk_columns)
    assert column_keys == ['`autonum`', '`c`', '`n`']
    assert set_keys == ['%s', '%s', '%s']
    assert set_values == [1, 'a', 1]
    values = {'autonum': 1, 'c': 'a', 'n': None}
    column_keys, set_keys, set_values = replclient.compose_columns_and_values(values, single_pk_columns,
                                                                              null_check=True)
    assert column_keys == ['`autonum`', '`c`']
    assert set_keys == ['%s', '%s']
    assert set_values == [1, 'a']
    values = {'autonum': 1, 'c': 'ó', 'n': 1}
    monkeypatch.setattr(replclient.mysql_applier, 'charset', 'utf8')
    column_keys, set_keys, set_values = replclient.compose_columns_and_values(values, single_pk_columns,
                                                                              charset_check=True)
    assert replclient.mysql_applier.charset == 'utf8'
    assert column_keys == ['`autonum`', '`c`', '`n`']
    assert set_keys == ['%s', 'UNHEX(%s)', '%s']
    assert set_values == [1, 'f3', 1]


def test_insert(mysql_dst_bootstrap, mysql_dst_teardown, mysql_dst_conn, single_pk_columns):
    mysql_dst_bootstrap()
    values = {'values': {'autonum': 1, 'c': 'a', 'n': 1}}

    replclient.mysql_applier.query('USE single_pk')
    assert replclient.insert(replcursor, 'single_pk', values, single_pk_columns) is None
    assert mysql_dst_conn.row('SELECT autonum, c, n FROM single_pk') == values['values']

    # No assert here since we use REPLACE INTO
    # with pytest.raises(mysql.connector.errors.IntegrityError) as excinfo:
    #    assert replclient.insert(cursor, 'single_pk', values)

    values = {'values': {'autonum': 1, 'c': 'a', 'n': None}}

    with pytest.raises(mysql.connector.errors.DatabaseError) as excinfo:
        assert replclient.insert(replcursor, 'single_pk', values, single_pk_columns)
    mysql_dst_teardown()


def test_update(mysql_dst_bootstrap, mysql_dst_teardown, mysql_dst_conn, single_pk_columns):
    mysql_dst_bootstrap()
    replclient.mysql_applier.query('USE single_pk')

    values = {'values': {'autonum': 1, 'c': 'a', 'n': 1}}
    assert replclient.insert(replcursor, 'single_pk', values, single_pk_columns) is None

    values = {'before_values': {'autonum': 1, 'c': 'a', 'n': 1},
              'after_values': {'autonum': 1, 'c': 'a', 'n': 2}}
    assert replclient.update(replcursor, 'single_pk', values, single_pk_columns) is None
    assert mysql_dst_conn.row('SELECT autonum, c, n FROM single_pk') == values['after_values']

    # Force duplicate key error
    values = {'values': {'autonum': 2, 'c': 'a', 'n': 2}}
    assert replclient.insert(replcursor, 'single_pk', values, single_pk_columns) is None

    values = {'before_values': {'autonum': 1, 'c': 'a', 'n': 2},
              'after_values': {'autonum': 2, 'c': 'a', 'n': 2}}

    with pytest.raises(mysql.connector.errors.IntegrityError) as excinfo:
        assert replclient.update(replcursor, 'single_pk', values, single_pk_columns)
    mysql_dst_teardown()


def test_delete(mysql_dst_bootstrap, mysql_dst_teardown, mysql_dst_conn, single_pk_columns):
    mysql_dst_bootstrap()
    replclient.mysql_applier.query('USE single_pk')

    values = {'values': {'autonum': 1, 'c': 'a', 'n': 1}}
    assert replclient.insert(replcursor, 'single_pk', values, single_pk_columns) is None
    assert replclient.delete(replcursor, 'single_pk', values) is None
    assert mysql_dst_conn.row('SELECT autonum, c, n FROM single_pk') is None
    mysql_dst_teardown()


def test_checksum_chunk(mysql_dst_bootstrap, mysql_dst_teardown, mysql_dst_conn, monkeypatch):
    mysql_dst_bootstrap()
    values = {
        'chunk': 1, 'lower_boundary': 1, 'upper_boundary': 1000,
        'tbl': 'single_pk', 'master_cnt': 1000, 'master_crc': 'aabbcc'
    }
    assert replclient.checksum_chunk(replcursor, values) is True
    monkeypatch.setattr(replclient, 'checksum', False)
    assert replclient.checksum_chunk(replcursor, values) is True
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

    assert replclient.checkpoint_end(force=True, fil='aaa', pos=120) is None
    assert replclient.trx_open is False
    assert replclient.mysql_applier.conn.in_transaction is False
    mysql_dst_teardown()


def test_log_event_metrics():
    assert replclient.log_event_metrics(start=True) is True
    assert replclient.metrics['events'] == 0
    assert replclient.log_event_metrics(binlog_fil='aaa', binlog_pos=120) is True


def evaluate_backoff(mysql_dst_bootstrap, mysql_dst_teardown, monkeypatch):
    monkeypatch.setattr(replclient, 'checkpoint_next_binlog_fil', 'bbb')
    monkeypatch.setattr(replclient, 'checkpoint_next_binlog_pos', 999)
    mysql_dst_bootstrap()
    assert replclient.evaluate_backoff() is True
    assert replclient.evaluate_backoff() is True
    monkeypatch.setattr(replclient, 'is_alive', False)
    assert replclient.evaluate_backoff() is False
    monkeypatch.setattr(replclient, 'backoff_counter', 3)
    assert replclient.evaluate_backoff() is False
    mysql_dst_teardown()


def test_backoff_reset(monkeypatch):
    assert replclient.backoff_reset() is None
    monkeypatch.setattr(replclient, 'backoff_last_pos', 1)
    assert replclient.backoff_reset() is None
    assert replclient.backoff_last_pos is None


