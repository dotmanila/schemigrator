#!/usr/bin/env python3

import mysql.connector
import pytest
import schemigrate
import conftest

opts, logger, schemigrator = conftest.init()
tablecopier = schemigrate.TableCopier(opts.src_dsn, opts.dst_dsn, opts.bucket, 'single_pk', 
                                      debug=False, pause_file=None, stop_file=None, 
                                      chunk_size=1000, replica_dsns=opts.replicas, max_lag=60, 
                                      use_inout_file=True, checksum=True)

def test_connect_source():
    assert tablecopier.connect_source() == None

def test_connect_target():
    assert tablecopier.connect_target() == None

def test_get_table_columns(mysql_src_conn_bucket):
    assert tablecopier.get_table_columns() == True
    assert tablecopier.columns_str == '`autonum`, `c`, `n`'
    assert tablecopier.columns_arr == ['autonum', 'c', 'n']
    assert list(tablecopier.columns_dict[0].keys()).sort() == ['col', 'is_nullable', 'data_type'].sort()

def test_setup_for_checksum(mysql_dst_conn_bucket):
    assert tablecopier.setup_for_checksum() == True

    sql = ("""SELECT TABLE_NAME AS t FROM INFORMATION_SCHEMA.TABLES 
           WHERE TABLE_SCHEMA = 'single_pk' AND TABLE_NAME = 'schemigrator_checksums'""")
    assert mysql_dst_conn_bucket.scalar(sql, 't') == 'schemigrator_checksums'

def test_get_table_primary_key(monkeypatch):
    assert tablecopier.get_table_primary_key() == True
    assert tablecopier.pk == 'autonum'

    monkeypatch.setattr(tablecopier, 'table', 'no_pk')
    with pytest.raises(Exception) as excinfo:
        assert tablecopier.get_table_primary_key()
    assert 'no PRIMARY KEY' in str(excinfo.value)

    monkeypatch.setattr(tablecopier, 'bucket', 'multi_pk')
    monkeypatch.setattr(tablecopier, 'table', 'multi_pk')

    with pytest.raises(Exception) as excinfo:
        assert tablecopier.get_table_primary_key()
    assert 'multiple PRIMARY KEY' in str(excinfo.value)

def test_set_tsv_file():
    assert tablecopier.set_tsv_file() == False

def test_get_checkpoint(mysql_dst_conn_bucket):
    assert tablecopier.get_checkpoint()['status'] == 0

def test_get_min_max_range():
    assert tablecopier.get_min_max_range() == (1, 3700)
