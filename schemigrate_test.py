#!/usr/bin/env python3

import mysql.connector
import pytest
import schemigrate
import conftest

opts, logger, schemigrator = conftest.init()

def test_sm_parse_dsn():
	assert schemigrate.sm_parse_dsn('p=xxx,D=x.x.x.x') == {'passwd':'xxx','db':'x.x.x.x'}
	assert schemigrate.sm_parse_dsn('localhost') == {'host': 'localhost'}

def test_escape():
    assert schemigrate.escape('`table`') == '``table``'
    assert schemigrate.escape('table') == 'table'

def test_list_to_col_str():
    assert schemigrate.list_to_col_str(['a', '`t']) == '`a`, ```t`'

def test_setup_bootstrap(monkeypatch, mysql_dst_conn):
    assert schemigrator.opts.dst_dsn['db'] == None
    assert schemigrator.opts.src_dsn['db'] == 'single_pk'

    monkeypatch.setitem(schemigrator.opts.dst_dsn, 'db', schemigrator.opts.bucket)
    with pytest.raises(mysql.connector.errors.ProgrammingError) as excinfo:
        assert schemigrator.setup_bootstrap()
    assert 'Unknown database' in str(excinfo.value)

    sql = ("""SELECT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES 
           WHERE TABLE_SCHEMA = '%s'""") % schemigrator.opts.bucket
    
    monkeypatch.delitem(schemigrator.opts.dst_dsn, 'db', raising=False)

    #schemigrator.setup_bootstrap()
    #with pytest.raises(mysql.connector.errors.ProgrammingError) as excinfo:
    #    assert schemigrator.setup_bootstrap()
    #assert excinfo.value == None
    assert mysql_dst_conn.scalar(sql, 'TABLE_SCHEMA') == 'single_pk'
