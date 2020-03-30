#!/usr/bin/env python3

import pytest
import schemigrate

opts = None
logger = None

sql_single_pk = ("""
CREATE TABLE single_pk (
    autonum INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    c CHAR(1) NOT NULL,
    n INT NOT NULL,
    KEY (n)
) ENGINE=INNODB
""")

sql_multi_pk = ("""
CREATE TABLE multi_pk (
    autonum INT UNSIGNED NOT NULL AUTO_INCREMENT,
    c CHAR(1) NOT NULL,
    n INT NOT NULL,
    PRIMARY KEY (autonum, c),
    KEY (n)
) ENGINE=INNODB
""")

logger = schemigrate.sm_create_logger(False, 'SchemigrateTest', null_handler=True)
opts = schemigrate.SchemigrateOptionParser('test')
opts.dst_dsn = {'host': '127.0.0.1', 'port': 13301, 'db': 'single_pk', 
                'user': 'schemigrator', 'passwd': 'schemigrator'}
opts.src_dsn = {'host': '127.0.0.1', 'port': 13300, 'db': 'single_pk', 
                'user': 'schemigrator', 'passwd': 'schemigrator'}
opts.dst_dsn = [{'host': '127.0.0.1', 'port': 13302, 'db': 'single_pk', 
                 'user': 'schemigrator', 'passwd': 'schemigrator'},
                {'host': '127.0.0.1', 'port': 13303, 'db': 'single_pk', 
                 'user': 'schemigrator', 'passwd': 'schemigrator'},
               ]
opts.bucket = 'single_pk'
opts.chunk_size_repl = 10
opts.chunk_size_copy = 100
opts.max_lag = 10
schemigrator = schemigrate.Schemigrate(opts, logger)

def test_sm_parse_dsn():
	assert schemigrate.sm_parse_dsn('p=xxx,D=x.x.x.x') == {'passwd':'xxx','db':'x.x.x.x'}
	assert schemigrate.sm_parse_dsn('localhost') == {'host': 'localhost'}

def test_escape():
    assert schemigrate.escape('`table`') == '``table``'
    assert schemigrate.escape('table') == 'table'

def test_list_to_col_str():
    assert schemigrate.list_to_col_str(['a', '`t']) == '`a`, ```t`'

def test_setup_bootstrap():
    schemigrator.setup_bootstrap()
    sql = 'SELECT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = "%s"' % schemigrator.opts.bucket
    assert schemigrator.mysql_dst.fetchone(sql) == 'single_pk'
