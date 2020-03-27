#!/usr/bin/env python3

import pytest
import schemigrate

opts = None
logger = None

def test_sm_parse_dsn():
	assert schemigrate.sm_parse_dsn('p=xxx,D=x.x.x.x') == {'passwd':'xxx','db':'x.x.x.x'}
	assert schemigrate.sm_parse_dsn('localhost') == {'host': 'localhost'}

def test_escape():
    assert schemigrate.escape('`table`') == '``table``'
    assert schemigrate.escape('table') == 'table'

def test_list_to_col_str():
    assert schemigrate.list_to_col_str(['a', '`t']) == '`a`, ```t`'

def setup_module():
	global opts
	global logger

	opts = True
	logger = True

def teardown_module():
	opts = None
	logger = None