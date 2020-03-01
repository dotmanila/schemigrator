#!/usr/bin/env python3

import pytest
import schemigrate

opts = None
logger = None

def test___parse_dsn():
	assert schemigrate.__parse_dsn('p=xxx,D=x.x.x.x') == {'password':'xxx','database':'x.x.x.x'}
	assert schemigrate.__parse_dsn('localhost') == {'host': 'localhost'}

def setup_module():
	global opts
	global logger

	opts = True
	logger = True

def teardown_module():
	opts = None
	logger = None