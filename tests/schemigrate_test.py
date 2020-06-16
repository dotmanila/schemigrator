#!/usr/bin/env python3

import schemigrate


def test_sm_parse_dsn():
    assert schemigrate.sm_parse_dsn('p=xxx,D=x.x.x.x') == {'connect_timeout': 2, 'passwd': 'xxx', 'db': 'x.x.x.x'}
    assert schemigrate.sm_parse_dsn('localhost') == {'connect_timeout': 2, 'host': 'localhost'}


def test_escape():
    assert schemigrate.escape('`table`') == '``table``'
    assert schemigrate.escape('table') == 'table'


def test_list_to_col_str():
    assert schemigrate.list_to_col_str(['a', '`t']) == '`a`, ```t`'
