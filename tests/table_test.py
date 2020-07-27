#!/usr/bin/env python3

import mysql.connector
import pytest
import schemigrate
import conftest

opts, logger, schemigrator = conftest.init()
table = schemigrate.Table(opts.src_dsn, opts.bucket, 'single_pk')


def test_properties():
    assert table.columns_dict is None
    assert table.primary_key_column is None
    assert table.primary_key == 'autonum'
    assert table.min_primary_key == 1
    assert table.max_primary_key == 3700
    assert list(table.columns_dict[0].keys()).sort() == ['col', 'is_nullable', 'data_type'].sort()


def test_get_table_columns():
    assert table.get_table_columns() is True
    assert table.columns_str == '`autonum`, `c`, `n`'
    assert table.columns_arr == ['autonum', 'c', 'n']
    assert list(table.columns_dict[0].keys()).sort() == ['col', 'is_nullable', 'data_type'].sort()


def test_get_min_max_range():
    assert table.get_min_max_range() == (1, 3700)

