#!/usr/bin/env python3

import conftest
import mysql.connector
import pytest
import schemigrate
import time

opts, logger, schemigrator = conftest.init()
schemigrate_base = schemigrate.SchemigrateBase(opts.src_dsn, opts.dst_dsn)
schemigrate_logger = schemigrate.sm_create_logger(False, 'test')


def test_properties():
    assert isinstance(schemigrate_base.mysql_src, schemigrate.MySQLConnection) is True
    assert isinstance(schemigrate_base.mysql_dst, schemigrate.MySQLConnection) is True


def test__loop_sleep_timer(monkeypatch):
    monkeypatch.setattr(schemigrate_base, 'logger', schemigrate_logger)
    assert schemigrate_base._loop_sleep_timer(0.0, 0.2) is True
    monkeypatch.setattr(schemigrate_base, 'is_alive', False)
    assert schemigrate_base._loop_sleep_timer(0.4, 0.2) is False


