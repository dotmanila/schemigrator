#!/usr/bin/env python3

import mysql.connector
import pytest
import schemigrate
import conftest

opts, logger, schemigrator = conftest.init()
table = schemigrate.Table(opts.src_dsn, opts.bucket, 'single_pk')
checksum = schemigrate.TableChecksum(opts.bucket, 'single_pk', table.columns)

row_checksum_sql = ("COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) "
                    "AS UNSIGNED)), 10, 16)), 0)") % '`autonum`, CONVERT(`c` USING binary), `n`'

src_checksum_sql = ("REPLACE INTO schemigrator_checksums "
                    "(db, tbl, chunk, chunk_index, lower_boundary, upper_boundary, "
                    "master_cnt, master_crc) "
                    "SELECT '{0}', '{1}', %s, '{2}', %s, %s, COUNT(*) AS cnt, {3} AS crc "
                    "FROM `{1}` FORCE INDEX(`PRIMARY`) "
                    "WHERE ((`{2}` >= %s)) AND ((`{2}` <= %s))").format(opts.bucket, 'single_pk', 'autonum',
                                                                        row_checksum_sql)

dst_checksum_sql = ("REPLACE INTO schemigrator_checksums "
                    "(db, tbl, chunk, chunk_index, lower_boundary, upper_boundary, "
                    "master_cnt, master_crc, this_cnt, this_crc) "
                    "SELECT '{0}', '{1}', %s, '{2}', %s, %s, %s, %s, COUNT(*) AS cnt, {3} AS crc "
                    "FROM `{1}` FORCE INDEX(`PRIMARY`) "
                    "WHERE ((`{2}` >= %s)) AND ((`{2}` <= %s))").format(opts.bucket, 'single_pk', 'autonum',
                                                                        row_checksum_sql)


def test_source_checksum_sql():
    assert checksum.source_checksum_sql == src_checksum_sql


def test_target_checksum_sql():
    assert checksum.target_checksum_sql == dst_checksum_sql


def test_row_checksum_sql():
    assert checksum.row_checksum_sql == row_checksum_sql
    assert checksum.primary_key == 'autonum'
