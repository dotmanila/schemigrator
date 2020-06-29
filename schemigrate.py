#!/usr/bin/env python3

import logging
import mysql.connector
import os
import signal
import sys
import time
import traceback
import warnings
from multiprocessing import Process, Lock, Manager
from mysql.connector import errorcode
from optparse import OptionParser
from pprint import pprint as pp
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent, UpdateRowsEvent,
    WriteRowsEvent
)
from pymysqlreplication.event import XidEvent
from pymysqlreplication.constants.FIELD_TYPE import VARCHAR, STRING, VAR_STRING, JSON

"""
sudo apt install libmysqlclient-dev python3 python3-pip
pip3 install mysql


TODO
Features/Enhancements
- Support for configuration files on MySQL DSNs

"""

VERSION = 1.1
SIGTERM_CAUGHT = False
logger = None

sql_schemigrator_checksums = ("""
CREATE TABLE IF NOT EXISTS schemigrator_checksums (
   db             CHAR(64)     NOT NULL,
   tbl            CHAR(64)     NOT NULL,
   chunk          BIGINT       NOT NULL,
   chunk_time     FLOAT            NULL,
   chunk_index    VARCHAR(200)     NULL,
   lower_boundary BIGINT           NULL,
   upper_boundary BIGINT           NULL,
   this_crc       CHAR(40)         NULL,
   this_cnt       INT              NULL,
   master_crc     CHAR(40)     NOT NULL,
   master_cnt     INT          NOT NULL,
   ts             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   chunk_sql      TEXT             NULL,
   PRIMARY KEY (db, tbl, chunk),
   INDEX ts_db_tbl (ts, db, tbl),
   INDEX chunk (chunk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
""")

sql_schemigrator_binlog_status = ("""
CREATE TABLE IF NOT EXISTS schemigrator_binlog_status (
    bucket VARCHAR(255) NOT NULL, 
    fil VARCHAR(255) NOT NULL, 
    pos BIGINT UNSIGNED NOT NULL, 
    PRIMARY KEY (bucket)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
""")

sql_schemigrator_checkpoint = ("""
CREATE TABLE IF NOT EXISTS schemigrator_checkpoint (
    tbl VARCHAR(255) NOT NULL, 
    minpk BIGINT UNSIGNED NOT NULL DEFAULT 0, 
    maxpk BIGINT UNSIGNED NOT NULL DEFAULT 0, 
    lastpk BIGINT UNSIGNED NOT NULL DEFAULT 0, 
    status TINYINT NOT NULL DEFAULT 0, 
    PRIMARY KEY (tbl)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
""")

sql_copy_checksum_progress = ("""
SELECT chkpt.tbl, chkpt.maxpk, COALESCE(chksm.lastsm, 0) AS lastsm 
FROM schemigrator_checkpoint chkpt 
LEFT JOIN (
  SELECT tbl, MAX(upper_boundary) lastsm 
  FROM schemigrator_checksums 
  GROUP BY tbl
) chksm ON (chkpt.tbl = chksm.tbl) 
WHERE chkpt.maxpk > lastsm
""")

sql_checksum_results = ("""
SELECT db, tbl, SUM(this_cnt) AS total_rows, COUNT(*) AS chunks
FROM schemigrator_checksums
WHERE (
 master_cnt <> this_cnt
 OR master_crc <> this_crc
 OR ISNULL(master_crc) <> ISNULL(this_crc))
GROUP BY db, tbl
""")

sql_checksum_results_bad = ("""
SELECT COUNT(*) AS chunks
FROM schemigrator_checksums
WHERE (
 master_cnt <> this_cnt
 OR master_crc <> this_crc
 OR ISNULL(master_crc) <> ISNULL(this_crc))
""")

schemigrator_tables = [
    'schemigrator_checksums',
    'schemigrator_binlog_status',
    'schemigrator_checkpoint'
]


def sm_sigterm_handler(signal, frame):
    global SIGTERM_CAUGHT
    print('Signal caught (%s), terminating' % str(signal))
    SIGTERM_CAUGHT = True


def sm_buildopts():
    opt_usage = "Usage: %prog [options] COMMAND"
    opt_desc = "Migrate databases from one MySQL server to another."
    opt_epilog = ""
    parser = SchemigrateOptionParser(opt_usage, version="%prog " + str(VERSION),
                                     description=opt_desc, epilog=opt_epilog)
    parser.add_option('-B', '--bucket', dest='bucket', type='string',
                      help='The bucket/database name to migrate', default=None)
    parser.add_option('-n', '--chunk-size', dest='chunk_size', type='int',
                      help='How many rows per transaction commit', default=1000)
    parser.add_option('', '--chunk-size-repl', dest='chunk_size_repl', type='int',
                      help=('How many rows per transaction commit for ReplicationClient, '
                            'overrides --chunk-size'), default=0)
    parser.add_option('', '--chunk-size-copy', dest='chunk_size_copy', type='int',
                      help='How many rows per transaction commit for TableCopier, '
                           'overrides --chunk-size', default=0)
    parser.add_option('-r', '--max-lag', dest='max_lag', type='int',
                      help='Max replication lag (seconds) on target to start throttling', default=60)
    parser.add_option('-R', '--replica-dsns', dest='replica_dsns', type='string', action='append',
                      help='Replica DSNs to check for replication lag', default=[])
    parser.add_option('-d', '--debug', dest='debug', action="store_true",
                      help='Enable debugging outputs', default=False)
    parser.add_option('-c', '--defaults-file', dest='dotmycnf', type='string',
                      help='Path to .my.cnf containing connection credentials to MySQL',
                      default='~/.my.cnf')
    parser.add_option('-L', '--log', dest='log', type='string',
                      help='Log output to specified file',
                      default=None)
    parser.add_option('-x', '--stop-file', dest='stop_file', type='string',
                      help='When this file exists, the script terminates itself',
                      default=None)
    parser.add_option('-p', '--pause-file', dest='pause_file', type='string',
                      help='When this script exists, the script pauses copying and replication',
                      default=None)
    parser.add_option('-X', '--dry-run', dest='dryrun', action="store_true",
                      help='Show what the script will be doing instead of actually doing it',
                      default=False)
    parser.add_option('-o', '--use-insert-select', dest='use_insert_select', action="store_true",
                      help=(('Instead of using SELECT INTO OUTFILE/LOAD DATA INFILE, use native '
                             'and slower simulated INSERT INTO SELECT')),
                      default=False)
    parser.add_option('-C', '--checksum', dest='checksum', action="store_true",
                      help=(('Checksum chunks as they are copied, '
                             'ReplicationClient validates the checksums')),
                      default=False)
    parser.add_option('-w', '--mode', dest='mode', type="choice", choices=["parallel", "serialized"],
                      help='Show what the script will be doing instead of actually doing it',
                      default="parallel")

    (opts, args) = parser.parse_args()

    if len(args) != 2:
        parser.error('Source and destination DSNs are required')

    opts.src_dsn = sm_parse_dsn(args[0])
    opts.dst_dsn = sm_parse_dsn(args[1])
    if 'port' not in opts.src_dsn:
        opts.src_dsn['port'] = 3306
    else:
        opts.src_dsn['port'] = int(opts.src_dsn['port'])

    if 'port' in opts.dst_dsn:
        opts.dst_dsn['port'] = int(opts.dst_dsn['port'])

    if 'charset' not in opts.dst_dsn and 'charset' in opts.src_dsn:
        opts.dst_dsn['charset'] = opts.src_dsn['charset']

    opts.dst_dsn = sm_copy_dsn(opts.src_dsn, opts.dst_dsn)
    opts.dst_dsn.pop('db', None)

    if opts.src_dsn['host'] == opts.dst_dsn['host'] and opts.src_dsn['port'] == opts.dst_dsn['port']:
        parser.error('Source and destination servers cannot be the same')

    if opts.bucket is None:
        if 'db' in opts.src_dsn:
            opts.bucket = opts.src_dsn['db']
    elif 'db' not in opts.src_dsn and opts.bucket is not None:
        opts.src_dsn['db'] = opts.bucket
        opts.dst_dsn['db'] = opts.bucket

    if opts.bucket is None:
        parser.error('Bucket name or source database was not specified')
    
    opts.replicas = []
    if len(opts.replica_dsns) > 0:
        for replica in opts.replica_dsns:
            opts.replicas.append(sm_copy_dsn(opts.dst_dsn, sm_parse_dsn(replica)))

    opts.use_inout_file = True
    if opts.use_insert_select:
        opts.use_inout_file = False

    if opts.chunk_size_copy == 0:
        opts.chunk_size_copy = opts.chunk_size

    if opts.chunk_size_repl == 0:
        opts.chunk_size_repl = opts.chunk_size

    opts.ppid = os.getpid()
    opts.pcwd = os.path.dirname(os.path.realpath(__file__))

    return opts


def sm_create_logger(debug, name, null_handler=False):
    logger = logging.getLogger(name)
    logformat = '%(asctime)s <%(process)d> %(levelname)s_[{0}]_:: %(message)s'.format(name.ljust(24))

    if debug:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    logger.setLevel(loglevel)
    formatter = logging.Formatter(logformat)

    if not null_handler:
        streamh = logging.StreamHandler()
    else:
        streamh = logging.NullHandler()
    streamh.setFormatter(formatter)
    logger.addHandler(streamh)
    
    return logger


def sm_parse_dsn(dsn):
    dsn_keys = {'h': 'host', 'u': 'user', 'P': 'port', 'p': 'passwd',
                'S': 'socket', 'D': 'db', 'A': 'charset'}
    params = {}
    if len(dsn) == 0:
        raise Exception('Invalid DSN value')

    dsn_parts = dsn.split(',')
    if len(dsn_parts) == 1:
        if '=' not in dsn:
            dsn_parts = ['h=%s' % dsn.strip()]

    for dsn_part in dsn_parts:
        kv = dsn_part.split('=')
        if len(kv) != 2:
            raise Exception('Invalid DSN value %s' % str(kv[1]))
        if kv[0] not in dsn_keys:
            raise Exception('Invalid DSN key %s' % str(kv[0]))

        if kv[0] == 'P':
            params[dsn_keys[kv[0]]] = int(kv[1])
        else:
            params[dsn_keys[kv[0]]] = kv[1]

    # Enforce short connection timeout
    # This is buggy on the driver, can lead to the below error
    # mysql.connector.errors.OperationalError: 2055: Lost connection to MySQL server at 'host:3306',
    # system error: The read operation timed out
    # https://bugs.mysql.com/bug.php?id=74933
    # params['connect_timeout'] = 2

    return params 


def sm_copy_dsn(src, dest):
    for k in src:
        if k not in dest:
            dest[k] = src[k]

    return dest


""" Taken from OnlineSchemaChange """


def escape(literal):
    """
    Escape the backtick in table/column name

    @param literal:  name string to escape
    @type  literal:  string

    @return:  escaped string
    @rtype :  string
    """
    return literal.replace('`', '``')


def list_to_col_str(column_list):
    """Basic helper function for turn a list of column names into a single
    string separated by comma, and escaping the column name in the meanwhile

    @param column_list:  list of column names
    @type  column_list:  list

    @return:  String of concated/escaped column names
    @rtype :  string
    """
    return ', '.join('`{}`'.format(escape(col)) for col in column_list)


class Schemigrate(object):
    def __init__(self, opts, logger):
        self.opts = opts
        self.logger = logger
        self.mysql_src = None
        self.mysql_dst = None
        self.table_copier = None
        self.replication_client = None
        self.is_alive = True
        self.checkpoint_only = True if self.opts.mode == 'serialized' else False
        self.state = 0
        self.proc_manager = None
        self.manager_dict = None

    def signal_handler(self, signal, frame):
        self.logger.info("Signal caught (%s), cleaning up" % str(signal))
        if self.replication_client is not None:
            self.replication_client.terminate()
            self.logger.info("Replication client terminated")

        if self.table_copier is not None:
            try:
                if self.table_copier.is_alive():
                    self.table_copier.terminate()
            except AssertionError as err:
                if 'can only test a child process' not in str(err):
                    raise err
            self.logger.info("Replication client terminated")

        self.logger.info("Waiting for main thread to terminate")
        self.is_alive = False

    def connect_source(self):
        if self.mysql_src is None or not self.mysql_src.conn.is_connected():
            self.mysql_src = MySQLConnection(self.opts.src_dsn, 'Schemigrator, src')
            if self.state > 0:
                self.mysql_src.query('USE %s' % self.opts.src_dsn['db'])

    def connect_target(self):
        if self.mysql_dst is None or not self.mysql_dst.conn.is_connected():
            self.mysql_dst = MySQLConnection(self.opts.dst_dsn, 'Schemigrator, dst')
            if self.state > 0:
                self.mysql_dst.query('USE %s' % self.opts.src_dsn['db'])

    def setup_bootstrap(self):
        self.connect_source()
        src_hostname = self.mysql_src.fetchone('SELECT @@hostname as h', 'h')
        self.logger.info('Test connection to source succeeded, got hostname "%s"' % src_hostname)

        self.connect_target()
        dst_hostname = self.mysql_dst.fetchone('SELECT @@hostname as h', 'h')
        self.logger.info('Test connection to destination succeeded, got hostname "%s"' % dst_hostname)

        self.mysql_dst.query('CREATE DATABASE IF NOT EXISTS %s' % self.opts.src_dsn['db'])
        self.mysql_dst.query('USE %s' % self.opts.src_dsn['db'])
        self.state = 1

    def setup_metadata_tables(self, tables):
        self.mysql_dst.query(sql_schemigrator_binlog_status)
        self.mysql_dst.query(sql_schemigrator_checksums)
        self.mysql_dst.query(sql_schemigrator_checkpoint)

        for table in tables:
            sql = "INSERT IGNORE INTO schemigrator_checkpoint (tbl) VALUES('%s')" % table
            self.mysql_dst.query(sql)
        self.state = 2

    def create_dst_tables(self, tables):
        """ Copy tables from source to dest, do not create if table already exists
        """
        tables_in_dst = self.list_bucket_tables(from_source=False)
        for table in tables:
            if table not in tables_in_dst:
                self.logger.info("Creating table '%s' on target" % table)
                create_sql = self.mysql_src.fetchone('SHOW CREATE TABLE %s' % table)
                self.mysql_dst.query(create_sql['Create Table'].strip())

    def list_bucket_tables(self, from_source=True):
        sql = ('SELECT TABLE_NAME AS tbl FROM INFORMATION_SCHEMA.TABLES '
               'WHERE ENGINE="InnoDB" AND TABLE_TYPE="BASE TABLE" AND '
               'TABLE_SCHEMA="%s"' % self.opts.bucket)

        if from_source:
            rows = self.mysql_src.query(sql)
        else:
            rows = self.mysql_dst.query(sql)

        if len(rows) == 0:
            if from_source:
                self.logger.error('Source bucket has no tables!')
            return False

        tables = []
        for row in rows:
            if row['tbl'] not in schemigrator_tables:
                tables.append(row['tbl'])

        return tables

    def is_table_copy_complete(self, table):
        sql = "SELECT status FROM schemigrator_checkpoint WHERE tbl = '%s'" % table
        self.connect_target()
        status = self.mysql_dst.fetchone(sql, 'status')
        if status is None:
            return 0

        return int(status)

    def list_incomplete_tables(self):
        sql = "SELECT tbl, status FROM schemigrator_checkpoint WHERE status < 2"
        self.connect_target()
        tables = self.mysql_dst.query_dict(sql)

        return tables

    def get_binlog_checkpoint(self):
        sql = 'SELECT fil, pos FROM schemigrator_binlog_status WHERE bucket = "%s"' % self.opts.bucket
        fil_pos = self.mysql_dst.query(sql)

        binlog_fil = None
        binlog_pos = None
        
        if len(fil_pos) > 1:
            raise Exception('Multiple binlog coordinates reported on schemigrator_binlog_status')
        elif len(fil_pos) == 1:
            binlog_fil = fil_pos[0]['fil']
            binlog_pos = fil_pos[0]['pos']

        return binlog_fil, binlog_pos

    def get_binlog_coords(self):
        binlog_fil, binlog_pos = self.get_binlog_checkpoint()

        if binlog_fil is None:
            binlog_fil, binlog_pos = self.master_status()
            self.logger.info('Binlog replication info is empty, SHOW MASTER STATUS')
            self.logger.info('Starting file: %s, position: %d' % (binlog_fil, binlog_pos))
        else:
            self.logger.info('Binlog replication found from checkpoint, resuming')
            self.logger.info('Starting file: %s, position: %d' % (binlog_fil, binlog_pos))

        return binlog_fil, binlog_pos

    def master_status(self):
        status = self.mysql_src.fetchone('SHOW MASTER STATUS')
        return status['File'], status['Position']

    def run_table_copier(self, table, return_dict):
        copier = TableCopier(self.opts.src_dsn, self.opts.dst_dsn, self.opts.bucket, table, 
                             debug=self.opts.debug, pause_file=self.opts.pause_file, stop_file=self.opts.stop_file,
                             chunk_size=self.opts.chunk_size_copy, replica_dsns=self.opts.replicas, 
                             max_lag=self.opts.max_lag, use_inout_file=self.opts.use_inout_file, 
                             checksum=self.opts.checksum)
        return_dict['returncode'] = copier.run()

    def run_replication_client(self, binlog_fil, binlog_pos, checkpoint_only):
        repl = ReplicationClient(self.opts.src_dsn, self.opts.dst_dsn, 
                                 self.opts.bucket, binlog_fil, binlog_pos, 
                                 debug=self.opts.debug, pause_file=self.opts.pause_file, 
                                 stop_file=self.opts.stop_file, 
                                 chunk_size=self.opts.chunk_size_repl, 
                                 replica_dsns=self.opts.replicas, 
                                 max_lag=self.opts.max_lag, checksum=self.opts.checksum,
                                 checkpoint_only=checkpoint_only)
        return repl.run()

    def start_table_copy(self, table):
        self.table_copier = Process(target=self.run_table_copier, args=(table, self.manager_dict, ), name='table_copier')
        self.table_copier.start()

        while self.is_alive and self.table_copier.is_alive():
            if not self.replication_client.is_alive() and not self.checkpoint_only:
                self.logger.error(('ReplicationClient terminated unexpectedly '
                                   'terminating TableCopier'))
                self.table_copier.terminate()
                self.is_alive = False
                return 0

            self.logger.debug("__main_loop_sleep")
            time.sleep(3)

        return self.manager_dict['returncode']

    def copy_tables(self, tables):
        for table in tables:
            retcode = self.start_table_copy(table)
            if retcode is not None and retcode > 0:
                return retcode
            if not self.is_alive or os.path.exists(self.opts.stop_file):
                break

        return 0

    def run(self):
        exitcode = 0
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.logger.debug(str(self.opts.src_dsn))
        self.logger.debug(str(self.opts.dst_dsn))

        self.setup_bootstrap()
        tables = self.list_bucket_tables()
        if not tables:
            self.logger.error('Source bucket has no tables!')
            return 0

        self.setup_metadata_tables(tables)
        self.create_dst_tables(tables)
        binlog_fil, binlog_pos = self.get_binlog_coords()

        self.checkpoint_only = True if self.opts.mode == 'serialized' else False
        if self.checkpoint_only:
            self.logger.info('Running in serialized mode')
        self.replication_client = Process(target=self.run_replication_client, 
                                          args=(binlog_fil, binlog_pos, self.checkpoint_only),
                                          name='replication_client')
        self.replication_client.start()

        self.proc_manager = Manager()
        self.manager_dict = self.proc_manager.dict()

        while True:
            incompletes = self.list_incomplete_tables()
            if incompletes is None:
                break

            tables = []
            for row in incompletes:
                if int(row['status']) < 2:
                    tables.append(row['tbl'])

            # More to copy
            if len(tables) > 0:
                exitcode = self.copy_tables(tables)
                # This is dirty, too deep loop within loop and hard to debug
                # exitcode > 1 is fatal error
                if not self.is_alive or exitcode > 1:
                    if self.replication_client.is_alive():
                        self.replication_client.terminate()
                    break
                continue

            # Some tables failed, force replication to terminate too
            if len(tables) == 0 and len(incompletes) > 0:
                self.logger.error('Some tables have failed copy and cannot be retried')
                self.is_alive = False
                exitcode = 2
                if self.replication_client.is_alive():
                    self.replication_client.terminate()

            break

        # When mode == serialized, resume replication only when all table
        # copy succeeds
        if self.checkpoint_only and exitcode == 0 and self.is_alive:
            self.logger.info('Table copy complete, resuming replication')
            binlog_fil, binlog_pos = self.get_binlog_coords()
            self.replication_client = Process(target=self.run_replication_client,
                                              args=(binlog_fil, binlog_pos, False),
                                              name='replication_client')
            self.replication_client.start()

        while self.replication_client.is_alive():
            if not self.is_alive:
                self.logger.info('Waiting for ReplicationClient')
            time.sleep(3)

        if self.replication_client.exitcode != 0:
            exitcode = self.replication_client.exitcode
            self.logger.error(('ReplicationClient terminated with code %s'
                              ) % str(self.replication_client.exitcode))

        # TableCopier can be None if we resume replication only
        while self.table_copier is not None and self.table_copier.is_alive():
            if not self.is_alive:
                self.logger.info('Waiting for TableCopier')
            time.sleep(3)

        if self.table_copier is not None and self.table_copier.exitcode != 0:
            self.logger.error(('TableCopier terminated with code %s'
                              ) % str(self.table_copier.exitcode))

        self.logger.info("Done")
        return exitcode


class TableCopier(object):
    def __init__(self, src_dsn, dst_dsn, bucket, table, debug=False, pause_file=None, 
                 stop_file=None, chunk_size=1000, replica_dsns=[], max_lag=0, 
                 use_inout_file=True, checksum=False):
        self.bucket = bucket
        self.table = table
        self.logger = sm_create_logger(debug, 'TableCopier (%s)' % self.table)
        self.pk = None
        self.columns_str = None
        self.columns_arr = None
        self.columns_dict = None
        self.columns_outfile = None
        self.columns_infile = None
        self.columns_set = None
        self.colcount = 0
        self.status = 0
        self.minpk = None
        self.maxpk = None
        self.pause_file = pause_file
        self.stop_file = stop_file
        self.chunk_size = chunk_size
        self.lastpk = 0
        self.metrics = {}
        self.is_alive = True
        self.src_dsn = src_dsn
        self.dst_dsn = dst_dsn
        self.dst_dsn['db'] = self.bucket
        self.mysql_src = None
        self.mysql_dst = None
        self.mysql_replicas = {}
        self.replica_dsns = replica_dsns
        self.max_lag = max_lag
        self.use_inout_file = use_inout_file
        self.inout_file_tsv = None
        self.copy_chunk_func = self.copy_chunk_inout_file
        self.checksum = checksum
        self.row_checksum = None
        self.chunk_sql = None

    def signal_handler(self, signal, frame):
        self.logger.info("Signal caught (%s), cleaning up" % str(signal))
        self.is_alive = False

    def log_event_metrics(self, start=False, rows=0, frompk=None, topk=None, 
                          commit_time=0.0, chunk_time=0.0):
        if start:
            self.metrics['timer'] = time.time()
            self.metrics['rows'] = 0
            self.metrics['commit_time'] = 0.0
            self.metrics['chunk_time'] = 0.0
            self.metrics['frompk'] = frompk
            self.metrics['topk'] = topk
            return True

        now = time.time()
        self.metrics['topk'] = topk
        self.metrics['rows'] += rows
        self.metrics['commit_time'] += commit_time
        self.metrics['chunk_time'] += chunk_time

        if (now - self.metrics['timer']) >= 10:
            self.logger.info(('Copy from: %d, to: %d, rows copied: %d, chunk_time: %.3f secs, '
                              'commit time: %.3f secs') % (self.metrics['frompk'], self.metrics['topk'], 
                                                           self.metrics['rows'], self.metrics['chunk_time'], 
                                                           self.metrics['commit_time']))
            self.log_event_metrics(start=True, rows=0, frompk=self.metrics['topk']+1, topk=self.metrics['topk']+1)

        return True

    def connect_source(self):
        self.mysql_src = MySQLConnection(self.src_dsn, 'TableCopier, %s, src' % self.table)
        self.logger.debug("Source character set: %s" % self.mysql_src.charset)

    def connect_target(self):
        self.mysql_dst = MySQLConnection(self.dst_dsn, 'TableCopier, %s, dst' % self.table, 
                                         allow_local_infile=self.use_inout_file)
        self.logger.debug("Target character set: %s" % self.mysql_dst.charset)

    def connect_replicas(self):
        self.logger.debug(self.replica_dsns)
        if len(self.replica_dsns) == 0:
            return True
        
        for dsn in self.replica_dsns:
            key = '%s:%d' % (dsn['host'], dsn['port'])
            self.mysql_replicas[key] = MySQLConnection(dsn, 'TableCopier, replmonitor, dst')
            self.logger.info('Connected to target replica %s:%d' % (dsn['host'], dsn['port']))

        return True

    def setup_for_checksum(self):
        self.mysql_src.query(sql_schemigrator_checksums)

        if self.columns_dict is None:
            self.get_table_columns()

        types_str = ['char','varchar','binary','varbinary','blob','text','enum','set','json']
        rows_checksum = []
        for column in self.columns_dict:
            col = ''
            if column['data_type'].lower() in types_str:
                col = 'CONVERT(`%s` USING binary)' % column['col']
                if column['is_nullable'].lower() != 'no':
                    col = 'COALESCE(%s, "")' % col
            else:
                col = '`%s`' % column['col']

            rows_checksum.append(col)

        self.row_checksum = ("COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) "
                             "AS UNSIGNED)), 10, 16)), 0)") % ', '.join(rows_checksum)

        self.chunk_sql = ("REPLACE INTO schemigrator_checksums "
                          "(db, tbl, chunk, chunk_index, lower_boundary, upper_boundary, "
                          "master_cnt, master_crc) "
                          "SELECT '{0}', '{1}', %s, '{2}', %s, %s, COUNT(*) AS cnt, {3} AS crc "
                          "FROM `{1}` FORCE INDEX(`PRIMARY`) "
                          "WHERE ((`{2}` >= %s)) AND ((`{2}` <= %s))").format(self.bucket, self.table, 
                                                                              self.pk, self.row_checksum)
        
        return True

    def max_replica_lag(self, max_lag=60):
        max_sbm = None
        sbm = 0
        replica = None

        for replica in self.mysql_replicas.keys():
            if not self.mysql_replicas[replica].conn.is_connected():
                self.logger.warning('Lost connection to replica %s, attempting reconnect' % replica)
                self.mysql_replicas[replica].conn.reconnect(attempts=5, delay=2)

            sbm = self.mysql_replicas[replica].seconds_behind_master()
            """ We can short circuit here given max_lag value but we don't to keep the connection
            open, otherwise we have to implement a keepalive somewhere
            """
            if sbm is not None and (max_sbm is None or sbm > max_sbm):
                max_sbm = sbm

        return max_sbm, replica

    def set_tsv_file(self):
        inout_file_tsv = self.mysql_src.get_variable('secure_file_priv')

        if inout_file_tsv == 'NULL':
            self.logger.error('Source server does not support secure_file_priv')
            return False
        elif inout_file_tsv == '':
            inout_file_tsv = '/tmp/schemigrator-chunk-%s.%s.tsv' % (self.bucket, self.table)
            self.logger.info('Using %s as chunk TSV INFILE/OUTFILE' % inout_file_tsv)
        else:
            try:
                with open(os.path.join(inout_file_tsv, 'test.tsv'), 'w') as testfd:
                    testfd.write('schemigrator')

                testfd.close()
            except Exception as err:
                self.logger.error("Unable to write on secure_file_priv directory %s" % inout_file_tsv)
                self.logger.error(str(err))
                return False

            inout_file_tsv = os.path.join(inout_file_tsv, 
                                          'schemigrator-chunk-%s.%s.tsv' % (self.bucket, self.table))
            self.logger.info('Using %s as chunk TSV INFILE/OUTFILE' % inout_file_tsv)

        return inout_file_tsv

    def get_checkpoint(self):
        """ Read from dest server current checkpoint position for the table """
        # Checkpoint is none when table has not been started before
        checkpoint = self.mysql_dst.fetchone('SELECT * FROM schemigrator_checkpoint WHERE tbl = "%s"' % self.table)
        if checkpoint is not None:
            self.status = checkpoint['status']

        return checkpoint

    def set_checkpoint(self, cursor, lastpk, status=1):
        sql = ('REPLACE INTO schemigrator_checkpoint (tbl, minpk, maxpk, lastpk, status) '
               'VALUES(%s, %s, %s, %s, %s)')
        vals = (self.table, self.minpk, self.maxpk, lastpk, status)
        cursor.execute(self.mysql_dst.sqlize(sql), vals)

    def get_table_primary_key(self):
        sql = ('SELECT column_name AS col FROM information_schema.key_column_usage '
               'WHERE table_schema = "{0}" AND table_name = "{1}" AND constraint_name = "PRIMARY"')
        pk_columns = self.mysql_src.query(sql.format(self.bucket, self.table))

        if len(pk_columns) == 0:
            raise Exception('Table %s has no PRIMARY KEY defined' % self.table)
        elif len(pk_columns) > 1:
            raise Exception('Table %s has multiple PRIMARY KEY columns' % self.table)

        self.pk = pk_columns[0]['col']

        return True

    def get_table_columns(self):
        types_str = ['char', 'varchar', 'text', 'enum', 'set', 'json']
        sql = ('SELECT column_name AS col, is_nullable, data_type '
               'FROM information_schema.columns '
               'WHERE table_schema = "{0}" AND table_name = "{1}" '
               'ORDER BY ordinal_position')
        columns = self.mysql_src.query(sql.format(self.bucket, self.table))

        if columns is None:
            raise Exception('Table %s has no columns defined' % self.table)

        colarr = []
        columns_outfile = []
        columns_infile = []
        columns_set = []
        self.colcount = len(columns)
        for column in columns:
            colarr.append(column['col'])
            if column['data_type'] in types_str:
                columns_outfile.append('HEX(CONVERT(`{0}` USING binary))'.format(escape(column['col'])))
                columns_infile.append('@{0}'.format(column['col']))
                columns_set.append('`{0}`=UNHEX(@{1})'.format(escape(column['col']), column['col']))
            else:
                columns_infile.append('`{0}`'.format(escape(column['col'])))
                columns_outfile.append('`{0}`'.format(escape(column['col'])))

        self.columns_outfile = ','.join(columns_outfile)
        if len(columns_infile) > 0:
            self.columns_infile = ','.join(columns_infile)
        else:
            self.columns_infile = ''

        if len(columns_set) > 0:
            self.columns_set = 'SET %s' % ','.join(columns_set)
        else:
            self.columns_set = ''

        self.columns_str = list_to_col_str(colarr)
        self.columns_arr = colarr
        self.columns_dict = columns

        return True        

    def get_min_max_range(self):
        """ Identify min max PK values we should only operate from """
        sql = ('SELECT COALESCE(MIN({0}), 0) AS minpk, COALESCE(MAX({0}), 0) AS maxpk '
               'FROM {1}').format(self.pk, self.table)
        pkrange = self.mysql_src.fetchone(sql)
        self.logger.debug('PK range for %s based on source %s' % (self.table, str(pkrange)))
        
        return pkrange['minpk'], pkrange['maxpk']

    def get_next_chunk_boundary(self):
        """ Identify next range of rows """
        pass

    def checksum_chunk(self, minpk, maxpk):
        self.logger.debug('Checksum %d > %d' % (minpk, maxpk))
        self.mysql_src.query(self.chunk_sql, (minpk, minpk, maxpk, minpk, maxpk, ))

    def execute_chunk_trx_select(self, cursor, sql, rows, topk):
        ts = None
        commit_ts = None

        try:
            """
            execute/executemany is slow - for 10k rows, cProfile timing below
               ncalls  tottime  percall  cumtime  percall filename:lineno(function)
                10000    0.102    0.000   20.048    0.002 cursor.py:515(execute)
                    1    0.027    0.027   20.084   20.084 cursor.py:631(executemany)
            """
            cursor.execute('BEGIN')
            cursor.executemany(sql, rows)
            self.set_checkpoint(cursor, topk+1, status=1)
            ts = time.time()
            cursor.execute('COMMIT')
            commit_ts = time.time() - ts
            self.status = 1
        except mysql.connector.Error as err:
            if err.errno in [errorcode.ER_LOCK_DEADLOCK, errorcode.ER_LOCK_WAIT_TIMEOUT]:
                self.logger.warn(str(err))
                return err.errno, commit_ts
            else: 
                self.logger.error(str(err))
                tb = traceback.format_exc().splitlines()
                for l in tb:
                    self.logger.error(l)
                return 1, commit_ts

        return 0, commit_ts

    def execute_chunk_trx_infile(self, cursor, sql):
        commit_ts = None

        try:
            commit_ts = time.time()
            cursor.execute(sql)
            commit_ts = time.time() - commit_ts
            self.status = 1
        except mysql.connector.Error as err:
            if err.errno in [errorcode.ER_LOCK_DEADLOCK, errorcode.ER_LOCK_WAIT_TIMEOUT]:
                self.logger.error(str(err))
                return err.errno, commit_ts
            else:
                self.logger.error(str(err))
                self.logger.error(sql)
                backtrace = traceback.format_exc().splitlines()
                for line in backtrace:
                    self.logger.debug(line)
                return err.errno, commit_ts

        return 0, commit_ts

    def copy_chunk_select(self, cursor, frompk, topk):
        """ Copy chunk specified by range using cursor.executemany """
        sql = ('SELECT /* SQL_NO_CACHE */ %s FROM %s '
               'WHERE %s BETWEEN %d AND %d') % (self.columns_str, self.table, self.pk, frompk, topk)
        rows = self.mysql_src.query_array(sql)
        rows_count = 0

        if not self.mysql_src.rowcount:
            """ Return non-zero as 0.0 is boolean False """
            return 0.0001, rows_count

        rows_count = self.mysql_src.rowcount

        vals = ', '.join(['%s'] * self.colcount)
        sql = 'INSERT IGNORE INTO %s (%s) VALUES (%s)' % (self.table, self.columns_str, vals)

        # self.logger.debug(sql)
        # self.logger.debug(rows)

        retries = 0
        while True:
            if not self.is_alive:
                return False, False

            code, commit_time = self.execute_chunk_trx_select(cursor, sql, rows, topk)
            if code > 0:
                if code == 1: 
                    return False, False
                elif code > 1200:
                    self.logger.info('Chunk copy transaction failed, retrying from %d to %d' % (frompk, topk))
                    retries += 1

                if retries >= 3:
                    return False, False

                continue

            break
        
        return commit_time, rows_count

    def copy_chunk_inout_file(self, cursor, frompk, topk):
        """ Copy chunk specified by range using LOAD DATA INFILE """
        if self.inout_file_tsv is None:
            return False, False

        if os.path.exists(self.inout_file_tsv):
            os.unlink(self.inout_file_tsv)

        # print(self.columns_outfile)
        # print(self.columns_infile)
        # print(self.columns_set)
        # sql = ('SELECT /* SQL_NO_CACHE */ %s INTO OUTFILE "%s" CHARACTER SET %s '
        #        'FIELDS TERMINATED BY "\t\t" FROM %s WHERE %s BETWEEN %d AND %d') % (
        #             self.columns_str, self.inout_file_tsv, self.mysql_src.charset,
        #             self.table, self.pk, frompk, topk)

        sql = (('SELECT /* SQL_NO_CACHE */ %s INTO OUTFILE "%s" CHARACTER SET %s '
                'FIELDS TERMINATED BY "\t\t" FROM %s WHERE %s BETWEEN %d AND %d') % (
                    self.columns_outfile, self.inout_file_tsv, self.mysql_src.charset,
                    self.table, self.pk, frompk, topk))

        rows = self.mysql_src.query_array(sql)
        rows_count = 0

        if not self.mysql_src.rowcount:
            """ Return non-zero as 0.0 is boolean False """
            return 0.0001, rows_count

        rows_count = self.mysql_src.rowcount

        # sql = ('LOAD DATA LOCAL INFILE "%s" IGNORE INTO TABLE %s CHARACTER SET %s '
        #        'FIELDS TERMINATED BY "\t\t" (%s)') % (self.inout_file_tsv, self.table,
        #                                               self.mysql_src.charset, self.columns_str)

        sql = (('LOAD DATA LOCAL INFILE "%s" IGNORE INTO TABLE %s CHARACTER SET %s '
                'FIELDS TERMINATED BY "\t\t" (%s) %s') % (self.inout_file_tsv, self.table,
                                                          self.mysql_src.charset, self.columns_infile,
                                                          self.columns_set))

        retries = 0
        while True:
            if not self.is_alive:
                return False, False

            code, commit_time = self.execute_chunk_trx_infile(cursor, sql)
            if code > 0:
                if code in [errorcode.ER_LOCK_DEADLOCK, errorcode.ER_LOCK_WAIT_TIMEOUT]:
                    self.logger.info('Chunk copy transaction failed, retrying from %d to %d' % (frompk, topk))
                    retries += 1
                else:
                    return False, False

                if retries >= 3:
                    return False, False

                continue

            self.set_checkpoint(cursor, topk+1, status=1)
            break
        
        return commit_time, rows_count

    def start_copier(self):
        self.connect_source()
        self.connect_target()

        checkpoint = self.get_checkpoint()
        if checkpoint['status'] == 2:
            self.logger.info('Table has previously completed copy process')
            return 0
        elif checkpoint['status'] > 2:
            self.logger.info('Table had a previous unrecoverable error, aborting')
            return 1

        self.connect_replicas()
        self.get_table_columns()
        self.get_table_primary_key()
        
        if self.checksum:
            self.setup_for_checksum()

        if not self.use_inout_file:
            self.copy_chunk_func = self.copy_chunk_select
        else:
            self.inout_file_tsv = self.set_tsv_file()
            if not self.inout_file_tsv:
                return 1
        
        commit_time = 0.0
        chunk_time = 0.0
        rows_copied = 0
        nextpk = 0

        cursor = mysql.connector.cursor.MySQLCursorDict(self.mysql_dst.conn)

        if checkpoint is None or self.status == 0:
            self.minpk, self.maxpk = self.get_min_max_range()
            self.lastpk = self.minpk
        else:
            self.minpk = checkpoint['minpk']
            self.maxpk = checkpoint['maxpk']
            self.lastpk = checkpoint['lastpk']
            self.status = checkpoint['status']

        if self.maxpk == 0:
            self.logger.info('Table %s has no rows' % self.table)
            self.set_checkpoint(cursor, 0, status=2)
            return 0
        elif self.maxpk == 1:
            # Artificially inflate maxpk so there is at least one copy loop
            self.maxpk = 2

        if self.lastpk >= self.maxpk and self.status != 2:
            self.logger.info('Table has completed copy, skipping')
            self.set_checkpoint(cursor, self.lastpk, status=2)
            return 0

        max_replica_lag_secs = None
        max_replica_lag_host = None
        max_replica_lag_time = time.time()

        self.logger.info('Starting copy at next PK value %d' % self.lastpk)
        self.log_event_metrics(start=True, rows=0, frompk=self.lastpk, topk=0)
        while self.lastpk < self.maxpk and self.is_alive:
            self.logger.debug('__chunk_loop')

            if os.path.exists(self.pause_file):
                self.logger.info('Paused, remove %s to continue' % self.pause_file)
                time.sleep(5)
                continue

            if os.path.exists(self.stop_file):
                self.logger.info('Stopped via %s file' % self.stop_file)
                break

            nextpk = self.lastpk + (self.chunk_size - 1)

            chunk_time = time.time()
            commit_time, rows_copied = self.copy_chunk_func(cursor, self.lastpk, nextpk)
            chunk_time = time.time() - chunk_time

            if not commit_time:
                # Chunk copy did not really fail on forced termination
                if not self.is_alive:
                    self.logger.error('Chunk copy failed, please check logs and try again')
                    return 1
                break

            if self.checksum:
                self.checksum_chunk(self.lastpk, nextpk)

            self.log_event_metrics(rows=rows_copied, frompk=self.lastpk, topk=nextpk, 
                                   commit_time=commit_time, chunk_time=chunk_time)
            self.lastpk = nextpk + 1

            while True and self.max_lag > 0 and self.is_alive and (time.time() - max_replica_lag_time) > 5 \
                    and len(self.replica_dsns) > 0:
                max_replica_lag_secs, max_replica_lag_host = self.max_replica_lag(max_lag=self.max_lag)

                if max_replica_lag_secs is None:
                    self.logger.warning('None of the replicas has Seconds_Behind_Master, paused')
                    time.sleep(5)
                elif max_replica_lag_secs > self.max_lag:
                    self.logger.warning('Replica lag is %d on %s, paused' % (max_replica_lag_secs, max_replica_lag_host))
                    time.sleep(5)
                else:
                    max_replica_lag_time = time.time()
                    break

        if self.lastpk >= self.maxpk:
            self.logger.info('Copying %s complete!' % self.table)
            self.set_checkpoint(cursor, self.lastpk, status=2)
        else:
            self.logger.info('Stopping copy at next PK value %d' % self.lastpk)

        return 0

    def run(self):
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.logger.info('My PID is %d' % os.getpid())
        self.logger.info("Copying table %s" % self.table)

        lost_connection_backoff = 10
        last_connection_failure = time.time()

        try:
            while True:
                try:
                    return self.start_copier()
                except mysql.connector.Error as err:
                    self.logger.warning('Stopping copy at next PK value %d' % self.lastpk)
                    if err.errno in [errorcode.CR_SERVER_LOST_EXTENDED, errorcode.CR_CONN_HOST_ERROR,
                                     errorcode.CR_SERVER_LOST]:
                        self.logger.warning(str(err))
                        if lost_connection_backoff >= 600:
                            self.logger.error('Too many connection failures, giving up')
                            return 2

                        if (time.time()-last_connection_failure) > 100:
                            lost_connection_backoff = 10

                        self.logger.info('Waiting %d seconds to reconnect' % lost_connection_backoff)
                        time.sleep(lost_connection_backoff)
                        lost_connection_backoff += lost_connection_backoff
                        last_connection_failure = time.time()
                    else:
                        raise err
        except Exception as err:
            self.logger.error(str(err))
            backtrace = traceback.format_exc().splitlines()
            for line in backtrace:
                self.logger.error(line)
            return 1

        return 0


class ReplicationClient(object):
    def __init__(self, src_dsn, dst_dsn, bucket, binlog_fil=None, binlog_pos=None, 
                 debug=False, pause_file=None, stop_file=None, chunk_size=1000, 
                 replica_dsns=[], max_lag=0, checksum=False, checkpoint_only=False):
        # Sync to checkpoint every chunk completion
        self.bucket = bucket
        self.src_dsn = src_dsn
        self.dst_dsn = dst_dsn
        self.binlog_fil = binlog_fil
        self.binlog_pos = binlog_pos
        self.checkpoint_next_binlog_fil = None
        self.checkpoint_next_binlog_pos = None
        self.checkpoint_committed_binlog_fil = None
        self.checkpoint_committed_binlog_pos = None
        self.debug = debug
        self.logger = sm_create_logger(debug, 'ReplicationClient')
        self.is_alive = True
        self.pause_file = pause_file
        self.stop_file = stop_file
        self.chunk_size = chunk_size
        self.pkcols = {}
        self.columns_str = {}
        self.columns_arr = {}
        self.columns_dict = {}
        self.trx_size = 0
        self.trx_open = False
        self.trx_open_ts = time.time()
        self.mysql_dst = None
        self.metrics = {}
        self.mysql_replicas = {}
        self.replica_dsns = replica_dsns
        self.max_lag = max_lag
        self.checksum = checksum
        self.checkpoint_only = checkpoint_only
        self.row_checksum = {}
        self.chunk_sql = {}
        self.backoff_counter = 1
        self.backoff_last_ts = None
        self.backoff_last_fil = None
        self.backoff_last_pos = None
        self.backoff_elapsed = 0

    def signal_handler(self, signal, frame):
        self.logger.info("Signal caught (%s), cleaning up" % str(signal))
        self.logger.info("It may take a few seconds to teardown, you can also kill -9 %d" % os.getpid())
        self.is_alive = False

    def sizeof_fmt(self, num, suffix='B'):
        for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, 'Yi', suffix)

    def log_event_metrics(self, start=False, binlog_fil=None, binlog_pos=None):
        if start:
            self.metrics['timer'] = time.time()
            self.metrics['timer_copy'] = time.time()
            self.metrics['events'] = 0
            self.metrics['rows'] = 0
            self.metrics['bytes'] = 0
            self.metrics['binlog_pos_last'] = 0
            self.metrics['commit_size'] = 0
            self.metrics['commit_time'] = 0.0
            return True

        now = time.time()
        self.metrics['events'] += 1

        if self.metrics['binlog_pos_last'] == 0:
            self.metrics['binlog_pos_last'] = binlog_pos
        elif self.metrics['binlog_pos_last'] > binlog_pos:
            self.metrics['bytes'] += (self.metrics['bytes'] + binlog_pos)
        else:
            self.metrics['bytes'] += (binlog_pos - self.metrics['binlog_pos_last'])

        self.metrics['binlog_pos_last'] = binlog_pos

        if (now - self.metrics['timer']) >= 10:
            self.logger.info('Status: %s:%s, %d events/r (%s), %d rows/w, %.4f lat (ms)' % (
                binlog_fil, binlog_pos, self.metrics['events'], self.sizeof_fmt(self.metrics['bytes']),
                self.metrics['rows'], (self.metrics['commit_time']*1000)))
            self.metrics['timer'] = now
            self.metrics['rows'] = 0
            self.metrics['events'] = 0
            self.metrics['bytes'] = 0
            self.metrics['binlog_pos_last'] = 0
            self.metrics['commit_size'] = 0
            self.metrics['commit_time'] = 0.0

        if (now - self.metrics['timer_copy']) >= 60:
            statuses = self.list_tables_status()
            if self.checksum:
                checksum = self.list_checksum_status()
                overall = self.list_overall_status()
                checksum_state = ', %d bad chksums' % checksum if checksum > 0 else ', no chksum errors'
                overall_state = ', migration in progress' if overall > 0 else ', migration COMPLETE'
            else:
                checksum_state = ''
                overall_state = ''
            self.logger.info("Tables: %d not started, %d in progress, %d complete, %d error%s%s" %(
                statuses['not_started'], statuses['in_progress'], statuses['complete'], statuses['error'],
                checksum_state, overall_state
            ))
            self.metrics['timer_copy'] = now

        return True

    def connect_target(self):
        if self.mysql_dst is None or not self.mysql_dst.conn.is_connected():
            self.mysql_dst = MySQLConnection(self.dst_dsn, 'ReplicationClient, applier, dst')

    def connect_replicas(self):
        self.logger.debug(self.replica_dsns)
        if len(self.replica_dsns) == 0:
            return True
        
        for dsn in self.replica_dsns:
            key = '%s:%d' % (dsn['host'], dsn['port'])
            self.mysql_replicas[key] = MySQLConnection(dsn, 'ReplicationClient, replmonitor, dst')
            self.logger.info('Connected to target replica %s:%d' % (dsn['host'], dsn['port']))

        return True

    def setup_for_checksum(self, table):
        sql = None
        types_str = ['char', 'varchar', 'binary', 'varbinary', 'blob', 'text', 'enum', 'set', 'json']
        rows_checksum = []
        for column in self.columns_dict[table]:
            col = ''
            if column['data_type'].lower() in types_str:
                col = 'CONVERT(`%s` USING binary)' % column['col']
                if column['is_nullable'].lower() != 'no':
                    col = 'COALESCE(%s, "")' % col
            else:
                col = '`%s`' % column['col']

            rows_checksum.append(col)

        sql = ("COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) "
               "AS UNSIGNED)), 10, 16)), 0)")
        self.row_checksum[table] = sql % ', '.join(rows_checksum)

        sql = ("REPLACE INTO schemigrator_checksums "
               "(db, tbl, chunk, chunk_index, lower_boundary, upper_boundary, "
               "master_cnt, master_crc, this_cnt, this_crc) "
               "SELECT '{0}', '{1}', %s, '{2}', %s, %s, %s, %s, COUNT(*) AS cnt, {3} AS crc "
               "FROM `{1}` FORCE INDEX(`PRIMARY`) "
               "WHERE ((`{2}` >= %s)) AND ((`{2}` <= %s))")
        self.chunk_sql[table] = sql.format(self.bucket, table, self.pkcols[table], 
                                           self.row_checksum[table])

        return True

    def max_replica_lag(self, max_lag=60):
        max_sbm = None
        sbm = 0
        replica = None

        for replica in self.mysql_replicas.keys():
            if not self.mysql_replicas[replica].conn.is_connected():
                self.logger.warning('Lost connection to replica %s, attempting reconnect' % replica)
                self.mysql_replicas[replica].conn.reconnect(attempts=5, delay=2)

            sbm = self.mysql_replicas[replica].seconds_behind_master()
            """ We can short circuit here given max_lag value but we don't to keep the connection
            open, otherwise we have to implement a keepalive somewhere
            """
            if sbm is not None and (max_sbm is None or sbm > max_sbm):
                max_sbm = sbm

        return max_sbm, replica

    def list_bucket_tables(self, from_source=True):
        sql = ('SELECT TABLE_NAME AS tbl FROM INFORMATION_SCHEMA.TABLES '
               'WHERE ENGINE="InnoDB" AND TABLE_TYPE="BASE TABLE" AND '
               'TABLE_SCHEMA="%s"' % self.bucket)

        if from_source:
            mysql_src = MySQLConnection(self.src_dsn, 'ReplicationClient, src')
            rows = mysql_src.query(sql)
            mysql_src.close()
        else:
            rows = self.mysql_dst.query(sql)

        if len(rows) == 0:
            if from_source:
                self.logger.error('Source bucket has no tables!')
            return False

        tables = []
        for row in rows:
            tables.append(row['tbl'])

        return tables

    def list_tables_status(self):
        sql = "SELECT status, count(*) as status_group FROM schemigrator_checkpoint GROUP BY status"
        self.connect_target()
        statuses = {'not_started': 0, 'in_progress': 0, 'complete': 0, 'error': 0}
        tables = self.mysql_dst.query_dict(sql)

        if tables is None:
            return statuses

        for status in tables:
            if status['status'] == 0:
                statuses['not_started'] = status['status_group']
            elif status['status'] == 1:
                statuses['in_progress'] = status['status_group']
            elif status['status'] == 2:
                statuses['complete'] = status['status_group']
            elif status['status'] > 2:
                statuses['error'] = status['status_group']

        return statuses

    def list_checksum_status(self):
        self.connect_target()
        checksums = self.mysql_dst.fetchone(sql_checksum_results_bad, 'chunks')
        if checksums is None:
            return 0

        return checksums

    def list_overall_status(self):
        self.connect_target()
        checksums = self.mysql_dst.query_array(sql_copy_checksum_progress)
        if checksums is None:
            return 0

        return len(checksums)

    def get_table_primary_key(self, table):
        mysql_src = MySQLConnection(self.src_dsn, 'ReplicationClient, %s, src' % table)
        sql = ('SELECT column_name AS col FROM information_schema.key_column_usage '
               'WHERE table_schema = "{0}" AND table_name = "{1}" AND constraint_name = "PRIMARY"')
        pk_columns = mysql_src.query(sql.format(self.bucket, table))

        if len(pk_columns) == 0:
            raise Exception('Table %s has no PRIMARY KEY defined' % table)
        elif len(pk_columns) > 1:
            raise Exception('Table %s has multiple PRIMARY KEY columns' % table)

        mysql_src.close()
        return pk_columns[0]['col']

    def get_table_columns(self, table):
        mysql_src = MySQLConnection(self.src_dsn, 'ReplicationClient, %s, src' % table)
        sql = ('SELECT column_name AS col, is_nullable, data_type '
               'FROM information_schema.columns '
               'WHERE table_schema = "{0}" AND table_name = "{1}" '
               'ORDER BY ordinal_position')
        columns = mysql_src.query(sql.format(self.bucket, table))

        if columns is None or len(columns) == 0:
            raise Exception('Table %s has no columns defined' % table)

        colarr = []
        self.colcount = len(columns)
        for column in columns:
            colarr.append(column['col'])

        self.columns_str[table] = list_to_col_str(colarr)
        self.columns_arr[table] = colarr
        self.columns_dict[table] = columns
        mysql_src.close()

        return True

    def checkpoint_write(self, checkpoint=None):
        """ Write last executed file and position
        Replication checkpoints should be within the same trx as the applied
        events so that in case of rollback, the checkpoint is also consistent
        """
        if checkpoint is None:
            checkpoint = (self.bucket, self.checkpoint_next_binlog_fil,
                          self.checkpoint_next_binlog_pos)
        
        sql = 'REPLACE INTO schemigrator_binlog_status (bucket, fil, pos) VALUES (%s, %s, %s)'
        cursor = mysql.connector.cursor.MySQLCursorDict(self.mysql_dst.conn)
        cursor.execute(sql, checkpoint)
        cursor.close()
        self.trx_size += 1
        self.checkpoint_committed_binlog_fil = checkpoint[1]
        self.checkpoint_committed_binlog_pos = checkpoint[2]

    def checkpoint_read(self):
        # Read last checkpoint to resume from
        # otherwise just start replication from recent file/pos
        pass

    def checksum_chunk(self, cursor, values):
        """ Calculate checksum based on row event from binlog stream """
        if not self.checksum:
            return True

        vals = (values['chunk'], values['lower_boundary'], values['upper_boundary'],
                values['master_cnt'], values['master_crc'], values['lower_boundary'],
                values['upper_boundary'],)
        self.mysql_dst.query(self.chunk_sql[values['tbl']], vals)

        return True

    def begin_apply_trx(self):
        self.mysql_dst.conn.start_transaction()
        # We track the time the trx is opened, we do not want to keep a transaction open
        # for a very long time even if the number of row events is less than chunk-size
        self.trx_open_ts = time.time()
        self.trx_open = True
        self.logger.debug('>>>>>>>>>>>>>>>>>>>>>>>> BEGIN')
        return True

    def commit_apply_trx(self):
        commit_time = 0.0
        commit_time_start = 0.0
        commit_time_size = 0

        commit_time_start = time.time()
        self.mysql_dst.conn.commit()
        commit_time = time.time() - commit_time_start
        commit_time_size = self.trx_size
        self.metrics['commit_size'] += self.trx_size
        self.metrics['commit_time'] += commit_time
        self.trx_size = 0
        self.trx_open = False
        self.logger.debug('>>>>>>>>>>>>>>>>>>>>>>>> COMMIT')

    def rollback_apply_trx(self):
        if self.mysql_dst.conn.in_transaction:
            self.mysql_dst.conn.rollback()
            self.logger.debug('>>>>>>>>>>>>>>>>>>>>>>>> ROLLBACK')
        self.trx_size = 0
        self.trx_open = False

    def update(self, cursor, table, values):
        set_vals = {}
        where_vals = {}

        for k in values['after_values'].keys():
            if values['after_values'][k] is not None:
                set_vals[k] = values['after_values'][k]

        for k in values['before_values'].keys():
            if values['before_values'][k] is not None:
                where_vals[k] = values['before_values'][k]

        set_pairs = '`{0}`=%s'.format('`=%s,`'.join(set_vals.keys()))
        where_pairs = '`{0}`=%s'.format('`=%s AND `'.join(where_vals.keys()))
        sql = 'UPDATE `%s` SET %s WHERE %s' % (table, set_pairs, where_pairs)

        # print(sql)
        # print(set_pairs, where_pairs)
        # print(tuple(set_vals.values()) + tuple(where_vals.values()))
        # print('---------------------------------------------------')

        try:
            cursor.execute(self.mysql_dst.sqlize(sql), tuple(set_vals.values()) + tuple(where_vals.values()))
        except mysql.connector.errors.ProgrammingError as err:
            self.logger.error(sql)
            self.logger.error(err.errno)
            self.logger.error(values)
            raise err
        except mysql.connector.errors.IntegrityError as err:
            self.logger.error(sql)
            self.logger.error(err.errno)
            self.logger.error(values)
            raise err

        self.trx_size += 1

    def insert(self, cursor, table, values, columns):
        keys = values['values'].keys()
        set_values = {}
        # We do this so that binlog_row_image = MINIMAL works
        for key in keys:
            if values['values'][key] is not None:
                set_values[key] = values['values'][key]

                # Handles columns encoded differently that connection charset
                # `Key` varchar(1024) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL
                if columns[key].character_set_name != self.mysql_dst.charset and \
                        columns[key].type in [VARCHAR, STRING, VAR_STRING, JSON] and \
                        isinstance(values['values'][key], str):
                    set_values[key] = values['values'][key].encode(columns[key].character_set_name)

        sql = 'REPLACE INTO `%s` (`%s`) VALUES (%s)' % (table, '`,`'.join(set_values.keys()),
                                                        ','.join(['%s'] * len(set_values)))
        # print(sql)
        # print(tuple(set_values.values()))
        # print('---------------------------------------------------')

        try:
            cursor.execute(self.mysql_dst.sqlize(sql), tuple(set_values.values()))
        except mysql.connector.errors.ProgrammingError as err:
            self.logger.error(sql)
            self.logger.error(values)
            raise err
        except mysql.connector.errors.IntegrityError as err:
            self.logger.error(sql)
            self.logger.error(values)
            self.logger.error(err.errno)
            raise err

        self.trx_size += 1

    def delete(self, cursor, table, values):
        where_vals = {}

        for k in values['values'].keys():
            if values['values'][k] is not None:
                where_vals[k] = values['values'][k]

        where_pairs = '`{0}`=%s'.format('`=%s AND `'.join(where_vals.keys()))
        sql = 'DELETE FROM `{0}` WHERE {1}'.format(table, where_pairs)

        # print(sql)
        # print(where_pairs)
        # print(tuple(where_vals.values()))
        # print('---------------------------------------------------')

        try:
            cursor.execute(self.mysql_dst.sqlize(sql), tuple(where_vals.values()))
        except mysql.connector.errors.ProgrammingError as err:
            self.logger.error(sql)
            self.logger.error(values)
            raise err

        self.trx_size += 1

    def checkpoint_begin(self):
        return self.begin_apply_trx()

    def checkpoint_end(self, fil=None, pos=None, force=False):
        if force or (self.trx_size >= self.chunk_size or (time.time() - self.trx_open_ts) >= 5):
            self.checkpoint_write()
            if self.trx_open:
                self.commit_apply_trx()

        if fil is not None:
            self.log_event_metrics(binlog_fil=fil, binlog_pos=pos)

    def halt_or_pause(self):
        if not self.is_alive:
            self.logger.info('Terminating binlog event processing')
            return False

        if self.stop_file is not None and os.path.exists(self.stop_file):
            self.logger.info('Stopped via %s file' % self.stop_file)
            self.is_alive = False
            return False

        if self.pause_file is not None:
            while os.path.exists(self.pause_file):
                self.logger.info('Paused, remove %s to continue' % self.pause_file)
                time.sleep(5)
                continue

        if self.checkpoint_only:
            self.logger.error('Checkpoint mode only, halting!')
            self.is_alive = False
            return False

        return True

    def sleep_timer(self, duration, interval=2):
        slept = 0
        while slept <= duration:
            time.sleep(interval)
            slept += interval
            if not self.halt_or_pause():
                return False

        return True

    def backoff_reset(self):
        self.backoff_elapsed = 0
        self.backoff_counter = 0
        self.backoff_last_ts = None
        self.backoff_last_fil = None
        self.backoff_last_pos = None

    def evaluate_backoff(self):
        # If backoff is triggered 3 times in succession, we provide space to table copier
        # and avoid lengthy deadlock conditions which can cause the process to get into
        # an infinite circular deadlock situation and not progressing.
        if self.backoff_counter < 10:
            self.backoff_counter += 1

            if self.backoff_last_ts is None:
                self.backoff_last_ts = time.time()
                self.backoff_elapsed = 10
                return True

            if self.backoff_last_fil != self.checkpoint_committed_binlog_fil or \
                    self.backoff_last_pos != self.checkpoint_committed_binlog_pos:
                self.logger.debug('Backoff reset')
                self.backoff_reset()
                self.backoff_last_fil = self.checkpoint_committed_binlog_fil
                self.backoff_last_pos = self.checkpoint_committed_binlog_pos
                return True

            self.backoff_elapsed = (time.time()-self.backoff_last_ts)+(self.backoff_elapsed*1.2)
            self.logger.debug('Backoff increment, %f seconds, counter %d' % (
                round(self.backoff_elapsed, 2), self.backoff_counter))
            self.logger.info('Backoff triggered, sleeping %d seconds' % round(self.backoff_elapsed, 2))
            if not self.sleep_timer(self.backoff_elapsed):
                return False
        else:
            self.logger.warning('Backing off from 3 consecutive deadlock/lock wait timeouts')
            self.logger.warning('Replication backoff for %s seconds' % str(self.backoff_elapsed*3))
            if not self.sleep_timer(self.backoff_elapsed*3):
                return False
            self.backoff_reset()

        return True

    def start_stream_reader(self, stream):
        max_replica_lag_secs = None
        max_replica_lag_host = None
        max_replica_lag_time = time.time()
        # Marks if a binlog event went to a commit, this helps us roll over properly
        # a new GTID event if in case we want to resume replication manually
        xid_event = False

        # Explicit rollback in case we are resuming from previous error
        self.rollback_apply_trx()

        for binlogevent in stream:
            self.binlog_pos = int(stream.log_pos)
            self.binlog_fil = stream.log_file
            
            if isinstance(binlogevent, XidEvent):
                xid_event = True
                # At this point, the binlog pos has advanced to the next one, which is kind of weird
                # so we grab the checkpoint positions here immediately after and next GTID event
                # we probably can also use end_log_pos to avoid using continue clause below
                self.checkpoint_next_binlog_fil = self.binlog_fil
                self.checkpoint_next_binlog_pos = self.binlog_pos

                """ Short circuit close trx otherwise without matching events it can hold
                trx open infinitely """
                if not self.trx_open:
                    self.checkpoint_begin()
                self.checkpoint_end(fil=stream.log_file, pos=stream.log_pos)

                if not self.halt_or_pause():
                    break

                continue

            """ We put the commit apply here as we need to capture the NEXT
            binlog event file and position into the checkpoint table instead
            of the previous one as long as the COMMIT here succeeds
            """
            if xid_event:
                self.checkpoint_end()
                xid_event = False

            if not self.halt_or_pause():
                break
            
            """ We keep this outside of the bucket check, to make sure that even when there are
            no events for the bucket we checkpoint binlog pos regularly.
            """
            if not self.trx_open and binlogevent.table != 'schemigrator_checksums':
                self.checkpoint_begin()

            cursor = mysql.connector.cursor.MySQLCursorDict(self.mysql_dst.conn)
            columns = {}
            for column in binlogevent.columns:
                columns[column.name] = column

            for row in binlogevent.rows:
                try:
                    if binlogevent.table == 'schemigrator_checksums':
                        if isinstance(binlogevent, WriteRowsEvent):
                            self.checksum_chunk(cursor, row["values"])
                        elif isinstance(binlogevent, UpdateRowsEvent):
                            self.checksum_chunk(cursor, row["after_values"])
                    elif isinstance(binlogevent, DeleteRowsEvent):
                        self.delete(cursor, binlogevent.table, row)
                    elif isinstance(binlogevent, UpdateRowsEvent):
                        self.update(cursor, binlogevent.table, row)
                    elif isinstance(binlogevent, WriteRowsEvent):
                        self.insert(cursor, binlogevent.table, row, columns)

                    self.metrics['rows'] += 1
                    xid_event = False
                except AttributeError as err:
                    self.logger.error(str(err))

                    event = (binlogevent.schema, binlogevent.table, stream.log_file, int(stream.log_pos))
                    tb = traceback.format_exc().splitlines()
                    for l in tb:
                        self.logger.error(l)
                    self.logger.error("Failed on: %s" % str(event))
                    self.is_alive = False
                    return 1
                
                sys.stdout.flush()

            self.log_event_metrics(binlog_fil=stream.log_file, binlog_pos=stream.log_pos)

            while True and self.max_lag > 0 and self.is_alive and (time.time() - max_replica_lag_time) > 5 \
                    and len(self.replica_dsns) > 0:
                max_replica_lag_secs, max_replica_lag_host = self.max_replica_lag(max_lag=self.max_lag)

                if max_replica_lag_secs is None:
                    self.logger.warning('None of the replicas has Seconds_Behind_Master, paused')
                    time.sleep(5)
                elif max_replica_lag_secs > self.max_lag:
                    self.logger.warning('Replica lag is %d on %s, paused' % (max_replica_lag_secs, max_replica_lag_host))
                    time.sleep(5)
                else:
                    max_replica_lag_time = time.time()
                    break

        """ This is here in case there are no binlog events coming in """
        if self.trx_open:
            self.checkpoint_end(fil=stream.log_file, pos=stream.log_pos, force=True)

        return 0

    def start_slave(self, binlog_fil=None, binlog_pos=None):
        self.connect_target()
        self.connect_replicas()

        self.mysql_dst.query('SET SESSION innodb_lock_wait_timeout=5')
        self.mysql_dst.query('SET NAMES %s' % self.src_dsn['charset'])
        self.mysql_dst.query('USE %s' % self.bucket)
        self.dst_dsn['db'] = self.bucket

        this_binlog_fil = self.binlog_fil
        this_binlog_pos = self.binlog_pos
        if binlog_fil is not None:
            this_binlog_fil = binlog_fil
            this_binlog_pos = binlog_pos

        self.checkpoint_committed_binlog_fil = this_binlog_fil
        self.checkpoint_committed_binlog_pos = this_binlog_pos

        if self.checkpoint_only:
            self.checkpoint_write((self.bucket, this_binlog_fil, this_binlog_pos, ))
            self.logger.info('Checkpoint mode only, completed and exiting now')
            return 0

        tables = self.list_bucket_tables(from_source=True)
        for table in tables:
            if table in schemigrator_tables:
                continue
            self.pkcols[table] = self.get_table_primary_key(table)
            if self.checksum:
                self.get_table_columns(table)
                self.setup_for_checksum(table)

        self.logger.info('Starting replication at %s:%d' % (this_binlog_fil, this_binlog_pos))

        stream = BinLogStreamReader(
            connection_settings=self.src_dsn, resume_stream=True,
            server_id=int(time.time()), log_file=this_binlog_fil, log_pos=this_binlog_pos,
            only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent, XidEvent], 
            blocking=False, only_schemas=[self.bucket])

        self.logger.info('Replication client started')

        stream_reader_result = 0
        self.log_event_metrics(start=True)
        while True:
            stream_reader_result = self.start_stream_reader(stream)
            if stream_reader_result > 0 or not self.is_alive:
                break

            """ We made the replication client non-blocking so we have better control
            when the binlog is EOF. Downside is frequent replication connects and 
            disconnects which may show up as annoying in the source error log
            """
            time.sleep(5)

        stream.close()
        return stream_reader_result

    def run(self):
        """ Start binlog replication process """
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.logger.info('My PID is %d' % os.getpid())

        retcode = 1
        lost_connection_backoff = 10
        last_connection_failure = time.time()

        while True:
            try:
                retcode = self.start_slave(self.checkpoint_committed_binlog_fil, 
                                           self.checkpoint_committed_binlog_pos)
            except mysql.connector.Error as err:
                if self.checkpoint_committed_binlog_fil is not None:
                    self.logger.warning('Replication client stopped on %s:%d' % (
                        self.checkpoint_committed_binlog_fil, int(self.checkpoint_committed_binlog_pos)))

                if err.errno in [errorcode.ER_LOCK_DEADLOCK, errorcode.ER_LOCK_WAIT_TIMEOUT]:
                    self.logger.warning(str(err))
                    self.rollback_apply_trx()
                    # Returns False if no self.is_alive
                    try:
                        if not self.evaluate_backoff():
                            retcode = 0
                    except Exception as err:
                        retcode = 0
                elif err.errno in [errorcode.CR_SERVER_LOST_EXTENDED, errorcode.CR_CONN_HOST_ERROR,
                                   errorcode.CR_SERVER_LOST]:
                    self.logger.warning(str(err))
                    backtrace = traceback.format_exc().splitlines()
                    for line in backtrace:
                        self.logger.debug(line)
                    if lost_connection_backoff >= 600:
                        self.logger.error('Too many connection failures, giving up')
                        retcode = 2
                        break

                    if (time.time()-last_connection_failure) > 100:
                        lost_connection_backoff = 10

                    self.logger.info('Waiting %d seconds to reconnect' % lost_connection_backoff)
                    time.sleep(lost_connection_backoff)
                    lost_connection_backoff += lost_connection_backoff
                    last_connection_failure = time.time()
                else:
                    self.logger.error(str(err))
                    backtrace = traceback.format_exc().splitlines()
                    for line in backtrace:
                        self.logger.error(line)
                    retcode = 1
                    break

                self.logger.info('Restarting replication')
            except Exception as err:
                self.logger.error(str(err))
                backtrace = traceback.format_exc().splitlines()
                for line in backtrace:
                    self.logger.error(line)
                retcode = 1
                break

            if retcode == 0:
                break

        if self.checkpoint_committed_binlog_fil is not None:
            self.logger.info('Replication client stopped on %s:%d' % (
                self.checkpoint_committed_binlog_fil, int(self.checkpoint_committed_binlog_pos)))

        return retcode


class MySQLConnection(object):
    def __init__(self, params, header='MySQLConnection', allow_local_infile=False):
        self.conn = mysql.connector.connect(**params, autocommit=True, use_pure=True, 
                                            allow_local_infile=allow_local_infile)
        self.query_header = header
        self.rowcount = None
        self.charset = self.fetchone(
            'SELECT @@session.character_set_connection as charset', 'charset')

    def disconnect(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def sqlize(self, sql):
        sql = '/* %s */ %s' % (self.query_header, sql)
        return sql

    def affected_rows(self):
        return self.conn.affected_rows

    def query(self, sql, args=None):
        results = None
        cursor = mysql.connector.cursor.MySQLCursorDict(self.conn)
        cursor.execute(self.sqlize(sql), args)
        
        if cursor.with_rows:
            results = cursor.fetchall()

        self.rowcount = cursor.rowcount

        cursor.close()
        return results

    def query_array(self, sql, args=None, cursor=None):
        results = None

        if cursor is None:
            cursor = mysql.connector.cursor.MySQLCursor(self.conn)
        cursor.execute(self.sqlize(sql), args)

        if cursor.with_rows:
            results = cursor.fetchall()

        self.rowcount = cursor.rowcount

        cursor.close()
        return results

    def query_dict(self, sql, args=None):
        cursor = mysql.connector.cursor.MySQLCursorDict(self.conn)
        return self.query_array(sql, args=args, cursor=cursor)

    def fetchone(self, sql, column=None):
        results = self.query(sql)
        if len(results) == 0:
            return None

        # logger.debug(results[0])
        if column is not None:
            return results[0][column]

        return results[0]

    def get_variable(self, varname, session=False):
        if session:
            sql = 'SELECT @@session.%s AS v' % varname
        else:
            sql = 'SELECT @@global.%s AS v' % varname

        return self.fetchone(sql, 'v')

    def close(self):
        self.conn.close()

    def seconds_behind_master(self):
        return self.fetchone('SHOW SLAVE STATUS', 'Seconds_Behind_Master')


# http://stackoverflow.com/questions/1857346/\
# python-optparse-how-to-include-additional-info-in-usage-output
class SchemigrateOptionParser(OptionParser):
    def format_epilog(self, formatter):
        return self.epilog


if __name__ == "__main__":
    try:
        signal.signal(signal.SIGTERM, sm_sigterm_handler)
        signal.signal(signal.SIGINT, sm_sigterm_handler)

        logger = None
        opts = None

        opts = sm_buildopts()
        logger = sm_create_logger(opts.debug, 'Schemigrator')
        schemigrator = Schemigrate(opts, logger)
        schemigrator.run()

    except Exception as e:
        if logger is not None:
            if opts is None or (opts is not None and opts.debug):
                tb = traceback.format_exc().splitlines()
                for l in tb:
                    logger.error(l)
            else:
                logger.error(str(e))
        else:
            traceback.print_exc()
        
        sys.exit(1)
