#!/usr/bin/env python3

import logging
import os
import signal
import sys
import time
import traceback
import warnings
import MySQLdb
from multiprocessing import Process, Lock, Manager
from optparse import OptionParser
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)


"""
sudo apt install libmysqlclient-dev python3 python3-pip
pip3 install mysql
"""

VERSION = 0.1
SIGTERM_CAUGHT = False
logger = None

sql_schemigrator_checksums = ("""
CREATE TABLE IF NOT EXISTS schemigrator_checksums (
   db             CHAR(64)     NOT NULL,
   tbl            CHAR(64)     NOT NULL,
   chunk          INT          NOT NULL,
   chunk_time     FLOAT            NULL,
   chunk_index    VARCHAR(200)     NULL,
   lower_boundary TEXT             NULL,
   upper_boundary TEXT             NULL,
   this_crc       CHAR(40)     NOT NULL,
   this_cnt       INT          NOT NULL,
   master_crc     CHAR(40)         NULL,
   master_cnt     INT              NULL,
   ts             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   PRIMARY KEY (db, tbl, chunk),
   INDEX ts_db_tbl (ts, db, tbl)
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
        help='How many rows to copy at a time', default=1000)
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
        help=('Show what the script will be doing instead of actually doing it'),
        default=False)

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
    
    opts.ppid = os.getpid()
    opts.pcwd = os.path.dirname(os.path.realpath(__file__))

    return opts

def sm_create_logger(debug, name):
    logger = logging.getLogger(name)
    logformat = '%(asctime)s <%(process)d> %(levelname)s_[{0}]_:: %(message)s'.format(name.ljust(24))

    if debug:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    logger.setLevel(loglevel)
    formatter = logging.Formatter(logformat)
    streamh = logging.StreamHandler()
    streamh.setFormatter(formatter)
    logger.addHandler(streamh)
    
    return logger

def sm_parse_dsn(dsn):
    dsn_keys = {'h': 'host', 'u':'user', 'P':'port', 'p':'passwd', 
                'S':'socket', 'D': 'db'}
    params = {}
    if len(dsn) == 0:
        raise Exception('Invalid DSN value')

    dsn_parts = dsn.split(',')
    if len(dsn_parts) == 1:
        if not '=' in dsn:
            params['host'] = dsn
            return params

    for dsn_part in dsn_parts:
        kv = dsn_part.split('=')
        if len(kv) != 2:
            raise Exception('Invalid DSN value %s' % str(kv[1]))
        if kv[0] not in dsn_keys:
            raise Exception('Invalid DSN key %s' % str(kv[0]))

        params[dsn_keys[kv[0]]] = kv[1]

    return params 

def sm_copy_dsn(src, dest):
    for k in src:
        if k not in dest:
            dest[k] = src[k]

    return dest


class Schemigrate(object):
    def __init__(self, opts, logger):
        self.opts = opts
        self.logger = logger
        self.mysql_src = None
        self.mysql_dst = None
        self.table_copier = None
        self.replication_client = None
        self.is_alive = True

        # Validations
        #   Able to connect from source and to dest
        #   Create schemigrate_runtime and schemigrate+checkpoint tables
        #       runtime has runtime configuration vars i.e. chunk size, pause
        #       these tables will also be used to validate replication
        #       should we add heartbeat?
        #   Create database if not exists
        #   Start replication client
        #   Continously check if new target is read_only/super_readonly
        #       We should use credentials that has no SUPER

    def signal_handler(self, signal, frame):
        self.logger.info("Signal caught (%s), cleaning up" % str(signal))
        if self.replication_client is not None:
            self.replication_client.terminate()
            self.logger.info("Replication client terminated")

        if self.table_copier is not None:
            if self.table_copier.is_alive():
                self.table_copier.terminate()
            self.logger.info("Replication client terminated")

        self.logger.info("Waiting for main thread to terminate")
        self.is_alive = False

    def setup_bootstrap(self):
        self.mysql_src = MySQLConnection(self.opts.src_dsn, 'Schemigrator, src')
        src_hostname = self.mysql_src.fetchone('SELECT @@hostname as h', 'h')
        self.logger.info('Test connection to source succeeded, got hostname "%s"' % src_hostname)

        self.mysql_dst = MySQLConnection(self.opts.dst_dsn, 'Schemigrator, dst')
        dst_hostname = self.mysql_dst.fetchone('SELECT @@hostname as h', 'h')
        self.logger.info('Test connection to destination succeeded, got hostname "%s"' % dst_hostname)

        self.mysql_dst.query('CREATE DATABASE IF NOT EXISTS %s' % self.opts.src_dsn['db'])
        self.mysql_dst.query('USE %s' % self.opts.src_dsn['db'])

    def setup_metadata_tables(self, tables):
        self.mysql_dst.query(sql_schemigrator_binlog_status)
        self.mysql_dst.query(sql_schemigrator_checksums)
        self.mysql_dst.query(sql_schemigrator_checkpoint)

        for table in tables:
            sql = "INSERT IGNORE INTO schemigrator_checkpoint (tbl) VALUES('%s')" % table
            self.mysql_dst.query(sql)

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
            tables.append(row['tbl'])

        return tables

    def get_binlog_checkpoint(self):
        sql = 'SELECT * FROM schemigrator_binlog_status'
        fil_pos = self.mysql_dst.query(sql)

        binlog_fil = None
        binlog_pos = None

        if len(fil_pos) > 1:
            raise Exception('Multiple binlog coordinates reported on schemigrator_binlog_status')
        elif len(fil_pos) == 1:
            binlog_fil = fil_pos['fil']
            binlog_pos = fil_pos['pos']

        return binlog_fil, binlog_pos

    def get_binlog_coords(self):
        binlog_fil, binlog_pos = self.get_binlog_checkpoint()

        if binlog_fil is None:
            binlog_fil, binlog_pos = self.master_status()
            self.logger.info('Binlog replication info is empty, SHOW MASTER STATUS')
            self.logger.info('Starting file: %s, position: %d' % (binlog_fil, binlog_pos))

        return binlog_fil, binlog_pos

    def master_status(self):
        status = self.mysql_src.fetchone('SHOW MASTER STATUS')
        return status['File'], status['Position']

    def run_table_copier(self, table):
        copier = TableCopier(self.opts.src_dsn, self.opts.dst_dsn, self.opts.bucket, table, 
                             debug=self.opts.debug, pause_file=self.opts.pause_file, stop_file=self.opts.stop_file,
                             chunk_size=self.opts.chunk_size)
        copier.run()

    def run_replication_client(self, binlog_fil, binlog_pos):
        repl = ReplicationClient(self.opts.src_dsn, self.opts.dst_dsn, self.opts.bucket, binlog_fil, binlog_pos, 
                                 debug=self.opts.debug, pause_file=self.opts.pause_file, stop_file=self.opts.stop_file,
                                 chunk_size=self.opts.chunk_size)
        repl.run()

    def run(self):
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

        self.replication_client = Process(target=self.run_replication_client, 
                                          args=(binlog_fil, binlog_pos,), name='replication_client')
        self.replication_client.start()

        #repl = ReplicationClient(self.opts.src_dsn, self.opts.dst_dsn, self.opts.bucket, binlog_fil, binlog_pos, 
        #                         debug=self.opts.debug, pause_file=self.opts.pause_file, stop_file=self.opts.stop_file)
        #repl.run()

        for table in tables:
            self.table_copier = Process(target=self.run_table_copier, args=(table,), name='table_copier')
            self.table_copier.start()

            while self.is_alive and self.table_copier.is_alive():
                self.logger.debug(".. sleep ...")
                time.sleep(1)
                
            if not self.is_alive or os.path.exists(self.opts.stop_file):
                break

        while self.replication_client.is_alive():
            if not self.is_alive:
                self.logger.info('Main thread is is ready to shutdown, but replication is still running, waiting')
                # We can keep sending kill signal, but would be redundant, ReplicationClient.is_alive
                # should be already set
                # self.replication_client.terminate()
            time.sleep(1)

        self.logger.info("Done")
        return 0


class TableCopier(object):
    def __init__(self, src_dsn, dst_dsn, bucket, table, debug=False, pause_file=None, stop_file=None, chunk_size=1000):
        # Grab SHOW CREATE TABLE from source
        # Identify if there is PK/UK
        # Get MIN/MAX PK/UK, record to checkpoint table
        self.bucket = bucket
        self.table = table
        self.logger = sm_create_logger(debug, 'TableCopier (%s)' % self.table)
        self.pk = None
        self.status = 0
        self.minpk = None
        self.maxpk = None
        self.pause_file = pause_file
        self.stop_file = stop_file
        self.chunk_size = chunk_size
        # These variables will only hold chunks that has been successfully 
        # copied
        self.current_minpk = 0
        self.current_maxpk = 0
        self.is_alive = True
        self.src_dsn = src_dsn
        self.dst_dsn = dst_dsn
        self.dst_dsn['db'] = self.bucket
        self.mysql_src = None
        self.mysql_dst = None
        self.logger.info('My PID is %d' % os.getpid())

    def signal_handler(self, signal, frame):
        self.logger.info("Signal caught (%s), cleaning up" % str(signal))
        self.is_alive = False

    def get_checkpoint(self):
        """ Read from dest server current checkpoint position for the table """
        # Checkpoint is none when table has not been started before
        checkpoint = self.mysql_dst.fetchone('SELECT * FROM schemigrator_checkpoint WHERE tbl = "%s"' % self.table)
        if checkpoint is not None:
            self.status = checkpoint['status']

        return checkpoint

    def set_checkpoint(self, checkpoint):
        checkpoint = {'table_name': self.table, 'status': self.status, 'minpk': self.minpk, 'maxpk': self.maxpk, 
                      'current_minpk': self.current_minpk, 'current_maxpk': self.current_maxpk}
        
        self.mysql_dst.query(
            ('REPLACE INTO schemigrator_checkpoint (tbl, minpk, maxpk, lastpk, status) '
             'VALUES("{0}", {1}, {2}, {3}, {4})').format(checkpoint['tbl'], checkpoint['mnipk'], 
                                                         checkpoint['maxpk'], checkpoint['lastpk'],
                                                         checkpoint['status'])
        )

        return self.mysql_dst.affected_rows()

    def get_table_primary_key(self):
        sql = ('SELECT column_name AS col FROM information_schema.key_column_usage '
               'WHERE table_schema = "{0}" AND table_name = "{1}" AND constraint_name = "PRIMARY"')
        pk_columns = self.mysql_src.query(sql.format(self.bucket, self.table))

        if len(pk_columns) == 0:
            raise Exception('Table %s has no PRIMARY KEY defined' % self.table)
        elif len(pk_columns) > 1:
            raise Exception('Table %s has multiple PRIMARY KEY columns' % self.table)

        return pk_columns[0]['col']

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

    def checksum_chunk(self):
        pass
        self.conn.start_transaction()
        # Calculate checksum
        self.conn.commit()

    def copy_chunk(self, range):
        """ Copy chunk specified by range """
        pass

    def run(self):
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.logger.info("Copying table %s" % self.table)

        self.mysql_src = MySQLConnection(self.src_dsn, 'TableCopier, %s, src' % self.table)
        self.mysql_dst = MySQLConnection(self.dst_dsn, 'TableCopier, %s, dst' % self.table)
        self.pk = self.get_table_primary_key()

        checkpoint = self.get_checkpoint()
        if checkpoint is None or self.status == 0:
            self.minpk, self.maxpk = self.get_min_max_range()
        else:
            self.minpk = checkpoint['minpk']
            self.maxpk = checkpoint['maxpk']

        if self.maxpk == 0:
            self.logger.info('Table %s has no rows' % self.table)
            return 0
        
        while self.current_maxpk <= self.maxpk and self.is_alive:
            self.logger.debug('__chunk_loop')

            if os.path.exists(self.pause_file):
                self.logger.info('Paused, remove %s to continue' % self.pause_file)
                time.sleep(5)
                continue

            if os.path.exists(self.stop_file):
                self.logger.info('Stopped via %s file' % self.stop_file)
                break

            time.sleep(1)
            continue

            minpk, maxpk = self.get_next_chunk_boundary()
            self.copy_chunk(minpk, maxpk)
            checksum = self.checksum_chunk(minpk, maxpk)
            self.check_replica_lag()            

        self.logger.info('Done')
        return 0


class ReplicationClient(object):
    def __init__(self, src_dsn, dst_dsn, bucket, binlog_fil=None, binlog_pos=None, 
                 debug=False, pause_file=None, stop_file=None, chunk_size=1000):
        # Sync to checkpoint every chunk completion
        self.bucket = bucket
        self.src_dsn = src_dsn
        self.dst_dsn = dst_dsn
        self.dst_dsn['db'] = self.bucket
        self.binlog_fil = binlog_fil
        self.binlog_pos = binlog_pos
        self.debug = debug
        self.logger = sm_create_logger(debug, 'ReplicationClient')
        self.is_alive = True
        self.pause_file = pause_file
        self.stop_file = stop_file
        self.chunk_size = chunk_size
        self.pkcols = {}
        self.trx_size = 0
        self.trx_open = False
        self.trx_open_ts = None
        self.mysql_dst = MySQLConnection(self.dst_dsn, 'ReplicationClient, applier, dst')
        self.metrics = {}

    def signal_handler(self, signal, frame):
        self.logger.info("Signal caught (%s), cleaning up" % str(signal))
        self.logger.info("It may take a fee seconds to teardown, you can also KILL -9 %d" % os.getpid())
        self.is_alive = False

    def sizeof_fmt(self, num, suffix='B'):
        for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, 'Yi', suffix)

    def log_event_metrics(self, start=False, binlog_fil=None, binlog_pos=None):
        if start:
            self.metrics['timer'] = time.time()
            self.metrics['events'] = 0
            self.metrics['rows'] = 0
            self.metrics['bytes'] = 0
            self.metrics['binlog_pos_last'] = 0
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
            self.logger.info('File: %s, position: %s, events read: %d, rows applied: %d, size: %s, time: %.2f secs' % (
                binlog_fil, binlog_pos, self.metrics['events'], self.metrics['rows'],
                self.sizeof_fmt(self.metrics['bytes']), (now - self.metrics['timer'])))
            self.metrics['timer'] = now
            self.metrics['rows'] = 0
            self.metrics['events'] = 0
            self.metrics['bytes'] = 0
            self.metrics['binlog_pos_last'] = 0

        return True

    def get_table_primary_key(self, table):
        mysql_src = MySQLConnection(self.src_dsn, 'ReplicationClient, %s, src' % table)
        sql = ('SELECT column_name AS col FROM information_schema.key_column_usage '
               'WHERE table_schema = "{0}" AND table_name = "{1}" AND constraint_name = "PRIMARY"')
        pk_columns = mysql_src.query(sql.format(self.bucket, table))

        if len(pk_columns) == 0:
            raise Exception('Table %s has no PRIMARY KEY defined' % self.table)
        elif len(pk_columns) > 1:
            raise Exception('Table %s has multiple PRIMARY KEY columns' % self.table)

        mysql_src.close()
        return pk_columns[0]['col']

    def checkpoint_write(self, cursor, checkpoint=None):
        """ Write last executed file and position
        Replication checkpoints should be within the same trx as the applied
        events so that in case of rollback, the checkpoint is also consistent
        """
        checkpoint = {
            'bucket': self.bucket,
            'fil': self.binlog_fil,
            'pos': self.binlog_pos
        }
        sql = 'REPLACE INTO schemigrator_binlog_status (%s) VALUES (%s)' % (table, ', '.join(checkpoint.keys()), 
                                                                            ', '.join(['%s'] * len(checkpoint)))
        self.logger.debug(sql)
        self.logger.debug(str(checkpoint.values()))
        cursor.execute(sql, checkpoint.values())
        self.trx_size += 1

    def checkpoint_read(self):
        # Read last checkpoint to resume from
        # otherwise just start replication from recent file/pos
        pass

    def checksum_chunk(self, values):
        """ Calculate checksum based on row event from binlog stream """
        pass

    def begin_apply_trx(self):
        self.mysql_dst.query('BEGIN')
        # We track the time the trx is opened, we do not want to keep a transation open
        # for a very long time even if the number of row events is less than chunk-size
        self.trx_open_ts = time.time()
        self.trx_open = True

    def commit_apply_trx(self):
        commit_time = 0.0
        commit_time_start = 0.0
        commit_time_size = 0

        commit_time_start = time.time()
        self.mysql_dst.query('COMMIT')
        commit_time = time.time() - commit_time_start
        commit_time_size = self.trx_size
        self.logger.info('COMMIT size: %d, time: %.2f' % (commit_time_size, commit_time))
        commit_time_size = 0
        commit_time = 0.0
        self.trx_size = 0
        self.trx_open = False

    def update(self, cursor, table, values):
        if table not in self.pkcols:
            self.pkcols[table] = self.get_table_primary_key(table)

        self.logger.info('REPLACE INTO %s WHERE %s = %d' % (table, self.pkcols[table], values[self.pkcols[table]]))
        self.trx_size += 1

    def insert(self, cursor, table, values):
        sql = 'INSERT INTO %s (%s) VALUES (%s)' % (table, ', '.join(values.keys()), 
                                                   ', '.join(['%s'] * len(values)))
        self.logger.debug(sql)
        self.logger.debug(str(values.values()))
        cursor.execute(sql, values.values())
        self.trx_size += 1

    def delete(self, cursor, table, values):
        if table not in self.pkcols:
            self.pkcols[table] = self.get_table_primary_key(table)

        sql = 'DELETE FROM {0} WHERE {1} = %s'.format(table, self.pkcols[table])
        self.logger.info(sql)
        cursor.execute(sql, (values[self.pkcols[table]], ))
        self.trx_size += 1

    def run(self):
        """ Start binlog replication process 
        TODO: What happens if replication dies?
        """

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.logger.info('My PID is %d' % os.getpid())

        #self.binlog_fil = 'log-bin-db04.014493'
        #self.binlog_pos = 1011862334

        stream = BinLogStreamReader(
            connection_settings=self.src_dsn, resume_stream=True,
            server_id=172313514, log_file=self.binlog_fil, log_pos=self.binlog_pos,
            only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent], 
            blocking=True)
        #, only_schemas=[self.bucket]
        cursor = None

        self.logger.info('Replication client started')
        self.log_event_metrics(start=True)

        for binlogevent in stream:
            self.logger.debug('__binlogevent_loop')
            self.binlog_pos = int(stream.log_pos)
            self.binlog_fil = stream.log_file

            """ We put the commit apply here as we need to capture the NEXT
            binlog event file and position into the checkpoint table instead
            of the previous one as long as the COMMIT here succeeds
            """
            if self.trx_open and (self.trx_size >= self.chunk_size or (time.time() - self.trx_open_ts) >= 5):
                self.commit_apply_trx()
                cursor.close()

            if not self.is_alive:
                self.logger.info('Terminating binlog event processing')
                break

            if os.path.exists(self.pause_file):
                self.logger.info('Paused, remove %s to continue' % self.pause_file)
                time.sleep(5)
                continue

            if os.path.exists(self.stop_file):
                self.logger.info('Stopped via %s file' % self.stop_file)
                break

            """ When there are no matching event from the stream because of only_schemas
            this loop blocks at SIGTERM/SIGKILL with `with binlogevent from stream`

            TODO: Ideally the check for the schema should be handled from python-mysql-replication
            however, there is not SIGNAL handler from the library and doing Ctrl-C will leave us
            zombie subprocesses. There is still a chance if implemented this way when there is 
            absolutely zero traffic.
            """
            if binlogevent.schema == self.bucket:
                if not self.trx_open:
                    cursor = self.mysql_dst.conn.cursor()
                    self.begin_apply_trx()
                    self.checkpoint_write(cursor)

                """ For close of open transaction so we can run the checksum safely
                """
                if self.trx_open and self.trx_size > 0 and binlogevent.table == 'schemigrator_checksums':
                    self.commit_apply_trx()
                    cursor.close()
                    cursor = self.mysql_dst.conn.cursor()
                    self.begin_apply_trx()
                    self.checkpoint_write(cursor)

                for row in binlogevent.rows:
                    try:
                        if binlogevent.table == 'schemigrator_checksums':
                            self.checksum_chunk(row["values"])
                        elif isinstance(binlogevent, DeleteRowsEvent):
                            self.delete(cursor, binlogevent.table, row["values"])
                        elif isinstance(binlogevent, UpdateRowsEvent):
                            self.update(cursor, binlogevent.table, row["after_values"])
                        elif isinstance(binlogevent, WriteRowsEvent):
                            self.insert(cursor, binlogevent.table, row["values"])

                        self.metrics['rows'] += 1
                    except AttributeError as e:
                        self.logger.error(str(e))
                        event = (binlogevent.schema, binlogevent.table, stream.log_file, int(stream.log_pos))
                        self.logger.error("Failed on: %s" % str(event))
                        self.is_alive = False
                        break
                    
                    sys.stdout.flush()

            self.log_event_metrics(binlog_fil=stream.log_file, binlog_pos=stream.log_pos)

        stream.close()

        self.logger.info('Replication client stopped')
        return 0


class MySQLConnection(object):
    def __init__(self, params, header):
        self.conn = MySQLdb.connect(**params)
        self.query_header = header

    def disconnect(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def sqlize(self, sql):
        sql = '/* %s */ %s' % (self.query_header, sql)
        logger.debug(sql)
        return sql

    def affected_rows(self):
        return self.conn.affected_rows

    def query(self, sql, args=None):
        cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(self.sqlize(sql), args)
        results = cursor.fetchall()
        cursor.close()
        return results

    def query_array(self, sql, args=None):
        cursor = self.conn.cursor(MySQLdb.cursors.Cursor)
        cursor.execute(self.sqlize(sql), args)
        return cursor.fetchall()

    def fetchone(self, sql, column=None):
        results = self.query(sql)
        if len(results) == 0:
            return None

        if column is not None:
            return results[0][column]

        return results[0]

    def execute(self, sql, args=None):
        with warnings.catch_warnings():
            warnings.filterwarnings('error', category=MySQLdb.Warning)
            try:
                cursor = self.conn.cursor()
                cursor.execute(self.sqlize(sql), args)
            except Warning as db_warning:
                logger.warning(
                    "MySQL warning: {}, when executing sql: {}, args: {}"
                    .format(db_warning, sql, args))
            return cursor.rowcount

    def close(self):
        self.conn.close()


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
