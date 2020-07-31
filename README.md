# schemigrator

[![CodeCov](https://codecov.io/gh/dotmanila/schemigrator/branch/master/graph/badge.svg?token=SC0J0VTCGJ)](https://codecov.io/gh/dotmanila/schemigrator) [![Build Status](https://travis-ci.com/dotmanila/schemigrator.svg?token=7UHM9mfoNRVp5qAhcQg6&branch=master)](https://travis-ci.com/dotmanila/schemigrator)

## Requirements

By default, the script uses `SELECT INTO OUTFILE` and `LOAD DATA LOCAL INFILE`, which means:

- The script must be ran from the source MySQL server and `--secure-file-priv` enabled.
- `local_infile` must be enabled on the destination server, this can be done dynamically with `SET GLOBAL local_infile=1`.

Use the `--use-insert-select` option if you want to be able to run the script from anywhere. This is much slower though as it relies on `mysql.connector.cursor()`'s `executemany()` function and significantly slower even when we are batching commits.

Checksumming is not enabled by default, enable with `--checksum` option explicitly. Checksumming requires that the account used on the source server is able to create a table on the source bucket called `schemigrator_checksums`. Checksum calculations from the source is inserted into this table which in turn the `ReplicationClient` uses this information to compare the checksum on the target. This is a similar and familiar approach with `pt-table-checksum` tool.

## Installation

On some environments, installing additional Python3 packages may not be possible. It is recommended to use `virtualenv` with `pypy3` to improve performance of the replication client.

    cd $HOME
    sudo apt install virtualenv
    wget https://bitbucket.org/pypy/pypy/downloads/pypy3.6-v7.3.1-linux64.tar.bz2
    tar xf pypy3.6-v7.3.1-linux64.tar.bz2
    git clone https://github.com/dotmanila/schemigrator.git
    cd schemigrator
    virtualenv -p $HOME/pypy3.6-v7.3.1-linux64/bin/pypy3 pypy3
    

The next steps below activates the virtual environment, once activated make sure to install the module requirements after which you can run the script like above.

    source pypy3/bin/activate
    pip3 install -r requirements.txt

When done running, exit from the `virtualenv`/sandbox using the following command.

    deactivate


## Limitations

- Only supports tables with auto-incrementing single column `PRIMARY KEY`.
- Table copy is single threaded.
- On small busy tables, deadlocks may be frequent, try reducing the ReplicationClient chunk size with `--chunk-size-repl`. It is safe to terminate the current process with `Ctrl+C` to change chunking parameters anytime and restart. The script maintains state and resumes where it left off.
- When `--checksum` is enabled, a checksum table is created and written to on the source bucket server. This means the user should be able to write i.e. `super_read_only` should be disabled.
- By default, when using `SELECT INTO OUTFILE`, `LOAD DATA INFILE`, the `FILE` privilege is required for the MySQL user on the source bucket server.
- Similarly, when using `INFILE`/`OUTFILE`, the OS user running the script should be able to read and write to the `secure_file_priv` directory. i.e. `sudo usermod -aG mysql ubuntu`
    
## Command Line Options

    usage: schemigrate.py [-h] [-v] [-B BUCKET] [-n CHUNK_SIZE]
                          [--chunk-size-repl CHUNK_SIZE_REPL]
                          [--chunk-size-copy CHUNK_SIZE_COPY] [-r MAX_LAG]
                          [-m MAX_LAG_COPY] [-M MAX_LAG_REPL] [-R REPLICA_DSNS]
                          [-d] [-c DOTMYCNF] [-L LOG] [-x STOP_FILE]
                          [-p PAUSE_FILE] [-X] [-o] [-C] [-O]
                          [-w {parallel,serialized}] [-i REPORT_INTERVAL]
                          source_dsn target_dsn
    
    Migrate databases from one MySQL server to another.
    
    positional arguments:
      source_dsn            Source DSN
      target_dsn            Target DSN
    
    optional arguments:
      -h, --help            show this help message and exit
      -v, --version         show program's version number and exit
      -B BUCKET, --bucket BUCKET
                            The bucket/database name to migrate
      -n CHUNK_SIZE, --chunk-size CHUNK_SIZE
                            How many rows per transaction commit
      --chunk-size-repl CHUNK_SIZE_REPL
                            How many rows per transaction commit for
                            ReplicationClient, overrides --chunk-size
      --chunk-size-copy CHUNK_SIZE_COPY
                            How many rows per transaction commit for TableCopier,
                            overrides --chunk-size
      -r MAX_LAG, --max-lag MAX_LAG
                            Max replication lag (seconds) on target to start
                            throttling
      -m MAX_LAG_COPY, --max-lag-copy MAX_LAG_COPY
                            Max replication lag (seconds) on target to start
                            throttling table copy
      -M MAX_LAG_REPL, --max-lag-repl MAX_LAG_REPL
                            Max replication lag (seconds) on target to start
                            throttling replication
      -R REPLICA_DSNS, --replica-dsns REPLICA_DSNS
                            Replica DSNs to check for replication lag
      -d, --debug           Enable debugging outputs
      -c DOTMYCNF, --defaults-file DOTMYCNF
                            Path to .my.cnf containing connection credentials to
                            MySQL
      -L LOG, --log LOG     Log output to specified file
      -x STOP_FILE, --stop-file STOP_FILE
                            When this file exists, the script terminates itself
      -p PAUSE_FILE, --pause-file PAUSE_FILE
                            When this script exists, the script pauses copying and
                            replication
      -X, --dry-run         Show what the script will be doing instead of actually
                            doing it
      -o, --use-insert-select
                            Instead of using SELECT INTO OUTFILE/LOAD DATA INFILE,
                            use native and slower simulated INSERT INTO SELECT
      -C, --checksum        Checksum chunks as they are copied, ReplicationClient
                            validates the checksums
      -O, --checksum-reset  Checksum only, useful when you want to re-validate
                            after all tables has been copied, re-initializes state
                            for checksum
      -w {parallel,serialized}, --mode {parallel,serialized}
                            Show what the script will be doing instead of actually
                            doing it
      -i REPORT_INTERVAL, --report-interval REPORT_INTERVAL
                            How often to print status outputs


## Example

    pypy3 schemigrate.py \
        u=xxxxxxxx,p=xxxxxxxx,h=127.0.0.1,P=3306,D=dbname,A=latin1 \
        h=172.16.21.21,P=3306,u=xxxxxxxx,p=xxxxxxxx \
        --stop-file=/tmp/schemigrator.stop --pause-file=/tmp/schemigrator.pause \
        --replica-dsns h=172.16.22.22,P=3306 \
        --max-lag-repl=1800 --max-lag-copy=60 \
        --chunk-size-repl=1000 --chunk-size-copy=20000 \
        --checksum --report-interval=10

With the command above:

- The first DSN is the source server which database will be migrated from.
- The second DSN is the target.
- `--replica-dsns` can be specified multiple times depending on how many replicas the destination server needs to be checked.
  - `--max-lag` is based on `Seconds_Behind_Master`
  - If a replica is not replicating i.e. `Seconds_Behind_Master` is `NULL` it is not considered to be lagging.

When `--checksum` is enabled, the following query can be used to see if there were inconsistencies in the destination server after all tables has been copied.

    SELECT db, tbl, SUM(this_cnt) AS total_rows, COUNT(*) AS chunks
    FROM schemigrator_checksums
    WHERE (
     master_cnt <> this_cnt
     OR master_crc <> this_crc
     OR ISNULL(master_crc) <> ISNULL(this_crc))
    GROUP BY db, tbl;

## Interpreting Status Output

    2020-07-31 00:58:49,474 <24405> INFO <Replication> Tables: 0 not started, 0 in progress, 
        0 checksum, 5 complete, 0 error, no checksum errors, copy complete

While the script is running, you will get status outputs like above, this is interpreted as:

- `0 not started`: No tables pending to be copied.
- `0 in progress`: No tables currently being copied.
- `0 checksum`: No tables currently being checksummed.
- `5 complete`: 5 tables has completed copy from source to target.
- `0 error`: No tables has failed copy.
- `no checksum errors`: No checksum errors were detected.
- `copy complete`: All tables have been copied to target.

Additionally, outputs like below will be emitted.

    2020-07-31 01:03:09,577 <24405> INFO <Replication> Status: mysql-bin.007623:599130064, 
        32093 events/r (12.8MiB), 0 rows/w, 0.4921 lat (ms), lag (P) 6.0 secs

These can be interpreted as:

- `32093 events/r`: The number of events read from binary logs since last report interval i.e. 10 seconds.
- `0 rows/w`: The number of rows applied to the current database being migrated.
- `0.4921 lat (ms)`: Total amount of time to apply the previous metric.
- `lag (P) 6.0 secs`: Heartbeat delay from source to target.
- `lag (S) 8.0 secs`: Max value of `Seconds_Behind_Master` when `--replica-dsns` is specified.


### Some Notes on DSN (Data Source Names)

The minimum values for DSNs is the host, this is assuming Python can figure out the rest of the credentials via configuration files.

If for example, the username/password is specified only from the source DSN, the same credentials will be used on the target and `--replica-dsns`.

Specifying `--bucket` option explicitly takes precedence when the database is specified in the source DSN i.e. `h=localhost,D=dbname`.

DSN Keys:

    h: hostname/IP
    u: username
    p: password
    D: database
    P: port
    A: character set

### Examples

When the following source and target DSN is specified:

    localhost some-remote-host

- The script assumes being able to login as root, without password using the default local socket on the source and using port 3306 on the target server.
- Since the `D` value is not specified on the source DSN, the `--bucket` option should be specified explicitly.

        h=10.1.1.2,u=myuser,p=p@ssword h=10.1.1.2,p=AaBbCcDd

- Since the user is not specified on the target DSN, the same user from the source DSN will be used, only with the password explicitly specified on that target DSN.

## Character Sets

It is important to be able to provide the correct character set for the source connection and the target. For example, if your application uses `latin1` for its connections, but writes `utf8` data, you will need to specify `latin1` as character set in your source DSN i.e. `A=latin1`.

To identify the correct character set to use, you need to sample your data and identify rows which may have multi byte characters on on them. In the example below, on a `latin1` connection the data looks garbled, but on `utf8` the data is properly displayed.

    mysql> SET NAMES latin1;
    Query OK, 0 rows affected (0.00 sec)
    
    mysql> SELECT * FROM t_utf8 WHERE CONVERT(s USING BINARY) 
        RLIKE CONCAT('[', UNHEX('80'), '-', UNHEX('FF'), ']') LIMIT 1;
    +----+---------------+
    | id | s             |
    +----+---------------+
    |  1 | �Celebraci�n!   |
    +----+---------------+
    1 row in set (0.00 sec)
    
    mysql> SET NAMES utf8;
    Query OK, 0 rows affected (0.00 sec)
    
    mysql> SELECT * FROM t_utf8 WHERE CONVERT(s USING BINARY) 
        RLIKE CONCAT('[', UNHEX('80'), '-', UNHEX('FF'), ']') LIMIT 1;
    +----+-----------------+
    | id | s               |
    +----+-----------------+
    |  1 | ¡Celebración!   |
    +----+-----------------+
    1 row in set (0.00 sec) 

In the above example, we should use `A=utf8`.

## Use Native MySQL Replication

If needed, you can use MySQL's native replication instead of relying on the script's simulated replication but this is NOT RECOMMENDED. Using the script's replication client combined with `pypy3` is much faster. The former would be faster in this case.

To verify if checksumming is complete, you can use the query below on the target database. If the query result is empty it means that all tables has been copied and checksum has been completed. Of course, also check that there are not bad checksum results above.

    SELECT chkpt.tbl, chkpt.maxpk, COALESCE(chksm.lastsm, 0) AS lastsm 
    FROM schemigrator_checkpoint chkpt 
    LEFT JOIN (
      SELECT tbl, MAX(upper_boundary) lastsm 
      FROM schemigrator_checksums 
      GROUP BY tbl
    ) chksm ON (chkpt.tbl = chksm.tbl) 
    WHERE chkpt.maxpk > lastsm;

As soon as checksumming and table copy is complete, you can stop the script and configure native replication on the target server. The replication coordinates to use will be displayed when the script is terminated.

    [ReplicationClient       ]_:: Replication client stopped on mysql-bin.000349:540463919

We can use these coordinates to configure replication on the target, however do not start replication immediately.

    CHANGE MASTER TO MASTER_HOST='<source_server_host>', MASTER_USER='username', 
    MASTER_PASSWORD=’xxxxxxxxxx’, MASTER_LOG_FILE='mysql-bin.000349', 
    MASTER_LOG_POS=540463919;

Before starting replication, we make sure to replicate only the database we are migrating.

    CHANGE REPLICATION FILTER REPLICATE_DO_DB=(MigratedDBName), REPLICATE_WILD_DO_TABLE=('MigratedDBName.%');

Then replication can be started.

    START SLAVE;

Once replication is running, `pt-table-checksum` can also be manually ran against the database we have migrated, if needed.

    pt-table-checksum --recursion-method=none --replicate=test.checksums --databases=test \
        h=127.0.0.1,P=5728,u=msandbox,p=msandbox


## Running Tests

    docker build --pull --force-rm --tag schemigrator:latest .
    docker run --name schemigrator -p 13300:10000 -p 13301:10001 -p 13302:10002 \
        -p 13303:10003 --detach schemigrator:latest
    pytest .
    docker stop schemigrator
    docker rm schemigrator