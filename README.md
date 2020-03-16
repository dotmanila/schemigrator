# schemigrator

## Requirements

    dnspython==1.16.0
    mysql-replication==0.21
    mysql-connector-python==8.0.19

By default, the script uses `SELECT INTO OUTFILE` and `LOAD DATA LOCAL INFILE`, which means:

- The script must be ran from the source MySQL server and `--secure-file-priv` enabled.
- `local_infile` must be enabled on the destination server, this can be done dynamically with `SET GLOBAL local_infile=1`.

Use the `--use-insert-select` option if you want to be able to run the script from anywhere. This is much slower though as it relies on `mysql.connector.cursor()`'s `executemany()` function and significantly slower even when we are batching commits.

Checksumming is not enabled by default, enable with `--checksum` option explicitly. Checksumming requires that the account used on the source server is able to create a table on the source bucket called `schemigrator_checksums`. Checksum calculations from the source is inserted into this table which in turn the `ReplicationClient` uses this information to compare the checksum on the target. This is a similar and familiar approach with `pt-table-checksum` tool.

## Limitations

- Only supports tables with auto-incrementing single column `PRIMARY KEY`.
- Table copy is single threaded.
- On small busy tables, deadlocks may be frequent, try reducing the ReplicationClient chunk size with `--chunk-size-repl`
    
## Command Line Options

    Usage: schemigrate.py [options] COMMAND

    Migrate databases from one MySQL server to another.

    Options:
      --version             show program's version number and exit
      -h, --help            show this help message and exit
      -B BUCKET, --bucket=BUCKET
                            The bucket/database name to migrate
      -n CHUNK_SIZE, --chunk-size=CHUNK_SIZE
                            How many rows to copy at a time
      -r MAX_LAG, --max-lag=MAX_LAG
                            Max replication lag (seconds) on target to start
                            throttling
      -R REPLICA_DSNS, --replica-dsns=REPLICA_DSNS
                            Replica DSNs to check for replication lag
      -d, --debug           Enable debugging outputs
      -c DOTMYCNF, --defaults-file=DOTMYCNF
                            Path to .my.cnf containing connection credentials to
                            MySQL
      -L LOG, --log=LOG     Log output to specified file
      -x STOP_FILE, --stop-file=STOP_FILE
                            When this file exists, the script terminates itself
      -p PAUSE_FILE, --pause-file=PAUSE_FILE
                            When this script exists, the script pauses copying and
                            replication
      -X, --dry-run         Show what the script will be doing instead of actually
                            doing it
      -o, --use-insert-select
                            Instead of using SELECT INTO OUTFILE/LOAD DATA INFILE,
                            use native and slower simulated INSERT INTO SELECT


## Example

    python3 schemigrate.py \
        u=msandbox,p=msandbox,h=127.0.0.1,P=5728,D=test \
        h=127.0.0.1,P=10001,u=msandbox,p=msandbox \
        --stop-file=/tmp/schemigrator.stop --pause-file=/tmp/schemigrator.pause \
        --chunk-size=5000 --replica-dsns h=127.0.0.1,P=10002 \
        --replica-dsns h=127.0.0.1,P=10003

With the command above:

- The first DSN is the source server which database will be migrated from.
- The second DSN is the target.
- `--replica-dsns` can be specified multiple times depending on how many replicas the destination server needs to be checked.
  - `--max-lag` is based on `Seconds_Behind_Master`
  - If a replica is not replicating i.e. `Seconds_Behind_Master` is `NULL` it is not considered to be lagging.

### Some Notes on DSN (Data Source Names)

The minimum values for DSNs is the host, this is assuming Python can figure out the rest of the credentials via configuration files.

If for example, the username/password is specified only from the source DSN, the same credentials will be used on the target and `--replica-dsns`.