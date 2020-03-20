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
- When `--checksum` is enabled, a checksum table is created and written to on the source bucket server. This means the user should be able to write i.e. `super_read_only`, `read_only`.
- By default, when using `SELECT INTO OUTFILE`, `LOAD DATA INFILE`, the `FILE` privilege is required for the MySQL user on the source bucket server.
- Similarly, when using `INFILE`/`OUTFILE`, the OS user running the script should be able to read and write to the `secure_file_priv` directory. i.e. `sudo usermod -aG mysql ubuntu`
    
## Command Line Options

    Usage: schemigrate.py [options] COMMAND

    Migrate databases from one MySQL server to another.

    Options:
      --version             show program's version number and exit
      -h, --help            show this help message and exit
      -B BUCKET, --bucket=BUCKET
                            The bucket/database name to migrate
      -n CHUNK_SIZE, --chunk-size=CHUNK_SIZE
                            How many rows per transaction commit
      --chunk-size-repl=CHUNK_SIZE_REPL
                            How many rows per transaction commit for
                            ReplicationClient, overrides --chunk-size
      --chunk-size-copy=CHUNK_SIZE_COPY
                            How many rows per transaction commit for TableCopier,
                            overrides --chunk-size
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
      -C, --checksum        Checksum chunks as they are copied, ReplicationClient
                            validates the checksums


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

## Running with virtualenv

On some environments, installing additional Python3 packages may not be possible. For these hosts, we can use `virtualenv` as long as Python3 is available on the host. `python3-pip` might also not be installed by default and can be installed separately.

    sudo apt install python3-pip
    git clone https://github.com/dotmanila/schemigrator.git
    pip3 install virtualenv
    virtualenv -p $(which python3) schemigrator
    cd schemigrator

The next steps below activates the virtual environment, once activated make sure to install the module requirements after which you can run the script like above.

    source bin/activate
    pip3 install -r requirements.txt

When done running, exit from the `virtualenv`/sandbox using the following command.

    deactivate

