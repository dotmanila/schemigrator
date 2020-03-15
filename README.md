# schemigrator

## Requirements

    dnspython==1.16.0
    mysql-replication==0.21
    mysql-connector-python==8.0.19

By default, the script uses `SELECT INTO OUTFILE` and `LOAD DATA LOCAL INFILE`, which means:

- The script must be ran from the source MySQL server and `--secure-file-priv` enabled.
- `local_infile` must be enabled on the destination server, this can be done dynamically with `SET GLOBAL local_infile=1`.

Use the `--use-insert-select` option if you want to be able to run the script from anywhere. This is much slower though as it relies on `mysql.connector.cursor()`'s `executemany()` function and significantly slower even when we are batching commits.
    
