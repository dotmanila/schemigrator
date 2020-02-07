#!/bin/bash

BUCKET=$1
REPLICA=${2:-}
MAX_LAG=${3:-0}

Chunk=
CurrentChunk=
CurrentTable=

function _echo() {
   echo "$(date +%Y-%m-%d_%H_%M_%S) $1"
}

function _d_inf() {
   _echo "$1"
   exit 1
}

function check_bucket_dump_metadata() {
  local _BucketDir=$1
  if [ ! -f "./${_BucketDir}/metadata" ]; then
    _echo "${_BucketDir} does not look like a valid dump"
    _d_inf "Dump metadata does not exist!"
  fi

  return 0
}

function check_replica_lag() {
  local _ReplicaHost=${1:-}
  local _MaxLag=${2:-0}
  local _SecondsBehindMaster=

  if [ "x$_ReplicaHost" == "x" ]; then
    return 0
  fi

  mysql -h $_ReplicaHost -Be 'SHOW SLAVE STATUS \G' > /tmp/schemigrator.slavestatus 2>&1
  if [ "$?" -ne 0 ]; then
    _d_inf "Could not determine replication status, aborting"
  fi

  grep Seconds_Behind_Master /tmp/schemigrator.slavestatus > /dev/null 2>&1
  if [ "$?" -ne 0 ]; then
    _d_inf "Unable to determine Seconds_Behind_Master"
  fi

  _SecondsBehindMaster=$(grep Seconds_Behind_Master /tmp/schemigrator.slavestatus | awk '{print $2}')
  if [ "x$_SecondsBehindMaster" == "xNULL" ]; then
    _d_inf "Replication is not running, aborting"
  fi

  # No MaxLag specified, just make sure replication is running
  if [ "$_MaxLag" -eq 0 ]; then
    return 0
  fi

  _SecondsBehindMaster=$(($_SecondsBehindMaster+1))
  if [ "$_SecondsBehindMaster" -ge $_MaxLag ]; then
    _echo "Replication behind $_SecondsBehindMaster seconds"
    return 1
  fi

  return 0
}

function signal_handler() {
  _d_inf "Interrupt caught, terminating"
}

trap 'signal_handler' SIGINT

mysql -BNe "SELECT @@hostname" > /dev/null 2>&1
if [ "$?" -ne 0 ]; then
  _d_inf "Unable to connect to MySQL, ~/.my.cnf issue?"
fi

if [ "x$REPLICA" != "x" ]; then
  mysql -h $REPLICA -BNe "SELECT @@hostname" > /dev/null 2>&1
  if [ "$?" -ne 0 ]; then
    _echo "Unable to connect to replica MySQL!"
    _d_inf "Does your ~/.my.cnf credentials work on the replica too?"
  fi
fi

if [ -f "$BUCKET" ]; then
  Chunk=$(basename $BUCKET)
  if [ "$(echo $Chunk | rev | cut -d'-' -f1 | rev)" == "schema.sql" ]; then
    BUCKET=$(basename $(dirname "$BUCKET"))
    check_bucket_dump_metadata "./${BUCKET}"
    CurrentTable=$(echo $Chunk | cut -d'-' -f1 | cut -d'.' -f2)
    _echo "Resuming table $CurrentTable"
    Chunk=
  else
    CurrentTable=$(echo $Chunk | cut -d'.' -f2)
    Chunk=$(basename $BUCKET)
    BUCKET=$(basename $(dirname $BUCKET))
    check_bucket_dump_metadata "./${BUCKET}"
    _echo "Resuming from table $CurrentTable on chunk $Chunk"
  fi
else
  check_bucket_dump_metadata "./${BUCKET}"
fi

mysql -BNe "SHOW TABLES FROM ${BUCKET}" > /dev/null 2>&1
if [ "$?" -ne 0 ]; then
  _echo "Database $BUCKET does not exists, creating"
  mysql -BNe "CREATE DATABASE IF NOT EXISTS ${BUCKET}"
fi

for SQLFile in $(find "./${BUCKET}/" -name \*-schema.sql | sort); do
  Table=$(basename $SQLFile | cut -d'.' -f2 | cut -d'-' -f1)
  if [ "x$CurrentTable" != "x" -a "x$CurrentTable" != "x$Table" ]; then
    _echo "Skipping table $Table"
    continue
  fi
  CurrentTable=

  mysql $BUCKET -BNe "SHOW TABLE STATUS LIKE '${Table}'" > /dev/null 2>&1
  if [ "$?" -ne 0 ]; then
    _echo "Table ${Table} does not exists, creating"
    mysql $BUCKET -BN < $SQLFile
    
    if [ "x$Chunk" != "x" ]; then
      _echo "Table is empty, ignoring specified chunk start"
      Chunk=
    fi
  fi

  _echo "Starting table import for $Table"

  for ChunkFile in $(eval "find ./$BUCKET/ -name $BUCKET.$Table.\*.sql" | sort -n); do
    CurrentChunk=$(basename $ChunkFile)
    if [ "x$Chunk" != "x" -a "x$Chunk" != "x$CurrentChunk" ]; then
      # _echo "Skipping $CurrentChunk"
      continue
    fi

    Chunk=
    _echo "Importing $ChunkFile"
    # Make sure we ignore duplicate key errors
    sed -i 's/INSERT INTO/INSERT IGNORE INTO/g' $ChunkFile
    mysql $BUCKET -BN < $ChunkFile
    if [ "$?" -ne 0 ]; then
      _d_inf "Chunk import failed for $ChunkFile"
    fi
    
    # For simplicity, we check only one replica for now
    check_replica_lag $REPLICA $MAX_LAG
    while [ "$?" -ne 0 ]; do
      sleep 2
    done
  done
done

_echo "Import complete"
exit 0
