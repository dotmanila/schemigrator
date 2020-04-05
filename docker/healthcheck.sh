#!/bin/bash

alias msrc="mysql -S /opt/mysql/src/mysqld.sock"
alias mn1="mysql -S /opt/mysql/n1/mysqld.sock"
alias mn2="mysql -S /opt/mysql/n2/mysqld.sock"
alias mn3="mysql -S /opt/mysql/n3/mysqld.sock"
msrc="mysql -S /opt/mysql/src/mysqld.sock"
mn1="mysql -S /opt/mysql/n1/mysqld.sock"
mn2="mysql -S /opt/mysql/n2/mysqld.sock"
mn3="mysql -S /opt/mysql/n3/mysqld.sock"


sql="GRANT ALL ON *.* TO 'schemigrator'@'%' IDENTIFIED BY 'schemigrator'"

$msrc -BNe "$sql"
$mn1 -BNe "$sql"

sql="CHANGE MASTER TO MASTER_HOST='127.0.0.1', MASTER_USER='schemigrator', MASTER_PASSWORD='schemigrator', MASTER_PORT=10001, MASTER_AUTO_POSITION=1; START SLAVE;"

if [ ! -f /opt/mysql/n2/master.info ]; then
    $mn2 -BNe "$sql"
fi

if [ ! -f /opt/mysql/n3/master.info ]; then
    $mn3 -BNe "$sql"
fi

$msrc -BNe "select @@hostname" || exit 1
$mn1 -BNe "select @@hostname" || exit 1
$mn2 -BNe "select @@hostname" || exit 1
$mn3 -BNe "select @@hostname" || exit 1

exit 0