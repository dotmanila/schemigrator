FROM ubuntu:bionic

WORKDIR /opt
RUN mkdir -p /opt/sandboxes
RUN mkdir -p /opt/mysql
RUN apt update
RUN apt -y install wget lsb-release gnupg2 openssl1.0 libnuma1 numactl libreadline5 libatomic1
RUN wget -O /opt/Percona-Server-5.7.29-32-Linux.x86_64.ssl102.tar.gz https://www.percona.com/downloads/Percona-Server-5.7/Percona-Server-5.7.29-32/binary/tarball/Percona-Server-5.7.29-32-Linux.x86_64.ssl102.tar.gz
RUN tar -xzf /opt/Percona-Server-5.7.29-32-Linux.x86_64.ssl102.tar.gz -C /opt/mysql
RUN mv -f /opt/mysql/Percona-Server-5.7.29-32-Linux.x86_64.ssl102 /opt/mysql/5.7.29
RUN wget -O /opt/percona-release_latest.bionic_all.deb https://repo.percona.com/apt/percona-release_latest.bionic_all.deb
RUN dpkg -i /opt/percona-release_latest.bionic_all.deb
RUN apt update
RUN apt -y install sysbench
RUN wget https://github.com/datacharmer/dbdeployer/releases/download/v1.45.0/dbdeployer-1.45.0.linux.tar.gz
RUN tar xvf dbdeployer-1.45.0.linux.tar.gz
RUN chmod +x dbdeployer-1.45.0.linux
RUN mv dbdeployer-1.45.0.linux /usr/bin/
ENV SANDBOX_HOME=/opt/sandboxes
ENV SANDBOX_BINARY=/opt/mysql
RUN dbdeployer-1.45.0.linux deploy single --base-port 10000 --port-as-server-id --gtid 5.7.29 --sandbox-home /opt/sandboxes --skip-start
RUN dbdeployer-1.45.0.linux deploy replication --base-port 10000 --nodes 3 --super-read-only-slaves --port-as-server-id --gtid 5.7.29 --sandbox-home /opt/sandboxes --skip-start

EXPOSE 5729 10001 10002 10003
#CMD sysbench --mysql-user=msandbox --mysql-password=msandbox --mysql-port=5728 --mysql-host=127.0.0.1 --mysql-db=test --tables=2 --table-size=1000000 --report-interval=1 --rate=1 --time=0 --events=0 oltp_read_write run
