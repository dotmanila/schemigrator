FROM percona:ps-5.7

USER root
WORKDIR /opt
RUN yum -y install epel-release
RUN yum -y install python-pip
RUN pip install supervisor
RUN mkdir -p /opt/mysql
RUN cp -r /var/lib/mysql /opt/mysql/n1
RUN cp -r /var/lib/mysql /opt/mysql/n2
RUN cp -r /var/lib/mysql /opt/mysql/n3
RUN cp -r /var/lib/mysql /opt/mysql/src
RUN chown -R mysql.mysql /opt/mysql

RUN mkdir -p /var/log/supervisor
RUN mkdir -p /var/run/supervisor

COPY ./docker/entrypoint.sh /opt/entrypoint.sh
RUN chmod 0755 /opt/entrypoint.sh
COPY ./docker/healthcheck.sh /opt/healthcheck.sh
RUN chmod 0755 /opt/healthcheck.sh
COPY ./docker/supervisord.conf /etc/supervisord.conf

RUN mysqld --initialize-insecure --datadir=/opt/mysql/src --port 10000 --user mysql
RUN mysqld --initialize-insecure --datadir=/opt/mysql/n1 --port 10001 --user mysql
RUN mysqld --initialize-insecure --datadir=/opt/mysql/n2 --port 10002 --user mysql
RUN mysqld --initialize-insecure --datadir=/opt/mysql/n3 --port 10003 --user mysql

HEALTHCHECK CMD /opt/healthcheck.sh
EXPOSE 10000 10001 10002 10003
CMD /opt/entrypoint.sh
