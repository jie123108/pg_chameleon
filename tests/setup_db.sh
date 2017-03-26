#!/usr/bin/env bash
here=`dirname $0`
psql -c "create user usr_test with password 'test';" -U postgres
psql -c 'create database db_test with owner usr_test;' -U postgres
if [ "${MYVER}" != "5.6" ]
then
	${here}/install_mysql.sh
fi
sudo cp -f ${here}/my${MYVER}.cnf /etc/mysql/conf.d/my.cnf
sudo service mysql restart
sudo cat /var/log/mysql/error.log
sudo mysql -u root < ${here}/setup_mysql.sql
