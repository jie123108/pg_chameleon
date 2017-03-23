#!/usr/bin/env bash
here=`dirname $0`
psql -c "create user usr_test with password 'test';" -U postgres
psql -c 'create database db_test with owner usr_test;' -U postgres
sudo cp -f ${here}/my.cnf /etc/mysql/my.cnf
sudo service mysql restart
sudo cat /var/log/mysql/error.log
mysql -u root < ${here}/setup_mysql.sql