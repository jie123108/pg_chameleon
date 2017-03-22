#!/usr/bin/env bash
here=`dirname $0`
psql -c "create user usr_test with password 'test';" -U postgres
psql -c 'create database db_test with owner usr_test;' -U postgres
mysql < ${here}/setup_mysql.sql
