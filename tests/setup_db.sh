#!/usr/bin/env bash
psql -c "create user usr_test with password 'test';" -U postgres
psql -c 'create database db_test with owner usr_test;' -U postgres
mysql < setup_mysql.sql
