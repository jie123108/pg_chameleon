#!/usr/bin/env bash
sudo service mysql stop
sudo apt-get remove mysql-common mysql-server-5.5 mysql-server-core-5.5 mysql-client-5.5 mysql-client-core-5.5
sudo apt-get autoremove
sudo apt-get autoclean
sudo rm -rf /var/lib/mysql
sudo rm -rf /var/log/mysql
echo mysql-apt-config mysql-apt-config/enable-repo select mysql-${MYVER} | sudo debconf-set-selections
wget https://dev.mysql.com/get/mysql-apt-config_0.8.1-1_all.deb 
sudo DEBIAN_FRONTEND=noninteractive dpkg --install mysql-apt-config_0.8.1-1_all.deb 
sudo apt-get update -q
sudo apt-get install -q -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" mysql-server
