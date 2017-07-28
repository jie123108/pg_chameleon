#!/bin/bash 
set -e 
#set -x

here=`dirname $0`
old_lib="${here}/../old_lib"
branch="ver1.6"
files='global_lib sql_util pg_lib mysql_lib'
if [ ! -d  "${old_lib}" ]
then 
	mkdir ${old_lib}
fi


for file in ${files}
do
git show ${branch}:pg_chameleon/lib/${file}.py   > ${old_lib}/${file}.py
done 
git show ${branch}:scripts/chameleon.py   > ${old_lib}/chameleon.py
