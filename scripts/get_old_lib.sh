#!/bin/bash
here=`dirname $0`
old_lib="${here}/../old_lib"
branch="ver1.6"
files='global_lib sql_util pg_lib mysql_lib'
for file in ${files}
do
git show ${branch}:pg_chameleon/lib/${file}.py   > ${old_lib}/${file}.py
done 
git show ${branch}:pg_chameleon/scripts/chameleon.py   > ${old_lib}/chameleon.py
