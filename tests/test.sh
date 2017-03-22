#!/usr/bin/env bash
here=`dirname $0`
chameleon.py set_config
cp ${here}/test.yaml ~/.pg_chameleon/connection/ 
chameleon.py create_service_schema --connfile test.yaml --connkey test
chameleon.py add_replica --connfile test.yaml --connkey test
chameleon.py init_replica --connfile test.yaml --connkey test
chameleon.py drop_replica --connfile test.yaml --connkey test --noprompt
chameleon.py drop_service_schema --connfile test.yaml --connkey test --noprompt


