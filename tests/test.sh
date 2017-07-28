#!/usr/bin/env bash
set -e 
here=`dirname $0`
chameleon.py set_config
cp ${here}/test.yaml ~/.pg_chameleon/connection/ 
chameleon.py create_service_schema --connfile test--connkey test
chameleon.py add_replica --connfile test --connkey test
chameleon.py init_replica --connfile test --connkey test
chameleon.py show_status --connfile test --connkey test
chameleon.py drop_replica --connfile test --connkey test --noprompt
chameleon.py drop_service_schema --connfile test --connkey test --noprompt


