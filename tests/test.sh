#!/usr/bin/env bash
chameleon.py create_service_schema --connfile test.yaml --connkey test
chameleon.py drop_service_schema --connfile test.yaml --connkey test --noprompt


