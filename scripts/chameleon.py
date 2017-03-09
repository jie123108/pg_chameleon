#!/usr/bin/env python
import argparse
from pg_chameleon import replica_engine
from daemonize import Daemonize
commands = [
	'list_connections',
	'show_connection', 
	'create_service_schema',
	]
command_help = 'Available commands, ' + ','.join(commands)
connection_help = 'Specify the connection filename. If omitted defaults to config/connection.yaml'
config_help = 'Configuration key name. Should be present in connection file. If omitted defaults to all.'

parser = argparse.ArgumentParser(description='Command line for pg_chameleon.',  add_help=True)
parser.add_argument('command', type=str, help=command_help)
parser.add_argument('--connfile', type=str,  default='config/connection.yaml',  required=False, help=connection_help)
parser.add_argument('--connkey',  type=str,  default='all',  required=False, help=config_help)
args = parser.parse_args()
pid='/tmp/test.pid'
replica = replica_engine(args.connfile)
if args.command == commands[0]:
	replica.list_connections()
elif args.command == commands[1]:
	replica.show_connection(args.connkey)
elif args.command == commands[2]:
	#daemon = Daemonize(app="test_app", pid=pid, action=replica.create_service_schema, foreground=False)
	#daemon.start()
	replica.create_service_schema()


