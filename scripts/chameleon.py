#!/usr/bin/env python
import argparse
from pg_chameleon import replica_engine
commands = [
	'list_connections',
	'show_connection', 
	'create_service_schema',
	'drop_service_schema', 
	'add_replica', 
	'drop_replica', 
	]
command_help = 'Available commands, ' + ','.join(commands)
connection_help = 'Specify the connection filename. If omitted defaults to config/connection.yaml'
config_help = 'Configuration key name. Should be present in connection file. If omitted defaults to all.'

parser = argparse.ArgumentParser(description='Command line for pg_chameleon.',  add_help=True)
parser.add_argument('command', type=str, help=command_help)
parser.add_argument('--connfile', type=str,  default='connection.yaml',  required=False, help=connection_help)
parser.add_argument('--connkey',  type=str,  default='all',  required=False, help=config_help)
args = parser.parse_args()

replica = replica_engine(args.connfile)

try:
	getattr(replica, args.command)(args)
except AttributeError as e:
	print (e)
	
