#!/usr/bin/env python
import argparse
from pg_chameleon import replica_engine
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

replica = replica_engine(args.connfile)
if args.command == commands[0]:
	replica.list_connections()
elif args.command == commands[1]:
	replica.show_connection(args.connkey)
elif args.command == commands[2]:
	replica.create_service_schema()

"""
if args.command in commands:
	replica = replica_engine(args.command)
	if args.command == commands[0]:
		replica.create_service_schema()
	elif args.command == commands[1]:
		replica.drop_service_schema()
		replica.create_service_schema()
		replica.create_schema()
		replica.copy_table_data()
		replica.create_indices()
	elif args.command == commands[2]:
		replica.run_replica()
	elif args.command == commands[3]:
		replica.upgrade_service_schema()
	elif args.command == commands[4]:
		replica.drop_service_schema()
"""
