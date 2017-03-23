#!/usr/bin/env python
import argparse, sys
from pg_chameleon import replica_engine
commands = [
	'list_connections',
	'show_connection', 
	'create_service_schema',
	'drop_service_schema', 
	'add_replica', 
	'drop_replica', 
	'init_replica', 
	'set_config', 
	]
command_help = 'Available commands, ' + ','.join(commands)
connection_help = 'Specify the connection filename. If omitted defaults to config/connection.yaml'
config_help = 'Configuration key name. Should be present in connection file. If omitted defaults to all.'
config_noprompt = 'Execute the commands without asking for confirm.'

parser = argparse.ArgumentParser(description='Command line for pg_chameleon.',  add_help=True)
parser.add_argument('command', type=str, help=command_help)
parser.add_argument('--connfile', type=str,  default='connection.yaml',  required=False, help=connection_help)
parser.add_argument('--connkey',  type=str,  default='all',  required=False, help=config_help)
parser.add_argument('--noprompt',  action='store_true',  required=False, help=config_noprompt)
args = parser.parse_args()

replica = replica_engine(args.connfile)

try:
	getattr(replica, args.command)(args)
	sys.exit()
#except AttributeError as e:
#	print (e)
except SystemExit:
	pass
except:
	print ("Unexpected error:", sys.exc_info()[0])
	sys.exit(1)

