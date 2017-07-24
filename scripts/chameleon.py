#!/usr/bin/env python
import argparse, sys
from pg_chameleon import replica_engine
from pkg_resources import get_distribution
__version__ = get_distribution('pg_chameleon').version


commands = [
	'list_connections',
	'show_connection', 
	'create_service_schema',
	'drop_service_schema', 
	'add_replica', 
	'drop_replica', 
	'init_replica', 
	'set_config', 
	'show_status', 
	]
command_help = """Available commands, """ + ','.join(commands)
connection_help = """Specify the connection filename. If omitted defaults to config/connection.yaml"""
config_help = """Configuration key name. Should be present in connection file. If omitted defaults to all."""
config_noprompt = """Execute the commands without asking for confirm."""
debug_help = """Run the script  without daemonisation and with log on stdout and with debug level. """
jobs_help= """Specifies the number of copy jobs to run in parallel when running the init_replica and sync_tables"""

parser = argparse.ArgumentParser(description='Command line for pg_chameleon.',  add_help=True)
parser.add_argument('command', type=str, help=command_help)
parser.add_argument('--connfile', type=str,  default='connection',  required=False, help=connection_help)
parser.add_argument('--connkey',  type=str,  default='all',  required=False, help=config_help)
parser.add_argument('--noprompt',  action='store_true',  required=False, help=config_noprompt)
parser.add_argument('--debug',  action='store_true',  required=False, help=debug_help)
parser.add_argument('--version', action='version',version='pg_chameleon {version}'.format(version=__version__))
parser.add_argument('--jobs',  type=int,  default='1',  required=False, help=jobs_help)
args = parser.parse_args()

replica = replica_engine(args)

try:
	getattr(replica, args.command)(args)
	sys.exit()
#except AttributeError as e:
#	print (e)
except SystemExit:
	pass
#except:
#	print ("Unexpected error:", sys.exc_info()[0])
#	sys.exit(1)
#	raise

