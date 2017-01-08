import yaml
import sys
import os
import time
import logging
import smtplib
from datetime import datetime

class global_config(object):
	"""
		This class parses the configuration file which defaults to config/config.yaml if not specified.
		The class variables used by the other libraries are retrieved from the yaml configuration file. 
		A separate connection file defaults to config/connection.yaml if no file is specified.
		The constructor checks if the configuration file is present and emits an error message followed by
		the sys.exit() command if the files are missing. 
		The class sets the log output file name using the start date and  from the parameter command.  If the log destination is stdout then the logfile is ignored
		
		:param command: the command specified on the pg_chameleon.py command line
	
	"""
	def __init__(self, load_config=False, config_file='config/config.yaml', connection_file='config/connection.yaml'):
		"""
			Class  constructor.
		"""
		self.config_file=config_file
		self.connection_file=connection_file
		if load_config:
			self.load_config()
	
	def load_config(self):
		if not os.path.isfile(self.config_file):
				print("**FATAL - configuration file missing **\ncopy config/config-example.yaml to %s and set your configuration settings." % (self.config_file))
				sys.exit()
		
		if not os.path.isfile(self.connection_file):
			print("**FATAL - connection file missing **\ncopy config/connection-example.yaml to %s and set your connection settings." % (self.connection_file))
			sys.exit()
		configfile=open(self.config_file, 'r')
		confdic=yaml.load(configfile.read())
		configfile.close()
		
		connectfile=open(self.connection_file, 'r')
		self.conndic=yaml.load(connectfile.read())
		connectfile.close()
		
		try:
			self.replica_batch_size=confdic["replica_batch_size"]
			self.tables_limit=confdic["tables_limit"]
			self.copy_mode=confdic["copy_mode"]
			self.hexify=confdic["hexify"]
			self.log_level=confdic["log_level"]
			self.log_dest=confdic["log_dest"]
			self.sleep_loop=confdic["sleep_loop"]
			self.pause_on_reindex=confdic["pause_on_reindex"]
			self.sleep_on_reindex=confdic["sleep_on_reindex"]
			self.reindex_app_names=confdic["reindex_app_names"]
			
			
			self.log_file=confdic["log_dir"]+"/replica.log"
			copy_max_memory=str(confdic["copy_max_memory"])[:-1]
			copy_scale=str(confdic["copy_max_memory"])[-1]
			try:
				int(copy_scale)
				copy_max_memory=confdic["copy_max_memory"]
			except:
				if copy_scale=='k':
					copy_max_memory=str(int(copy_max_memory)*1024)
				elif copy_scale=='M':
					copy_max_memory=str(int(copy_max_memory)*1024*1024)
				elif copy_scale=='G':
					copy_max_memory=str(int(copy_max_memory)*1024*1024*1024)
				else:
					print("**FATAL - invalid suffix in parameter copy_max_memory  (accepted values are (k)ilobytes, (M)egabytes, (G)igabytes.")
					sys.exit()
			self.copy_max_memory=copy_max_memory
		except KeyError as key_missing:
			print('Missing key %s in configuration file. check config/config-example.yaml for reference' % (key_missing, ))
			sys.exit()
		

class replica_engine(object):
	def __init__(self):
		self.global_config=global_config()
		
	def init_replica(self, replica_key):
		if replica_key == "":
			print("**FATAL - You should specify the replica to initialise.")
			sys.exit()

	def list_replica(self):
		self.global_config.load_config()
		for connection in self.global_config.conndic:
			replica_conn=self.global_config.conndic[connection]
			my_conn=replica_conn["mysql_conn"]
			pg_conn=replica_conn["pg_conn"]
			print("Replica ===> %s" % (connection, ))
			print("======== MySQL ========")
			print("Host %(host)s  - port: %(port)s - Schema: %(my_database)s" % (my_conn))
			print("======== PostgreSQL ========")
			print("Host %(host)s  - port: %(port)s - Database: %(pg_database)s -  Destination Schema: %(destination_schema)s" % (pg_conn))
			print(" ")
			
