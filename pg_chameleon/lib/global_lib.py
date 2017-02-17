import yaml
import sys
import os
import time
import logging
import smtplib
from datetime import datetime

class global_config(object):
	"""
		This class parses the configuration file which defaults to config/config.yaml and the config/connection.yaml if not specified.
		The class variables used by the other libraries are retrieved from the yaml configuration file. 
		A separate connection file defaults to config/connection.yaml if no file is specified.
		The constructor checks if the configuration file is present and emits an error message followed by
		the sys.exit() command if the files are missing. 
		The class sets the log output file name using the start date and  from the parameter command.  If the log destination is stdout then the logfile is ignored
		
	
	"""
	def __init__(self, connection_file, load_config=False):
		"""
			Class  constructor.
		"""
		if connection_file:
			self.connection_file = connection_file
			if load_config:
				self.load_config()
		else:
			print("**FATAL - invalid connection file specified **\Expected filename got %s." % (self.connection_file))
			sys.exit(1)
	
	def set_conn_vars(self, conndic):
		try:
			self.replica_batch_size = conndic["replica_batch_size"]
			self.tables_limit = conndic["tables_limit"]
			self.copy_mode = conndic["copy_mode"]
			self.hexify = conndic["hexify"]
			self.log_level = conndic["log_level"]
			self.log_dest = conndic["log_dest"]
			self.sleep_loop = conndic["sleep_loop"]
			self.pause_on_reindex = conndic["pause_on_reindex"]
			self.sleep_on_reindex = conndic["sleep_on_reindex"]
			self.reindex_app_names = conndic["reindex_app_names"]
			
			
			self.log_file = conndic["log_dir"]+"/replica.log"
			copy_max_memory = str(conndic["copy_max_memory"])[:-1]
			copy_scale=str(conndic["copy_max_memory"])[-1]
			try:
				int(copy_scale)
				copy_max_memory = conndic["copy_max_memory"]
			except:
				if copy_scale =='k':
					copy_max_memory = str(int(copy_max_memory)*1024)
				elif copy_scale =='M':
					copy_max_memory = str(int(copy_max_memory)*1024*1024)
				elif copy_scale =='G':
					copy_max_memory = str(int(copy_max_memory)*1024*1024*1024)
				else:
					print("**FATAL - invalid suffix in parameter copy_max_memory  (accepted values are (k)ilobytes, (M)egabytes, (G)igabytes.")
					sys.exit()
			self.copy_max_memory = copy_max_memory
		except KeyError as key_missing:
			print('Using global value for key %s ' % (key_missing, ))
		
		
	
	def load_connection(self):
		""" 
		"""
		
		if not os.path.isfile(self.connection_file):
			print("**FATAL - connection file missing **\ncopy config/connection-example.yaml to %s and set your connection settings." % (self.connection_file))
			sys.exit()
		
		connectfile = open(self.connection_file, 'r')
		self.connection = yaml.load(connectfile.read())
		connectfile.close()
		conndic = self.connection
		try:
			self.replica_batch_size = conndic["replica_batch_size"]
			#self.tables_limit = conndic["tables_limit"]
			self.copy_mode = conndic["copy_mode"]
			self.hexify = conndic["hexify"]
			self.log_level = conndic["log_level"]
			self.log_dest = conndic["log_dest"]
			self.sleep_loop = conndic["sleep_loop"]
			self.pause_on_reindex = conndic["pause_on_reindex"]
			self.sleep_on_reindex = conndic["sleep_on_reindex"]
			self.reindex_app_names = conndic["reindex_app_names"]
			
			
			self.log_file = conndic["log_dir"]+"/replica.log"
			copy_max_memory = str(conndic["copy_max_memory"])[:-1]
			copy_scale=str(conndic["copy_max_memory"])[-1]
			try:
				int(copy_scale)
				copy_max_memory = conndic["copy_max_memory"]
			except:
				if copy_scale =='k':
					copy_max_memory = str(int(copy_max_memory)*1024)
				elif copy_scale =='M':
					copy_max_memory = str(int(copy_max_memory)*1024*1024)
				elif copy_scale =='G':
					copy_max_memory = str(int(copy_max_memory)*1024*1024*1024)
				else:
					print("**FATAL - invalid suffix in parameter copy_max_memory  (accepted values are (k)ilobytes, (M)egabytes, (G)igabytes.")
					sys.exit()
			self.copy_max_memory = copy_max_memory
		except KeyError as key_missing:
			print('Missing key %s in configuration file. check config/config-example.yaml for reference' % (key_missing, ))
			sys.exit()
		
		

class replica_engine(object):
	def __init__(self, conn_file):
		self.global_config = global_config(conn_file)
	
	def create_service_schema(self):
		print("ok")
	
	def list_connections(self):
		self.global_config.load_connection()
		print ("Connection key\t\tSource\t\tDestination\tType" )
		print ("==================================================================" )
		self.conn_list=self.global_config.connection["connections"]
		for connkey in self.conn_list:
			conndic = self.conn_list[connkey]
			print ("%s\t%s\t%s\t%s" % (connkey, conndic["src_conn"]["host"], conndic["dest_conn"]["host"] , conndic["src_conn"]["type"]))
	
	def show_connection(self, connkey):
		if connkey == 'all':
			print("**FATAL - no connection key specified. Use --connkey on the command line.\nAvailable connections " )
			sys.exit()
		self.global_config.load_connection()
		try:
			conndic = self.global_config.connection[connkey]
		except KeyError as key_missing:
			print("**FATAL - wrong connection key specified." )
			self.list_connections()
			sys.exit(2)
		self.global_config.set_conn_vars(conndic)
