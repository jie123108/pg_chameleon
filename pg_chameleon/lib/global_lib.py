import yaml
import sys
import os
from tabulate import tabulate
import logging
from logging.handlers  import TimedRotatingFileHandler
from pg_chameleon import pg_engine, mysql_engine
from daemonize import Daemonize
from distutils.sysconfig import get_python_lib
from shutil import copy

		
class replica_logging(object):
	def __init(self):
		self.logger=None

	def init_logger(self, log_dest,log_level,log_days_keep, log_dir, connkey, debug_mode):
		"""
			
		"""
		log_file= '%s/%s.log' % (log_dir,connkey)
		log_file = os.path.expanduser(log_file)
		self.logger = logging.getLogger(__name__)
		self.logger.setLevel(logging.DEBUG)
		self.logger.propagate = False
		formatter = logging.Formatter("%(asctime)s: [%(levelname)s] - %(filename)s (%(lineno)s): %(message)s", "%b %e %H:%M:%S")
		
		if log_dest=='stdout' or debug_mode:
			fh=logging.StreamHandler(sys.stdout)
			
		elif log_dest=='file':
			fh = TimedRotatingFileHandler(log_file, when="d",interval=1,backupCount=log_days_keep)
		
		if log_level=='debug' or debug_mode:
			fh.setLevel(logging.DEBUG)
		elif log_level=='info':
			fh.setLevel(logging.INFO)
			
		fh.setFormatter(formatter)
		self.logger.addHandler(fh)
		return fh


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
		local_conndir= "%s/.pg_chameleon/connection/" % os.path.expanduser('~')	
		
		if connection_file:
			self.connection_file = '%s/%s'%(local_conndir, connection_file)
			if load_config:
				self.load_config()
		else:
			print("**FATAL - invalid connection file specified **\Expected filename got %s." % (self.connection_file))
			sys.exit(1)
		self.lst_skip=["src_conn", "dest_conn", "connections"]
		self.conn_pars={}
		self.log_kwargs={}
		
	def set_log_kwargs(self, connkey = "all"):
		self.load_connection()
		log_pars = ['log_dest','log_level','log_days_keep', 'log_dir']
		for par in log_pars:
			self.log_kwargs[par] = self.conn_pars[par]
		self.log_kwargs["connkey"] = connkey
	
	def set_copy_max_memory(self):
		copy_max_memory = str(self.conn_pars["copy_max_memory"])[:-1]
		copy_scale=str(self.conn_pars["copy_max_memory"] )[-1]
		try:
			int(copy_scale)
			copy_max_memory = self.conn_pars["copy_max_memory"]
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
		self.conn_pars["copy_max_memory"] = copy_max_memory
	
	
	def set_conn_vars(self, connkey):
		try:
			self.currentconn= self.connection["connections"][connkey]
			self.conn_pars["connections"] = None
			for key in self.currentconn:
				self.conn_pars[key] = self.currentconn[key]
			self.set_copy_max_memory()	
			self.conn_pars["pid_dir"] = os.path.expanduser(self.conn_pars["pid_dir"])
			self.conn_pars["log_dir"] = os.path.expanduser(self.conn_pars["log_dir"])
			self.conn_pars["connkey"] = connkey
		except KeyError as e:
			print("wrong connection key specified %s" % (e, ))
			sys.exit(1)
		except:
			raise
		
	
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
		for key in conndic:
			self.conn_pars[key] = conndic[key]
		self.set_copy_max_memory()	
		
		

class replica_engine(object):
	def __init__(self, args):
		self.global_config = global_config(args.connfile)
		self.pg_eng = pg_engine()
		self.my_eng = mysql_engine()
		self.lst_yes = ['yes',  'Yes', 'y', 'Y']
		self.fh = None
		python_lib=get_python_lib()
		cham_dir = "%s/.pg_chameleon" % os.path.expanduser('~')	
		local_conn = "%s/connection/" % cham_dir 
		local_logs = "%s/logs/" % cham_dir 
		local_pid = "%s/pid/" % cham_dir 
		self.global_conn_example = '%s/pg_chameleon/connection/connection-example.yaml' % python_lib
		self.local_conn_example = '%s/connection-example.yaml' % local_conn
		self.conf_dirs=[
			cham_dir, 
			local_conn, 
			local_logs, 
			local_pid, 
			
		]
		self.args = args
		self.set_config()
		
		
	def set_config(self):
		""" 
			The method loops the list self.conf_dirs creating it only if missing.
			
			The method checks the freshness of the config-example.yaml file and copies the new version
			from the python library determined in the class constructor with get_python_lib().
			
			If the configuration file is missing the method copies the file with a different message.
		
		"""

		for confdir in self.conf_dirs:
			if not os.path.isdir(confdir):
				print ("creating directory %s" % confdir)
				os.mkdir(confdir)
		
		if os.path.isfile(self.local_conn_example):
			if os.path.getctime(self.global_conn_example)>os.path.getctime(self.local_conn_example):
				print ("updating connection example with %s" % self.local_conn_example)
				copy(self.global_conn_example, self.local_conn_example)
		else:
			print ("copying connection  example in %s" % self.local_conn_example)
			copy(self.global_conn_example, self.local_conn_example)
		
	
	def init_logger(self, args, log_dest):
		self.global_config.set_log_kwargs(args.connkey)
		replog = replica_logging()
		if log_dest:
			self.global_config.log_kwargs["log_dest"] = log_dest
		self.global_config.log_kwargs["debug_mode"] = args.debug
		self.fh = replog.init_logger(**self.global_config.log_kwargs)
		return replog
	
	def init_replica(self, args):
		replog = self.init_logger(args, None)
		self.my_eng.logger = replog.logger
		self.global_config.set_conn_vars(args.connkey)
		self.my_eng.conn_pars = self.global_config.conn_pars
		self.my_eng.pg_eng = self.pg_eng
		keep_fds = [self.fh.stream.fileno()]
		pid='%s/%s.pid' % (self.global_config.conn_pars["pid_dir"], args.connkey)
		if self.global_config.log_kwargs["log_dest"]  == 'stdout':
			self.my_eng.init_replica()
		else:
			daemon = Daemonize(app="test_app", pid=pid, action=self.my_eng.init_replica, foreground=False , keep_fds=keep_fds)
			daemon.start()
		
	
	def add_replica(self, args):
		replog = self.init_logger(args, 'stdout')
		self.global_config.set_conn_vars(args.connkey)
		self.pg_eng.conn_pars = self.global_config.conn_pars
		self.pg_eng.logger = replog.logger
		self.pg_eng.add_replica()
		
	def drop_replica(self, args):
		replog = self.init_logger(args, 'stdout')
		self.global_config.set_conn_vars(args.connkey)
		self.pg_eng.conn_pars = self.global_config.conn_pars
		self.pg_eng.logger = replog.logger
		if args.noprompt:
			drop_rep = 'YES'
		else:
			drp_msg = 'Dropping the replica %s will remove any replica reference. THERE IS NO UNDO!m\n Are you sure? YES/No\n'  % args.connkey
			drop_rep = input(drp_msg)
		if drop_rep == 'YES':
			self.pg_eng.drop_replica()
		elif drop_rep in  self.lst_yes:
			print('Please type YES all uppercase to confirm')
		sys.exit()
		
		
		
	def create_service_schema(self, args):
		
		if args.connkey == 'all':
			print('You should specify a connection key')
			self.list_connections(args)
		else:
			replog = self.init_logger(args, 'stdout')
			#keep_fds = [fh.stream.fileno()]
			self.global_config.set_conn_vars(args.connkey)
			#pid='%s/%s.pid' % (self.global_config.conn_pars["pid_dir"], args.connkey)
			self.pg_eng.conn_pars=self.global_config.conn_pars
			self.pg_eng.logger=replog.logger
			self.pg_eng.create_service_schema()
			#daemon = Daemonize(app="test_app", pid=pid, action=self.pg_eng.create_service_schema, foreground=True, keep_fds=keep_fds)
			#daemon.start()
	
	def drop_service_schema(self, args):
		
		if args.connkey == 'all':
			print('You should specify a connection key')
			self.list_connections(args)
		else:
			
			replog = self.init_logger(args, 'stdout')
			self.global_config.set_conn_vars(args.connkey)
			self.pg_eng.conn_pars=self.global_config.conn_pars
			self.pg_eng.logger=replog.logger
			if args.noprompt:
				drop_sch = 'YES'
			else:
				drp_msg = 'Dropping the service schema from %s will DESTROY any replica reference.\n Are you sure? YES/No\n'  % args.connkey
				drop_sch = input(drp_msg)
			if drop_sch == 'YES':
				self.pg_eng.drop_service_schema()
			elif drop_sch in  self.lst_yes:
				print('Please type YES all uppercase to confirm')
			sys.exit()
			
			
	
	def list_connections(self, args):
		self.global_config.load_connection()
		tab_headers=["Connection key", "Source host", "Destination host", "Replica type"]
		tab_body=[]
		self.conn_list=self.global_config.connection["connections"]
		for connkey in self.conn_list:
			conndic = self.conn_list[connkey]
			tab_row=[connkey, conndic["src_conn"]["host"], conndic["dest_conn"]["host"] , conndic["src_conn"]["type"]]
			tab_body.append(tab_row)
		print(tabulate(tab_body, headers=tab_headers))
	
	def show_connection(self, args):
		tab_body=[]
		if args.connkey == 'all':
			print("**FATAL - no connection key specified. Use --connkey on the command line.\nAvailable connections " )
			self.list_connections(args)
			sys.exit()
		self.global_config.load_connection()
		try:
			self.global_config.set_conn_vars(args.connkey)
		except KeyError:
			print("**FATAL - wrong connection key specified." )
			self.list_connections()
			sys.exit(2)
		
		tab_headers=["Parameter", "Value"]
		for conn_par in self.global_config.conn_pars:
			if conn_par not in self.global_config.lst_skip:
				tab_row=[conn_par, self.global_config.conn_pars[conn_par]]
				tab_body.append(tab_row)
		print(tabulate(tab_body, headers=tab_headers))
			
		
