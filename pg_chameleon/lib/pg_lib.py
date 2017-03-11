import psycopg2
import sys
import json
import datetime
import decimal
import time
import base64
import os

class pg_encoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, datetime.time) or isinstance(obj, datetime.datetime) or  isinstance(obj, datetime.date) or isinstance(obj, decimal.Decimal):
			return str(obj)
		return json.JSONEncoder.default(self, obj)


class pg_engine(object):
	def __init__(self):
		self.sql_dir = "%s/.pg_chameleon/sql/" % os.path.expanduser('~')	
		
		self.type_dictionary = {
						'integer':'integer',
						'mediumint':'bigint',
						'tinyint':'integer',
						'smallint':'integer',
						'int':'integer',
						'bigint':'bigint',
						'varchar':'character varying',
						'text':'text',
						'char':'character',
						'datetime':'timestamp without time zone',
						'date':'date',
						'time':'time without time zone',
						'timestamp':'timestamp without time zone',
						'tinytext':'text',
						'mediumtext':'text',
						'longtext':'text',
						'tinyblob':'bytea',
						'mediumblob':'bytea',
						'longblob':'bytea',
						'blob':'bytea', 
						'binary':'bytea', 
						'decimal':'numeric', 
						'double':'double precision', 
						'double precision':'double precision', 
						'float':'float', 
						'bit':'integer', 
						'year':'integer', 
						'enum':'enum', 
						'set':'text', 
						'json':'text'
					}
					
	def connect_db(self):
		dest_pars = self.conn_pars['dest_conn']
		strconn = "dbname=%(pg_database)s user=%(user)s host=%(host)s password=%(password)s port=%(port)s"  % dest_pars
		self.pgsql_conn = psycopg2.connect(strconn)
		self.pgsql_conn .set_client_encoding(dest_pars["pg_charset"])
		self.pgsql_cur = self.pgsql_conn .cursor()
		self.logger.info("connection to dest database established")

	def set_autocommit(self, mode):
		self.logger.debug("changing session autocommit to %s" % mode)
		self.pgsql_conn.set_session(autocommit=mode)
	
	def disconnect_db(self):
		self.pgsql_conn.close()
	
	
	def check_service_schema(self):
		sql_check="""
					SELECT 
						count(*)
					FROM 
						information_schema.schemata  
					WHERE 
						schema_name='sch_chameleon'
			"""

		self.pgsql_cur.execute(sql_check)
		num_schema = self.pgsql_cur.fetchone()
		return num_schema
	
	
	def create_service_schema(self):
		try:
			self.connect_db()
			self.set_autocommit(True)
			num_schema  = self.check_service_schema()
			if num_schema[0] == 0:
				self.logger.info("Installing service schema")
				file_schema=open(self.sql_dir+'/create_schema.sql', 'rb')
				sql_schema=file_schema.read()
				file_schema.close()
				self.pgsql_cur.execute(sql_schema)
				self.logger.info("service schema created " )
			else:
				self.logger.error("the service schema is already created." )
		except Exception as e:
			self.logger.error("an error occurred when creating the service schema")
			self.logger.error(e)
