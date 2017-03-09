import psycopg2
import sys
import json
import datetime
import decimal
import time
import base64

class pg_encoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, datetime.time) or isinstance(obj, datetime.datetime) or  isinstance(obj, datetime.date) or isinstance(obj, decimal.Decimal):
			return str(obj)
		return json.JSONEncoder.default(self, obj)


class pg_engine(object):
	def __init__(self):
		self.type_dictionary={
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
		self.pgsql_cur=self.pgsql_conn .cursor()
		self.logger.info("connection to dest database established")

	def set_autocommit(self, mode):
		self.logger.debug("changing session autocommit to %s" % mode)
		self.pgsql_conn.set_session(autocommit=mode)
	
	def disconnect_db(self):
		self.pgsql_conn.close()
	
	
	def create_service_schema(self):
		try:
			self.connect_db()
			self.set_autocommit(True)
			self.logger.info("service schema created")
		except:
			self.logger.info("an error occurred when creating the service schema")
