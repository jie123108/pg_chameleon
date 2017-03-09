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
					
					
	def create_service_schema(self):
		time.sleep(30)
		print ("schema created")
