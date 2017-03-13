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
		self.table_ddl={}
		self.idx_ddl={}
		self.type_ddl={}
		
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
		self.dest_conn= self.conn_pars['dest_conn']
		strconn = "dbname=%(pg_database)s user=%(user)s host=%(host)s password=%(password)s port=%(port)s"  % self.dest_conn
		self.pgsql_conn = psycopg2.connect(strconn)
		self.pgsql_conn .set_client_encoding(self.dest_conn["pg_charset"])
		self.pgsql_cur = self.pgsql_conn .cursor()
		self.logger.info("connection to dest database established")

	def set_autocommit(self, mode):
		self.logger.debug("changing session autocommit to %s" % mode)
		self.pgsql_conn.set_session(autocommit=mode)
	
	def disconnect_db(self):
		self.pgsql_conn.close()
	
	def create_schema(self):
		self.connect_db()
		self.set_autocommit(True)
		self.dest_schema = self.dest_conn["destination_schema"]
		
		sql_drop = "DROP SCHEMA IF EXISTS " + self.dest_schema + " CASCADE;"
		
		sql_create = " CREATE SCHEMA IF NOT EXISTS " + self.dest_schema + ";"
		self.logger.info("dropping the schema %s " % self.dest_schema)
		self.pgsql_cur.execute(sql_drop)
		self.logger.info("creating the schema %s " % self.dest_schema)
		self.pgsql_cur.execute(sql_create)
		
	
	def build_tab_ddl(self):
		""" the function iterates over the list l_tables and builds a new list with the statements for tables"""
		for table_name in self.table_metadata:
			table=self.table_metadata[table_name]
			columns=table["columns"]
			
			ddl_head="CREATE TABLE "+'"'+table["name"]+'" ('
			ddl_tail=");"
			ddl_columns=[]
			ddl_enum=[]
			for column in columns:
				if column["is_nullable"]=="NO":
					col_is_null="NOT NULL"
				else:
					col_is_null="NULL"
				column_type=self.type_dictionary[column["data_type"]]
				if column_type=="enum":
					enum_type="enum_"+table["name"]+"_"+column["column_name"]
					sql_drop_enum='DROP TYPE IF EXISTS '+enum_type+' CASCADE;'
					sql_create_enum="CREATE TYPE "+enum_type+" AS ENUM "+column["enum_list"]+";"
					ddl_enum.append(sql_drop_enum)
					ddl_enum.append(sql_create_enum)
					column_type=enum_type
				if column_type=="character varying" or column_type=="character":
					column_type=column_type+"("+str(column["character_maximum_length"])+")"
				if column_type=='numeric':
					column_type=column_type+"("+str(column["numeric_precision"])+","+str(column["numeric_scale"])+")"
				if column_type=='bit' or column_type=='float':
					column_type=column_type+"("+str(column["numeric_precision"])+")"
				if column["extra"]=="auto_increment":
					column_type="bigserial"
				ddl_columns.append('"'+column["column_name"]+'" '+column_type+" "+col_is_null )
			def_columns=str(',').join(ddl_columns)
			self.type_ddl[table["name"]]=ddl_enum
			self.table_ddl[table["name"]]=ddl_head+def_columns+ddl_tail
	
	def create_tables(self):
		sql_path=" SET search_path="+self.dest_schema+";"
		self.pgsql_cur.execute(sql_path)
		for table in self.table_ddl:
			try:
				ddl_enum=self.type_ddl[table]
				for sql_type in ddl_enum:
					self.pgsql_cur.execute(sql_type)
			except psycopg2.Error as e:
				self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
				self.logger.error(sql_type)
				
			sql_create=self.table_ddl[table]
			try:
				self.pgsql_cur.execute(sql_create)
			except psycopg2.Error as e:
				self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
				self.logger.error(sql_create)
			self.store_table(table)


	def store_table(self, table_name):
		table_data=self.table_metadata[table_name]
		for index in table_data["indices"]:
			if index["index_name"]=="PRIMARY":
				sql_insert=""" INSERT INTO sch_chameleon.t_replica_tables 
										(
											i_id_replica,
											v_table_name,
											v_schema_name,
											v_table_pkey
										)
										VALUES (
														%s,
														%s,
														%s,
														ARRAY[%s]
													)
										ON CONFLICT (i_id_replica,v_table_name,v_schema_name)
											DO UPDATE 
												SET v_table_pkey=EXCLUDED.v_table_pkey
										;
								"""
				self.pgsql_cur.execute(sql_insert, (self.i_id_replica, table_name, self.dest_schema, index["index_columns"].strip()))	
	
	
	def set_replica_id(self, replica_status):
		sql_source = """
					UPDATE sch_chameleon.t_replica
					SET
						enm_status=%s
					WHERE
						t_conn_key=%s
					RETURNING i_id_replica,t_dest_schema
				;
			"""
		
		self.pgsql_cur.execute(sql_source, (replica_status, self.conn_pars["connkey"]))
		rep_data=self.pgsql_cur.fetchone()
		try:
			self.i_id_replica=rep_data[0]
			self.dest_schema=rep_data[1]
		except:
			self.logger.error("Replica %s is not registered." % self.conn_pars["connkey"])
			sys.exit()
	
	
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
			num_schema = self.check_service_schema()
			if num_schema[0] == 0:
				self.logger.info("Installing the service schema")
				file_schema = open(self.sql_dir+'/create_schema.sql', 'rb')
				sql_schema = file_schema.read()
				file_schema.close()
				self.pgsql_cur.execute(sql_schema)
				self.logger.info("service schema created " )
			else:
				self.logger.error("the service schema is already created." )
		except Exception as e:
			self.logger.error("an error occurred when creating the service schema")
			self.logger.error(e)
	
	def drop_service_schema(self):
		try:
			self.connect_db()
			self.set_autocommit(True)
			num_schema = self.check_service_schema()
			if num_schema[0] > 0:
				self.logger.info("Dropping the service schema")
				file_schema = open(self.sql_dir+'/drop_schema.sql', 'rb')
				sql_schema = file_schema.read()
				file_schema.close()
				self.pgsql_cur.execute(sql_schema)
				self.logger.info("service schema removed " )
			else:
				self.logger.error("the service schema is already dropped." )
		except Exception as e:
			self.logger.error("an error occurred when creating the service schema")
			self.logger.error(e)
	
	def drop_replica(self):
		self.connect_db()
		self.set_autocommit(True)
		connkey = self.conn_pars["connkey"]	
		sql_delete = """ DELETE FROM sch_chameleon.t_replica
				WHERE  t_conn_key=%s; """
		self.logger.info("removing replica %s from the replica catalogue" % connkey )
		self.pgsql_cur.execute(sql_delete, (connkey, ))

	def add_replica(self):
		self.connect_db()
		self.set_autocommit(True)
		connkey = self.conn_pars["connkey"]	
		dest_schema = self.conn_pars["dest_conn"]["destination_schema"]	
		self.logger.info("checking if replica %s is already registered " % connkey )
		sql_source = """
					SELECT 
						count(i_id_replica)
					FROM 
						sch_chameleon.t_replica
					WHERE 
						t_conn_key=%s
				;
			"""
		self.pgsql_cur.execute(sql_source, (connkey, ))
		rep_data = self.pgsql_cur.fetchone()
		cnt_rep= rep_data [0]
		if cnt_rep== 0:
			self.logger.info("adding replica %s to the replica catalogue " % connkey )
			sql_add = """INSERT INTO sch_chameleon.t_replica
						( t_conn_key,t_dest_schema) 
					VALUES 
						(%s,%s); """
			self.pgsql_cur.execute(sql_add, (connkey, dest_schema ))
		else:
			self.logger.error("replica %s is already registered " % connkey )
			
	
