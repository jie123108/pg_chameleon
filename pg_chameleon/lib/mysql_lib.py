import io
import pymysql
import sys
import codecs
import binascii
import datetime
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from pymysqlreplication.event import RotateEvent
from os import remove
class mysql_engine(object):
	def __init__(self):
		self.my_tables = {}
		self.table_file={}
		self.conn_pars = {}
	
	def copy_table_data(self):
		copy_max_memory = self.conn_pars["copy_max_memory"]
		copy_mode = self.conn_pars["copy_mode"]
		my_database = self.src_conn["replica_database"]
		self.logger.info("start copy loop")
		self.connect_db()
		for table_name in self.my_tables:
			out_file = '/tmp/%s_%s.csv' % (my_database, table_name)
			self.logger.info("copying data for table %s" % (table_name))
			slice_insert = []
			table = self.my_tables[table_name]
			table_name = table["name"]
			table_columns = table["columns"]
			self.logger.debug("estimating rows in "+table_name)
			sql_count = """ 
					SELECT 
							table_rows,
							CASE
								WHEN avg_row_length>0
								then
									round(("""+copy_max_memory+"""/avg_row_length))
							ELSE
								0
							END as copy_limit
						FROM 
							information_schema.TABLES 
						WHERE 
								table_schema=%s 
							AND	table_type='BASE TABLE'
							AND table_name=%s 
						;
			"""
			self.my_dict_cursor.execute(sql_count, (my_database, table_name))
			count_rows = self.my_dict_cursor.fetchone()
			total_rows = count_rows["table_rows"]
			copy_limit = int(count_rows["copy_limit"])
			if copy_limit == 0:
				copy_limit = 1000000
			num_slices = int(total_rows//copy_limit)
			range_slices = list(range(num_slices+1))
			total_slices = len(range_slices)
			slice = range_slices[0]
			self.logger.debug("%s will be copied in %s slices of %s rows"  % (table_name, total_slices, copy_limit))
			columns_csv = self.generate_select(table_columns, mode="csv")
			columns_ins = self.generate_select(table_columns, mode="insert")
			csv_data=""
			sql_out="SELECT "+columns_csv+" as data FROM "+table_name+";"
			try:
				self.logger.debug("Executing query for table %s"  % (table_name, ))
				self.my_cursor.execute(sql_out)
			except:
				self.logger.error("error when pulling data from %s. sql executed: " % (table_name, sql_out))
			self.logger.debug("Starting extraction loop for table %s"  % (table_name, ))
			while True:
				csv_results = self.my_cursor.fetchmany(copy_limit)
				if len(csv_results) == 0:
					break
				csv_data="\n".join(d[0] for d in csv_results )
				
				if copy_mode=='direct':
					csv_file=io.StringIO()
					csv_file.write(csv_data)
					csv_file.seek(0)

				if copy_mode=='file':
					csv_file=codecs.open(out_file, 'wb', self.src_conn["my_charset"])
					csv_file.write(csv_data)
					csv_file.close()
					csv_file=open(out_file, 'rb')
					
				try:
					raise
					self.pg_eng.copy_data(table_name, csv_file, self.my_tables)
					self.print_progress(slice+1,total_slices, table_name)
				except:
					self.logger.info("table %s error in PostgreSQL copy, saving slice number for the fallback to insert statements " % (table_name, ))
					slice_insert.append(slice)
				
				slice+=1
				csv_file.close()
			try:
				remove(out_file)
			except:
				self.logger.debug("Skipping not existing file %s"  % (out_file, ))
			if len(slice_insert)>0:
				ins_arg=[]
				ins_arg.append(slice_insert)
				ins_arg.append(table_name)
				ins_arg.append(columns_ins)
				ins_arg.append(copy_limit)
				self.insert_table_data(ins_arg)
		self.disconnect_db()
	
	def insert_table_data(self, ins_arg):
		"""fallback to inserts for table and slices """
		slice_insert = ins_arg[0]
		table_name = ins_arg[1]
		columns_ins = ins_arg[2]
		copy_limit = ins_arg[3]
		cnt_slice = 1
		total_slices = len(slice_insert)
		for slice in slice_insert:
			sql_out="SELECT "+columns_ins+"  FROM "+table_name+" LIMIT "+str(slice*copy_limit)+", "+str(copy_limit)+";"
			self.my_dict_cursor.execute(sql_out)
			insert_data =  self.my_dict_cursor.fetchall()
			self.logger.info("insert in table %s, slice %s of %s" %(table_name, cnt_slice, total_slices))
			self.pg_eng.insert_data(table_name, insert_data , self.my_tables)
			
			cnt_slice=+1

	
	def print_progress (self, iteration, total, table_name):
		if total>1:
			self.logger.info("Table %s copied %d %%" % (table_name, 100 * float(iteration)/float(total)))
		else:
			self.logger.debug("Table %s copied %d %%" % (table_name, 100 * float(iteration)/float(total)))
	
	
	def generate_select(self, table_columns, mode="csv"):
		column_list=[]
		columns = ""
		if mode == "csv":
			for column in table_columns:
					column_list.append("COALESCE(REPLACE("+column["column_csv"]+", '\"', '\"\"'),'NULL') ")
			columns="REPLACE(CONCAT('\"',CONCAT_WS('\",\"',"+','.join(column_list)+"),'\"'),'\"NULL\"','NULL')"
		if mode == "insert":
			for column in table_columns:
				column_list.append(column["column_select"])
			columns=','.join(column_list)
		return columns
	
	def get_column_metadata(self, table):
		hexify = self.conn_pars["hexify"]
		sql_columns="""
					SELECT 
						column_name,
						column_default,
						ordinal_position,
						data_type,
						character_maximum_length,
						extra,
						column_key,
						is_nullable,
						numeric_precision,
						numeric_scale,
						CASE 
							WHEN data_type="enum"
						THEN	
							SUBSTRING(COLUMN_TYPE,5)
						END AS enum_list,
						CASE
							WHEN 
								data_type IN ('"""+"','".join(hexify)+"""')
							THEN
								concat('hex(',column_name,')')
							WHEN 
								data_type IN ('bit')
							THEN
								concat('cast(`',column_name,'` AS unsigned)')
						ELSE
							concat('`',column_name,'`')
						END
						AS column_csv,
						CASE
							WHEN 
								data_type IN ('"""+"','".join(hexify)+"""')
							THEN
								concat('hex(',column_name,')')
							WHEN 
								data_type IN ('bit')
							THEN
								concat('cast(`',column_name,'` AS unsigned) AS','`',column_name,'`')
						ELSE
							concat('`',column_name,'`')
						END
						AS column_select
			FROM 
						information_schema.COLUMNS 
			WHERE 
									table_schema=%s
						AND 	table_name=%s
			ORDER BY 
							ordinal_position
			;
		"""
		self.my_dict_cursor.execute(sql_columns, (self.src_conn["replica_database"], table))
		column_data=self.my_dict_cursor.fetchall()
		return column_data

	def get_index_metadata(self, table):
		sql_index="""
				SELECT 
					index_name,
					non_unique,
					GROUP_CONCAT(concat('"',column_name,'"') ORDER BY seq_in_index) as index_columns
				FROM
					information_schema.statistics
				WHERE
									table_schema=%s
						AND 	table_name=%s
						AND	index_type = 'BTREE'
				GROUP BY 
					table_name,
					non_unique,
					index_name
				;
		"""
		self.my_dict_cursor.execute(sql_index, (self.src_conn["replica_database"], table))
		index_data=self.my_dict_cursor.fetchall()
		return index_data
	
	def get_table_metadata(self):
		self.logger.debug("retrieving tables metadata")
		table_include = ""
		rep_tables = self.conn_pars["replicate_tables"]
		if rep_tables:
			self.logger.info("table copy limited to tables: %s" % ','.join(rep_tables))
			table_include = "AND table_name IN ('"+"','".join(rep_tables)+"')"
		sql_tables="""
				SELECT 
							table_schema,
							table_name
				FROM 
							information_schema.TABLES 
				WHERE 
										table_type='BASE TABLE' 
							AND 	table_schema=%s
							"""+table_include+"""
				;
			"""
		
		self.my_dict_cursor.execute(sql_tables, (self.src_conn["replica_database"]))
		table_list=self.my_dict_cursor.fetchall()
		for table in table_list:
			self.logger.debug("collecting metadata for table %s" % (table["table_name"]))
			column_data=self.get_column_metadata(table["table_name"])
			index_data=self.get_index_metadata(table["table_name"])
			dic_table={'name':table["table_name"], 'columns':column_data,  'indices': index_data}
			self.my_tables[table["table_name"]]=dic_table
			
	
	
	def connect_db(self):
		"""  Establish connection with the database """
		self.src_conn=self.conn_pars["src_conn"]
		self.my_conn= pymysql.connect(
			host=self.src_conn["host"],
			user=self.src_conn["user"],
			password=self.src_conn["passwd"],
			db=self.src_conn["replica_database"],
			charset=self.src_conn["my_charset"],
			cursorclass=pymysql.cursors.SSCursor
		)
		self.my_cursor=self.my_conn.cursor()
	
	def connect_dict_db(self):
		"""  Establish connection with the database """
		self.src_conn=self.conn_pars["src_conn"]
		self.my_dict_conn= pymysql.connect(
			host=self.src_conn["host"],
			user=self.src_conn["user"],
			password=self.src_conn["passwd"],
			db=self.src_conn["replica_database"],
			charset=self.src_conn["my_charset"],
			cursorclass=pymysql.cursors.DictCursor
		)
		self.my_dict_cursor=self.my_dict_conn.cursor()
	
	def disconnect_dict_db(self):
		try:
			self.my_dict_conn.close()
		except:
			pass

	def disconnect_db(self):
		try:
			self.my_conn.close()
		except:
			pass
	
	def get_master_status(self):
		t_sql_master="SHOW MASTER STATUS;"
		self.my_dict_cursor.execute(t_sql_master)
		self.master_status=self.my_dict_cursor.fetchall()		
		
		
	def lock_tables(self):
		""" lock tables and get the log coords """
		self.locked_tables=[]
		lock_tables = ""
		for table_name in self.my_tables:
			table = self.my_tables[table_name]
			self.locked_tables.append(table["name"])
		lock_tables = ", ".join(self.locked_tables) 
		self.logger.info("flushing tables with read lock")
		t_sql_lock="FLUSH TABLES %s WITH READ LOCK;" % lock_tables
		self.my_dict_cursor.execute(t_sql_lock)
		self.get_master_status()
	
	def unlock_tables(self):
		""" unlock tables previously locked """
		self.logger.info("releasing the read lock")
		t_sql_unlock="UNLOCK TABLES;"
		self.my_dict_cursor.execute(t_sql_unlock)
	
	
	
	def init_replica(self):
		self.connect_dict_db()
		self.get_table_metadata()
		self.lock_tables()
		self.logger.info("Creating the schema in target database")
		self.pg_eng.conn_pars = self.conn_pars
		self.pg_eng.logger = self.logger
		self.pg_eng.table_metadata  = self.my_tables
		self.pg_eng.create_schema()
		self.pg_eng.set_replica_id("initialising")
		self.pg_eng.build_tab_ddl()
		self.pg_eng.create_tables()
		self.copy_table_data()
		self.unlock_tables()
		
		self.disconnect_dict_db()
