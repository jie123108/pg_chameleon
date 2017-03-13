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

class mysql_engine(object):
	def __init__(self):
		self.my_tables = {}
		self.conn_pars = {}
	
	
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
			column_data=self.get_column_metadata(table["table_name"])
			index_data=self.get_index_metadata(table["table_name"])
			dic_table={'name':table["table_name"], 'columns':column_data,  'indices': index_data}
			#self.my_tables[table["table_name"]]=dic_table
			self.logger.info(dic_table)
	
	
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
		if len(self.my_tables)>0:
			for table_name in self.my_tables:
				table = self.my_tables[table_name]
				self.locked_tables.append(table["name"])
			lock_tables = ", ".join(self.locked_tables) 
		t_sql_lock="FLUSH TABLES %s WITH READ LOCK;" % lock_tables
		self.my_dict_cursor.execute(t_sql_lock)
		self.get_master_status()
	
	def unlock_tables(self):
		""" unlock tables previously locked """
		t_sql_unlock="UNLOCK TABLES;"
		self.my_dict_cursor.execute(t_sql_unlock)
	
	
	
	def init_replica(self):
		self.logger.debug(self.conn_pars)
		self.connect_dict_db()
		self.lock_tables()
		self.get_table_metadata()
		self.logger.debug(self.master_status)
		#self.unlock_tables()
		self.disconnect_dict_db()
