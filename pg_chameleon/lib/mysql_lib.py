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
	
	
	def get_table_metadata(self):
		self.logger.debug("retrieving tables metadata")
		table_include = ""
		rep_tables = self.conn_pars["replicate_tables"]
		if len(rep_tables) > 0:
			self.logger.info("table copy limited to tables: %s" % ','.join(rep_tables))
			table_include="AND table_name IN ('"+"','".join(rep_tables)+"')"
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
		
		self.my_cursor.execute(sql_tables, (self.src_conn["replica_database"]))
		table_list=self.mysql_con.my_cursor.fetchall()
		for table in table_list:
			column_data=self.get_column_metadata(table[0])
			index_data=self.get_index_metadata(table[0])
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
	
	def get_master_status(self):
		t_sql_master="SHOW MASTER STATUS;"
		self.my_cursor.execute(t_sql_master)
		self.master_status=self.my_cursor.fetchall()		
		
		
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
		self.my_cursor.execute(t_sql_lock)
		self.get_master_status()
	
	def unlock_tables(self):
		""" unlock tables previously locked """
		t_sql_unlock="UNLOCK TABLES;"
		self.my_cursor.execute(t_sql_unlock)
	
	
	
	def init_replica(self):
		self.logger.debug(self.conn_pars)
		self.connect_db()
		self.lock_tables()
		self.logger.debug(self.master_status)
		self.unlock_tables()
		
