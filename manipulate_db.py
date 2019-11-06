from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine,MetaData, Table, insert, Column,Integer, String,update,delete,Float,Boolean
import pyodbc 
from constants import *
import urllib

class DatabaseMarshal(object):
    #initialize class with database connection 
    def __init__(self,Database_name):
        self.metadata = MetaData()
        #create database connection
        database_connection_string = "Driver={};Server={};Database={};Uid={};Pwd={};Encrypt={};TrustServerCertificate={};Connection Timeout={};".format(
                                        DRIVER,NSIO_SERVER,Database_name,NSIO_UID,NSIO_PWD,ENCRYPT,TRUSTSERVERCERTIFICATE,CONNECTION_TIMEOUT)
        db_connection=urllib.parse.quote_plus(database_connection_string)
        self.engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % db_connection)
        self.session = sessionmaker(bind=self.engine)()   
      
    #function used to convert list of objects to dictionary
    def _convert_list_to_dict(self,details_list):
        details_dict={}
        for item in details_list:
            details_dict.update(item)
        return(details_dict)

    #function used by create column to add columns to created table
    def _add_column(self,table_name, column):
            column_name = column.compile(dialect=self.engine.dialect)
            column_type = column.type.compile(self.engine.dialect)
            self.engine.execute('ALTER TABLE %s ADD %s %s' % (table_name, column_name, column_type))
    
    #method used by create table to convert data type to sqlalchemy type
    def _initiate_column(self, col_name, col_type):
        if col_type.lower() is "int":
            col_type = Integer
        elif col_type.lower() is "str":
            col_type = String
        elif col_type.lower() is "float":
            col_type= Float
        elif col_type.lower() is "boolean":
            col_type= Boolean
        else:
            col_type = String
        #convert to sqlalchemy column format
        col = Column(col_name, col_type) 
        return (col)

    #add columns to table
    def _create_columns(self, table_name,_table_details):
        table_list=[]
        for item in _table_details:
            if item.get('column'):
                col_name = item.get('column')
                col_type = item.get('type')
                table_list.append(self._initiate_column(col_name, col_type)) 
        for item in table_list:
            #add each column to table using _add_column function 
            self._add_column(table_name,item) 

    #use to load already created table from the database
    def _reload_table(self,_record_details):
        record_details_dict=self._convert_list_to_dict(_record_details) 
        table_name=record_details_dict.pop("table_name") 
        record_id=record_details_dict.pop("record_id",None) 
        #load table from db
        table = Table(table_name, self.metadata,autoload_with=self.engine)
        return(record_details_dict,table,record_id,table_name)

    #check that table and record are available before update, delete and insert
    def check_availability_and_execute(self,record_details, function_name):
        try:
            record_details_dict,table,record_id,table_name= self._reload_table(record_details)
            #check if table exist in the database
            if self.engine.dialect.has_table(self.engine.connect(), table_name):   
                #check if record id exist for update and delete methods
                if self.session.query(table).filter_by(id=record_id).first():      
                    if function_name is "update":
                        #func(table,record_id,record_details_dict)
                        i = update(table).where(table.c.id==record_id).values(record_details_dict)
                    elif function_name is "delete":
                        i = delete(table).where(table.c.id==record_id)
                else:
                    #insert does not have a record_id so it is done outside the if condition
                    if function_name is "insert" :                      
                        i = insert(table).values(record_details_dict)
                    else:
                        print("++++++++ Record id does not exist")     
                        return()
                self.session.execute(i)
                self.session.commit()
                print('+++++++++++++ record {} successful'.format(function_name))
        except Exception as err:
            self.session.rollback()   #rollsback a session if error exist
            print("+++++++++Record {} failed - {} ".format(function_name, err))

    #create table with column details
    def create_table(self,table_details):
        try:
            table_details_dict= self._convert_list_to_dict(table_details)
            table_name=table_details_dict.pop("table_name")
            #check if table does not already exist
            if not self.engine.dialect.has_table(self.engine.connect(), table_name):
                #create a table with a single column called id which is the primary key
                table = Table(table_name, self.metadata, Column('id', Integer, primary_key=True))
                table.metadata.create_all(self.engine)
                self._create_columns(table_name,table_details) 
                print('+++++++++ table created')
            else : 
                print("+++++++++ table already exist, try with another table name")
        except Exception as err:
            print("++++++++ table creation failed - [ %s ] " % (err))
    
    #insert record into column
    def insert_record(self, record_details):   
        self.check_availability_and_execute(record_details, "insert")

    #update rcord in table
    def update_record(self,record_details):
        self.check_availability_and_execute(record_details, "update")    
        
    #delete record in table
    def delete_record(self,record_details):
        self.check_availability_and_execute(record_details, "delete")  
    

    

'''
###############sample 

test=DatabaseMarshal('nsio_bir_metrics')
test.create_table([{'table_name':"nsio_table"},{'column':'book1','type':'str'},{'column':'book2','type':'str'}])
test.insert_record([{"table_name":"nsio_table"},{"book1": "idowu"}, {"book2": "kayode"}])
test.update_record([{"record_id":1},{"table_name":"nsio_table"},{"book1": "kayode"}, {"book2": "idowu"}])
test.delete_record([{"record_id":1},{"table_name":"nsio_table"}])

'''