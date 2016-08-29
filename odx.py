
import os 
import pyorient



class OrientDBRecord(object):
    def __init__(self, type_name, record_id=None, **kwargs):
        self.type_name = type_name
        self.attributes = {}

        
    def add_attribute(self, name, value):
        self.attributes[name] = value


    def json(self):
        pass # implement in subclasses


    
class VertexRecord(OrientDBRecord):
    def __init__(self, type_name):
        OrientDBRecord.__init__(self, type_name)


    def json(self):        
        return { '@%s' % self.type_name : self.attributes }

    
    
class EdgeRecord(OrientDBRecord):
    def __init__(self, type_name, from_vertex_id, to_vertex_id):
        OrientDBRecord.__init__(self, type_name)
        self.to_vertex_id = to_vertex_id
        self.from_vertex_id = from_vertex_id
        self.add_attribute('@to', self.to_vertex_id)
        self.add_attribute('@from', self.from_vertex_id)

        
    def json(self):        
        return { '@%s' % self.type_name : self.attributes }

    

class OrientDatabase(object):
    def __init__(self, hostname, port):
        self.host = hostname
        self.port = port
        self.client = pyorient.OrientDB(self.host, 2424)
        self.session_id = None

        
    def connect(self, instance_admin_username, instance_admin_password):
        self.session_id = self.client.connect(instance_admin_username, instance_admin_password)

    
    def open(self, db_name, username, password):
        if not self.session_id:
            raise Exception('Cannot open database %s without first connecting to a server instance. Please call connect() first.' 
                            % db_name)
        self.client.db_open(db_name, username, password)


    def close(self):
        self.client.db_close()


    def open_transaction_context(self):
        return self.client.tx_commit()


    def create_new_record(self, orientdb_record):
        return self.client.record_create(-1, orientdb_record.json())

    
    def execute_query(self, query_string):
        if not self.session_id:
            raise Exception('Cannot execute query without first connecting to a server instance. Please call connect() first.')

        return self.client.command(query_string)
    
    

class OrientDBPersistenceManager(object):
    def __init__(self, orient_database):
        self.database = orient_database

        
    def save_record_txn(self, odb_record):
        txn = self.database.open_transaction_context()
        txn.begin()
        try:
            rec_position = self.database.create_new_record(odb_record)
            txn.attach(rec_position)        
            return txn.commit()
        except Exception, err:
            txn.rollback()
            raise err

    
    def save_batch_txn(self, odb_record_array):
        txn = self.database.open_transaction_context()
        txn.begin()
        try:
            for rec in odb_record_array:
                rec_position = self.database.create_new_record(rec)
                txn.attach(rec_position)
            
            return txn.commit()
        except Exception, err:
            txn.rollback()
            raise err
