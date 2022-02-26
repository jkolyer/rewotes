import sqlite3
from math import floor
from datetime import datetime
from enum import Enum
from cuid import cuid
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def dateSinceEpoch(mydate=datetime.now()):
    result = (mydate - datetime(1970, 1, 1)).total_seconds()
    return floor(result)

class BaseModel:
    def __init__(self):
        pass

class FileModel(BaseModel):

    @classmethod
    def table_name(cls):
        return 'FileStat'
    
    @classmethod
    def create_table_sql(cls):
        return ["""
        CREATE TABLE IF NOT EXISTS {table_name}
                  ( id TEXT PRIMARY KEY, 
                    fileSize INTEGER,
                    lastModified INTEGER,
                    permissions TEXT,
                    fileName TEXT,
                    filePath TEXT,
                    UNIQUE(fileName, filePath)
                  );
                """.format(table_name=cls.table_name()),
                f"CREATE INDEX IF NOT EXISTS IdxFileName ON {cls.table_name()}(fileName)",
                f"CREATE INDEX IF NOT EXISTS IdxFilePath ON {cls.table_name()}(filePath)",
                ]

class UploadJobModel(BaseModel):
    @classmethod
    def table_name(cls):
        return 'UploadJob'
    
    @classmethod
    def create_table_sql(cls):
        return ["""
        CREATE TABLE IF NOT EXISTS {table_name}
                  ( id TEXT PRIMARY KEY, 
                    batchId TEXT,
                    fileId TEXT,
                    status INTEGER,
                    createdAt INTEGER,
                    FOREIGN KEY (fileId) REFERENCES {file_table_name}(id),
                    FOREIGN KEY (batchId) REFERENCES {batch_table_name}(id)
                  );
               """.format(
                   table_name=cls.table_name(),
                   file_table_name=FileModel.table_name(),
                   batch_table_name=BatchJobModel.table_name()
               ),
                f"CREATE INDEX IF NOT EXISTS IdxJobFile ON {cls.table_name()}(fileId);",
                f"CREATE INDEX IF NOT EXISTS IdxStatus ON {cls.table_name()}(status);"]

class BatchStatus(Enum):
    PENDING = 1
    IN_PROGRESS = 2
    COMPLETED = 3
    FAILED = 4

class BatchJobModel(BaseModel):

    def __init__(self, props):
        self.id = props[0]
        self.status = props[1]
        self.createdAt = props[2]

    @classmethod
    def table_name(cls):
        return 'BatchJob'
    
    @classmethod
    def create_table_sql(cls):
        return ["""
        CREATE TABLE IF NOT EXISTS {table_name}
        ( id TEXT PRIMARY KEY, 
        status INTEGER,
        createdAt INTEGER
        );
        """.format(table_name=cls.table_name()),
                f"CREATE INDEX IF NOT EXISTS IdxCreatedAt ON {cls.table_name()}(createdAt);"]

    @classmethod
    def new_record_sql(cls):
        return """
        INSERT INTO {table_name}
                  ( id, status, createdAt )
                  VALUES 
                  ( '{idval}', {status}, {createdAt} )
                """.format(
                    table_name=cls.table_name(),
                    idval=cuid(),
                    status=BatchStatus.PENDING.value,
                    createdAt=dateSinceEpoch(),
                )
    
    @classmethod
    def query_latest(cls, db_conn):
        sql = f"SELECT * FROM {cls.table_name()} ORDER BY createdAt DESC LIMIT 1"
        cursor = db_conn.cursor()
        try:
            result = cursor.execute(sql).fetchall()
            if len(result) > 0:
                logger.debug(f"BatchJobModel.query_latest: {result}")
                model = BatchJobModel(result[0])
                return model
        except sqlite3.Error as error:
            logger.error(f"Error running sql: {error}; ${sql}")
        finally:
            cursor.close()
        return None
        
    
    def generate_file_records(self):
        return 'BatchJob'
    
    
