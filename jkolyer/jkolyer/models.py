from math import floor
from datetime import datetime
from enum import Enum
from cuid import cuid

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
                    filePath TEXT
                  );
                """.format(table_name=cls.table_name()),
                'CREATE INDEX IF NOT EXISTS IdxFileName ON FileStat(fileName)',
                'CREATE INDEX IF NOT EXISTS IdxFilePath ON FileStat(filePath)',
                'CREATE INDEX IF NOT EXISTS IdxFileSize ON FileStat(fileSize)'
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
                    updatedAt INTEGER,
                    FOREIGN KEY (fileId) REFERENCES {file_table_name}(id),
                    FOREIGN KEY (batchId) REFERENCES {batch_table_name}(id)
                  );
               """.format(
                   table_name=cls.table_name(),
                   file_table_name=FileModel.table_name(),
                   batch_table_name=BatchJobModel.table_name()
               ),
                'CREATE INDEX IF NOT EXISTS IdxJobFile ON UploadJob(fileId);',
                'CREATE INDEX IF NOT EXISTS IdxStatus ON UploadJob(status);'
                ]


class BatchStatus(Enum):
    PENDING = 1
    IN_PROGRESS = 2
    COMPLETED = 3
    FAILED = 4

class BatchJobModel(BaseModel):

    @classmethod
    def table_name(cls):
        return 'BatchJob'
    
    @classmethod
    def create_table_sql(cls):
        return ["""
                  CREATE TABLE IF NOT EXISTS {table_name}
                  ( id TEXT PRIMARY KEY, 
                    status INTEGER,
                    createdAt INTEGER,
                    updatedAt INTEGER
                  );
                """.format(table_name=cls.table_name())
                ]

    @classmethod
    def new_record_sql(cls):
        return """
        INSERT INTO {table_name}
                  ( id, status, createdAt, updatedAt )
                  VALUES 
                  ( '{idval}', {status}, {createdAt}, {updatedAt} )
                """.format(
                    table_name=cls.table_name(),
                    idval=cuid(),
                    status=BatchStatus.PENDING.value,
                    createdAt=dateSinceEpoch(),
                    updatedAt=dateSinceEpoch()
                )
    
    def generate_file_records(self):
        return 'BatchJob'
    
    
    
