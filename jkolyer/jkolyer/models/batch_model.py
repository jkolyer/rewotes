import os
import stat
import sqlite3
import asyncio
from cuid import cuid
from pathlib import Path
import logging

from jkolyer.models.base_model import BaseModel, UploadStatus, dateSinceEpoch
from jkolyer.models.file_model import FileModel

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
        
class BatchJobModel(BaseModel):

    def __init__(self, props):
        self.id = props[0]
        self.status = props[1]
        self.created_at = props[2]
        self.root_dir = props[3]

    @classmethod
    def table_name(cls):
        return 'BatchJob'
    
    @classmethod
    def create_table_sql(cls):
        return ["""
        CREATE TABLE IF NOT EXISTS {table_name}
        ( id TEXT PRIMARY KEY, 
        status INTEGER,
        created_at INTEGER,
        root_dir TEXT
        );
        """.format(table_name=cls.table_name()),
                f"CREATE INDEX IF NOT EXISTS IdxCreatedAt ON {cls.table_name()}(created_at);",]

    @classmethod
    def new_record_sql(cls, root_dir):
        return """
        INSERT INTO {table_name}
                  ( id, status, created_at, root_dir )
                  VALUES 
                  ( '{idval}', {status}, {created_at}, '{root_dir}' )
                """.format(
                    table_name=cls.table_name(),
                    idval=cuid(),
                    status=UploadStatus.PENDING.value,
                    created_at=dateSinceEpoch(),
                    root_dir=root_dir,)
    
    @classmethod
    def query_latest(cls):
        sql = f"SELECT * FROM {cls.table_name()} ORDER BY created_at DESC LIMIT 1"
        cursor = cls.db_conn.cursor()
        try:
            result = cursor.execute(sql).fetchall()
            if len(result) == 0: return None
            
            # logger.debug(f"BatchJobModel.query_latest: {result}")
            model = BatchJobModel(result[0])
            return model
        
        except sqlite3.Error as error:
            logger.error(f"Error running sql: {error}; ${sql}")
        finally:
            cursor.close()
        return None

    def generate_file_records(self):
        cursor = self.db_conn.cursor()
        file_count = 0
        try:
            for file_path in Path(self.root_dir).rglob('*'):
                fstat = os.stat(file_path)
                fmode = fstat.st_mode
                if stat.S_ISDIR(fmode): continue

                # logger.debug(file_path)
                file_size = fstat.st_size
                last_modified = fstat.st_mtime
                permissions = oct(fstat.st_mode)[-3:]
                status = UploadStatus.PENDING.value

                file_obj = FileModel((
                    cuid(),
                    dateSinceEpoch(),
                    file_size,
                    last_modified,
                    permissions,
                    file_path,
                    status
                ))
                file_obj.save(cursor)
                self.db_conn.commit()
                
                file_count += 1
        
        except sqlite3.Error as error:
            logger.error(f"Error running sql: {error}")
        finally:
            cursor.close()
        return file_count

    def _fetch_files(self, cursor, page_num, page_size):
        offset = page_num * page_size
        
        # paginate without using sql OFFSET https://gist.github.com/ssokolow/262503
        sql = """
        SELECT * FROM {table_name} 
        WHERE status = {status} AND 
        (id NOT IN ( SELECT id FROM {table_name} ORDER BY file_size ASC LIMIT {offset} ))
        ORDER BY file_size ASC
        LIMIT {page_size}
        """.format(
            table_name = FileModel.table_name(),
            status = UploadStatus.PENDING.value,
            offset = offset,
            page_size = page_size
        )
        results = cursor.execute(sql).fetchall()
        return results
        

    def file_iterator(self, cursor=None):
        _cursor = cursor if cursor else self.db_conn.cursor()

        page_num = 0
        page_size = 10
        try:
            while True:
                results = self._fetch_files(_cursor, page_num, page_size)
                if len(results) == 0: break

                page_num += 1
                for result in results:
                    model = FileModel(result)
                    # logger.debug(f"id = {model.id}")
                    yield model, _cursor
                    
        except sqlite3.Error as error:
            logger.error(f"Error running sql: {error}")
        finally:
            if cursor is None:
                _cursor.close

    async def async_upload_files(self):
        cursor = self.db_conn.cursor()
        max_concur = 8
        sem = asyncio.Semaphore(max_concur)

        async def task_wrapper(model, cursor):
            # logger.debug(f"task_wrapper:  model = {model.file_path}")
            try:
                model.start_upload(cursor)
            finally:
                sem.release()
                
        for model, cursor in self.file_iterator(cursor):
            await sem.acquire()
            asyncio.create_task(task_wrapper(model, cursor))

        # wait for all tasks to complete
        for i in range(max_concur):
            await sem.acquire()
        cursor.close()

    def reset_file_status(self):
        cursor = self.db_conn.cursor()
        try:
            sql = f"UPDATE {FileModel.table_name()} SET status = {UploadStatus.PENDING.value}"
            cursor.execute(sql)
            self.db_conn.commit()
        except sqlite3.Error as error:
            logger.error(f"Error running sql: {error}; {sql}")
        finally:
            cursor.close
        
        
        
