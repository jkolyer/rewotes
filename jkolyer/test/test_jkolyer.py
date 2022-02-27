import pytest
import sqlite3
from jkolyer.orchestration import Orchestration
from jkolyer.models import FileModel, UploadJobModel, BatchJobModel


class TestJkolyer(object):

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    
@pytest.fixture
def orchestration():
    orch = Orchestration()
    orch.connect_db()
    return orch
    
@pytest.fixture
def batch_job(orchestration):
    sql = BatchJobModel.new_record_sql('./samples')
    orchestration.run_sql_command(sql)
    

class TestOrchestration(TestJkolyer):
    def test_create(self, orchestration):
        assert orchestration != None

    def test_connectDb(self, orchestration):
        orchestration.connect_db()
        assert orchestration.db_conn is not None
        orchestration.disconnect_db()
        assert orchestration.db_conn is None

    def test_create_tables(self, orchestration):
        orchestration.create_tables()
        sql = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{FileModel.table_name()}'"
        result = orchestration.run_sql_query(sql)
        assert result[0][0] == FileModel.table_name()
        sql = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{UploadJobModel.table_name()}'"
        result = orchestration.run_sql_query(sql)
        assert result[0][0] == UploadJobModel.table_name()
        
class TestBatchJob(TestJkolyer):
    
    def test_create_table(self, orchestration):
        sql = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{BatchJobModel.table_name()}'"
        result = orchestration.run_sql_query(sql)
        assert result[0][0] == BatchJobModel.table_name()

    def test_create_batch(self, batch_job, orchestration):
        result = BatchJobModel.query_latest(orchestration.db_conn)
        assert result is not None
        

class TestFileModel(TestJkolyer):
    db_conn = sqlite3.connect(BatchJobModel.db_name())
    
    @classmethod
    def setup_class(cls):
        cursor = cls.db_conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {FileModel.table_name()}")
        cls.db_conn.commit()
        for sql in FileModel.create_table_sql(): cursor.execute(sql)
        cls.db_conn.commit()
        cursor.close()

    def test_create_file_records(self, orchestration):
        result = BatchJobModel.query_latest(orchestration.db_conn)
        file_count = result.generate_file_records(orchestration.db_conn)
        
        cursor = self.db_conn.cursor()
        result = cursor.execute(f"SELECT COUNT(*) FROM {FileModel.table_name()}").fetchall()
        assert result[0][0] == file_count
        cursor.close()
        

