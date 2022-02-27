"""
Tests for `jkolyer` module.
"""
import pytest
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

    def test_create_file_records(self, orchestration):
        result = BatchJobModel.query_latest(orchestration.db_conn)
        result.generate_file_records()
        

