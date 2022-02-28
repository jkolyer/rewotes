import pytest
import sqlite3
import boto3
from moto import mock_s3
import logging

for name in logging.Logger.manager.loggerDict.keys():
    if ('boto' in name) or ('urllib3' in name) or ('s3transfer' in name) or ('boto3' in name) or ('botocore' in name) or ('nose' in name):
        logging.getLogger(name).setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)                    

from jkolyer.orchestration import Orchestration
from jkolyer.models import BaseModel, BatchJobModel, FileModel, UploadStatus

class TestJkolyer(object):

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    
@pytest.fixture
def batch_job():
    sql = BatchJobModel.new_record_sql('./samples')
    BaseModel.run_sql_command(sql)
    
@pytest.fixture
def s3():
    """Pytest fixture that creates the recipes bucket in 
    the fake moto AWS account
    Yields a fake boto3 s3 client
    """
    with mock_s3():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket = FileModel.bucket_name)
        yield s3
        
@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

class TestTables(TestJkolyer):
    def test_create_tables(self):
        BaseModel.create_tables()
        sql = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{FileModel.table_name()}'"
        result = BaseModel.run_sql_query(sql)
        assert result[0][0] == FileModel.table_name()
        
        
class TestBatchJob(TestJkolyer):
    
    def test_create_table(self):
        sql = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{BatchJobModel.table_name()}'"
        result = BaseModel.run_sql_query(sql)
        assert result[0][0] == BatchJobModel.table_name()

    def test_create_batch(self, batch_job):
        result = BatchJobModel.query_latest()
        assert result is not None

        
class TestFileModel(TestJkolyer):
    
    @classmethod
    def setup_class(cls):
        FileModel.bootstrap_table()

    def test_create_file_records(self):
        batch = BatchJobModel.query_latest()
        file_count = batch.generate_file_records()
        
        cursor = BaseModel.db_conn.cursor()
        result = cursor.execute(f"SELECT COUNT(*) FROM {FileModel.table_name()}").fetchall()
        assert result[0][0] == file_count

        # ensure no duplicates are created
        new_file_count = batch.generate_file_records()
        result = cursor.execute(f"SELECT COUNT(*) FROM {FileModel.table_name()}").fetchall()
        assert result[0][0] == file_count
        
        cursor.close()

    @mock_s3
    def test_file_upload(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=FileModel.bucket_name)
                
        model = FileModel.fetch_record(UploadStatus.PENDING.value)
        assert model is not None
        cursor = FileModel.db_conn.cursor()
        model.start_upload(cursor)
        cursor.close()

        assert model.status == UploadStatus.COMPLETED.value
        model2 = FileModel.fetch_record(UploadStatus.COMPLETED.value)
        assert model2 is not None
        assert model2.id == model.id
        
        file_contents = model.get_uploaded_file()
        assert file_contents is not None
        
    def xtest_batch_uploads(self):
        batch = BatchJobModel.query_latest()
        batch.upload_files()

        
