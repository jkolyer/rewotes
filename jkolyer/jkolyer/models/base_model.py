""" Base model class for 

SUMMARY

Args:
    name (type): describe
    name (type): describe

Returns:
    type: describe
"""
from abc import ABC, abstractmethod
import sqlite3
from datetime import datetime
from enum import Enum
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def dateSinceEpoch(mydate=datetime.now()):
    """Describe
    :param name: describe
    :param name: describe
    :return: type describe
    """
    result = (mydate - datetime(1970, 1, 1)).total_seconds()
    return result

class UploadStatus(Enum):
    """Describe
    :param name: describe
    :param name: describe
    :return: type describe
    """
    PENDING = 1
    IN_PROGRESS = 2
    COMPLETED = 3
    FAILED = 4

class BaseModel(ABC):
    """Describe
    :param name: describe
    :param name: describe
    :return: type describe
    """
    db_name = 'parallel-file-upload.db'
    db_conn = sqlite3.connect(db_name)
    bucket_name = 'rewotes-pfu-bucket'
    
    @classmethod
    @abstractmethod
    def table_name(cls):
        pass

    @classmethod
    @abstractmethod
    def create_table_sql(cls):
        pass
    
    @classmethod
    def create_tables(cls):
        """Describe
        :param name: describe
        :param name: describe
        :return: type describe
        """
        cursor = cls.db_conn.cursor()
        try:
            sqls = cls.create_table_sql()
            for sql in sqls: cursor.execute(sql)
            cls.db_conn.commit()
        except sqlite3.Error as error:
            logger.error(f"Error running sql: {error}; ${sql}")
        finally:
            cursor.close()

    @classmethod
    def run_sql_query(cls, sql):
        """Describe
        :param name: describe
        :param name: describe
        :return: type describe
        """
        cursor = cls.db_conn.cursor()
        try:
            return cursor.execute(sql).fetchall()
        except sqlite3.Error as error:
            logger.error(f"Error running sql: {error}; ${sql}")
        finally:
            cursor.close()

    @classmethod
    def run_sql_command(cls, sql):
        """Describe
        :param name: describe
        :param name: describe
        :return: type describe
        """
        cursor = cls.db_conn.cursor()
        try:
            cursor.execute(sql)
            cls.db_conn.commit()
        except sqlite3.Error as error:
            logger.error(f"Error running sql: {error}; ${sql}")
        finally:
            cursor.close()

    def __init__(self):
        pass
