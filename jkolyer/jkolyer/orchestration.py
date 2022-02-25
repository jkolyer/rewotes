import sqlite3
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

class Orchestration:
    def __init__(self):
        pass

    def disconnectDb(self):
        if self.sqliteConnection is None: return
        self.sqliteConnection.close()
        self.sqliteConnection = None
        logger.info("The SQLite connection is closed")
    
    def connectDb(self):
        try:
            self.sqliteConnection = sqlite3.connect('parallel-file-upload.db')
            cursor = self.sqliteConnection.cursor()
            logger.debug("Database created and Successfully Connected to SQLite")

            sqlite_select_Query = "select sqlite_version();"
            cursor.execute(sqlite_select_Query)
            record = cursor.fetchall()
            logger.debug(f"SQLite Database Version is: {record}")
            cursor.close()

        except sqlite3.Error as error:
            self.sqliteConnection = None
            logger.error(f"Error while connecting to sqlite: {error}")
            
