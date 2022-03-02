import pathlib
import argparse
import pathlib
import sys
import logging

import asyncio
from jkolyer.models.batch_model import BatchJobModel, parallel_upload_files
from jkolyer.models.file_model import FileModel

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
        
def parse_cmd_line_arguments():
    """Describe
    :param name: describe
    :param name: describe
    :return: type describe
    """
    parser = argparse.ArgumentParser(
        description="PFU: Parallel File Upload",
        epilog="Thanks for using the service!",
    )
    parser.add_argument(
        "--parallel",
        action='store_true',
        help="Runs the uploads in multiple processes (up to CPU count), default is concurrent.",
    )
    parser.add_argument(
        "--concurrent",
        action='store_true',
        help="Runs the uploads in a single process using asyncio (default).",
    )

    parser.add_argument(
        "--root_dir",
        metavar="ROOT_DIR",
        action="store",
        required=True,
        help="Directory to load files for upload",
    )
    return parser.parse_args()

def perform_file_upload(parallel=False):
    logger.info(f"initializing database")
    FileModel.create_tables()
    BatchJobModel.create_tables()
    sql = BatchJobModel.new_record_sql(root_dir)
    BatchJobModel.run_sql_command(sql)

    batch = BatchJobModel.query_latest()
    batch.generate_file_records()

    breakpoint()
    logger.info(f"performing upload")
    if parallel:
        parallel_upload_files(batch, True)
    else:
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(batch.async_upload_files())
        finally:
            loop.close()

if __name__ == '__main__':
    args = parse_cmd_line_arguments()
    root_dir = pathlib.Path(args.root_dir)
    if not root_dir.is_dir():
        print("The specified root directory doesn't exist")
        sys.exit()
        
    concurrent = args.concurrent
    parallel = args.parallel
    if concurrent and parallel:
        parallel = False

    perform_file_upload(parallel)
    

