import pathlib
import argparse
import pathlib
import sys

import bootstrap

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

if __name__ == '__main__':
    args = parse_cmd_line_arguments()
    root_dir = pathlib.Path(args.root_dir)
    if not root_dir.is_dir():
        print("The specified root directory doesn't exist")
        sys.exit()
        
    concurrent = args.concurrent
    parallel = args.parallel
    if current and parallel:
        parallel = False

    bootstrap.perform_file_upload(parallel)
    
