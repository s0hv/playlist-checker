import argparse
import logging
import os
import sys

dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(dir_path)

import filelock
from dotenv import load_dotenv
from filelock import Timeout

load_dotenv()

from src.app import PlaylistChecker

logs_dir = os.getenv('LOGS_DIR', dir_path)
logger = logging.getLogger('debug')
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(filename=os.path.join(logs_dir, 'debug.log'), encoding='utf-8', mode='a')
handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:[%(module)s] %(message)s'))
logger.addHandler(handler)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:[%(module)s] %(message)s'))
logger.addHandler(handler)

parser = argparse.ArgumentParser()
parser.add_argument('-s', '--specific', nargs='+')
parser.add_argument('-nd', '--no-downloads', action='store_true', default=False)

if __name__ == '__main__':
    lock = os.path.join(dir_path, '.lock')

    try:
        if os.path.exists(lock):
            os.remove(lock)
    except OSError:
        logger.info('Instance of the program is already running. Aborting')
        exit()

    with open(lock, 'w'):
        pass

    lockfile = filelock.FileLock(lock)
    try:
        lockfile.acquire(timeout=1)
    except Timeout:
        logger.info('Could not acquire lock file. Aborting')
        exit()

    checker = PlaylistChecker()
    args = sys.argv
    args = parser.parse_args(args[1:])

    try:
        checker.check_all(args.specific, no_download=args.no_downloads)
    except Exception:
        logger.exception('Playlist check failed')
        raise
