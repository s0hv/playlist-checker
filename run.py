import json
import logging
import os
import filelock
from filelock import Timeout
from src.app import PlaylistChecker

dir_path = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger('debug')
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(filename=os.path.join(dir_path, 'debug.log'), encoding='utf-8', mode='a')
handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
logger.addHandler(handler)

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

    with open(os.path.join(dir_path, 'config.json'), 'r', encoding='utf-8') as f:
        config = json.load(f)

    checker = PlaylistChecker(config)

    checker.check_all()
