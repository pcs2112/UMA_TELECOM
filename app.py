import sys
import os
import importlib
import logging
from logging.handlers import RotatingFileHandler
from src.utils import log
from src.mssql_db import init_db, close as close_db
from src.config import get_config


config = get_config()
arg_count = len(sys.argv)
logger_name = 'AppLogger'


def init_components():
    # Init logger
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    handler = RotatingFileHandler(os.path.join(config['LOGS_DIR'], 'log.txt'), maxBytes=100000, backupCount=5)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Init DB
    db_config = {
        'DB_DRIVER': config['DB_DRIVER'],
        'DB_SERVER': config['DB_SERVER'],
        'DB_NAME': config['DB_NAME'],
        'DB_USER': config['DB_USER'],
        'DB_PASSWORD': config['DB_PASSWORD'],
        'DB_TRUSTED_CONNECTION': config['DB_TRUSTED_CONNECTION']
    }

    init_db(db_config)


def _exit(_exit=True):
    close_db()
    if _exit:
        exit()


def main(args):
    if len(args) == 1:
        print("Please enter a command.")
        _exit()
    
    cmd = None
    
    try:
        module = importlib.import_module(f'src.commands.{args[1]}')
        cmd = getattr(module, args[1])
    except AttributeError:
        print(f"\"{args[1]}\" is an invalid command.")
        _exit()
    except ModuleNotFoundError:
        print(f"\"{args[1]}\" is an invalid command.")
        _exit()

    module_exit = getattr(module, 'exit', None)
    module_error_exit = getattr(module, 'error_exit', None)

    init_components()
    
    try:
        if len(args) > 2:
            cmd(*args[2:len(args)])
        else:
            cmd()

        if module_exit:
            module_exit()

    except KeyboardInterrupt as e:
        logger = logging.getLogger(logger_name)
        logger.exception('KeyboardInterrupt: %s', e)
        if module_error_exit:
            module_error_exit()
        pass
    except Exception as e:
        logger = logging.getLogger(logger_name)
        logger.exception('Exception: %s', e)
        if module_error_exit:
            module_error_exit()

        _exit(False)
        raise e
    
    _exit()


if __name__ == '__main__':
    main(sys.argv)
