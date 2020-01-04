import sys
import os
import importlib
import logging
from src.utils import create_logger, log
from src.mssql_db import close as close_db
from src.config import get_config


config = get_config()
arg_count = len(sys.argv)


def _exit(_exit=True):
    close_db()
    if _exit:
        exit()


def main(args):
    logger = create_logger(os.path.join(config['LOGS_DIR'], 'log.txt'), 'AppLogger')

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
    
    try:
        if len(args) > 2:
            cmd(*args[2:len(args)])
        else:
            cmd()

        if module_exit:
            module_exit()

    except KeyboardInterrupt as e:
        logger.exception('KeyboardInterrupt: %s', e)
        if module_error_exit:
            module_error_exit()
        pass
    except Exception as e:
        logger.exception('Exception: %s', e)
        if module_error_exit:
            module_error_exit()

        _exit(False)
        raise e
    
    _exit()


if __name__ == '__main__':
    main(sys.argv)
