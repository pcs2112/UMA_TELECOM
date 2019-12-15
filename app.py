import sys
import importlib
from src.mssql_db import close as close_db

arg_count = len(sys.argv)


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
    
    try:
        if len(args) > 2:
            cmd(*args[2:len(args)])
        else:
            cmd()

        if module_exit:
            module_exit()

    except KeyboardInterrupt:
        if module_error_exit:
            module_error_exit()
        pass
    except Exception as e:
        if module_error_exit:
            module_error_exit()

        _exit(False)
        raise e
    
    _exit()


if __name__ == '__main__':
    main(sys.argv)
