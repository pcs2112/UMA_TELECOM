import pyodbc
import requests
from src.mssql_db import execute_sp, get_sp_result_set, get_sp_first_result_set
from src.utils import execute_sp_with_required_in_args, get_out_arg


def execute_scheduled_tasks_sp(*args, out_arg='sp_status_code', only_first=False):
    """
    Helper function to execute the MWH.MANAGE_SCHEDULE_TASK_JOBS stored procedure.
    :return: Stored procedure result sets and out argument
    :rtype: list
    """
    results = execute_sp_with_required_in_args(*args, sp_args_length=11, out_arg=out_arg)
    status_code = get_out_arg(results, out_arg)

    if status_code > -1:
        raise pyodbc.ProgrammingError(f'Stored Procedure call to "{args[0]}" failed.', status_code)

    result = get_sp_result_set(results, 0, out_arg)
    if not result:
        return None if only_first else []

    return result if not only_first else result[0]


def get_next():
    return execute_scheduled_tasks_sp(
        'MWH.MANAGE_SCHEDULE_TASK_JOBS',
        'GET_NEXT_SCHEDULE_TASK',
        only_first=True
    )
