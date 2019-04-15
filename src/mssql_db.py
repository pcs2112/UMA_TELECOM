""" DB module used to interact with SQL server """
import sys
import pyodbc
import re

this = sys.modules[__name__]
this._database = None
this._config = None  # Reference to the DB configuration settings


def init_db(config):
    this._config = {
        'DB_DRIVER': config['DB_DRIVER'],
        'DB_SERVER': config['DB_SERVER'],
        'DB_NAME': config['DB_NAME'],
        'DB_USER': config['DB_USER'],
        'DB_PASSWORD': config['DB_PASSWORD'],
        'DB_TRUSTED_CONNECTION': config['DB_TRUSTED_CONNECTION']
    }


def get_db():
    """
    Returns the current DB connection. This function makes sure there only
    exists one connection per request.
    """
    config = getattr(this, '_config', None)
    db = getattr(this, '_database', None)
    if db is None:
        if config['DB_TRUSTED_CONNECTION']:
            cnxn_str = 'Driver=%s;Server=%s;DATABASE=%s;Trusted_Connection=yes;' % (
                config['DB_DRIVER'],
                config['DB_SERVER'],
                config['DB_NAME']
            )
            
            db = this._database = pyodbc.connect(
                cnxn_str,
                autocommit=True
            )
        else:
            db = this._database = pyodbc.connect(
                p_str=None,
                driver=config['DB_DRIVER'],
                server=config['DB_SERVER'],
                database=config['DB_NAME'],
                uid=config['DB_USER'],
                pwd=config['DB_PASSWORD'],
                autocommit=True
            )
    return db


def close(*varargs):
    """
    Closes the current DB connection if any.
    """
    db = getattr(this, '_database', None)
    if db is not None:
        db.close()
        this._database = None


def result_as_dict(schema, row):
    """
    Converts a result to a dict using the values in
    schema as the keys.

    :param schema: Row schema
    :param row: Raw result
    :type schema: list
    :type row: list
    :return:
    :rtype: dict
    """
    return dict(zip([field.lower() for field in schema], row))


def result_set_as_dicts(schema, rows):
    """
    Converts a result set rows to dicts using the values in
    schema as the keys.

    :param schema: Row schema
    :param rows: Raw result set
    :type schema: list
    :type rows: list
    :return:
    :rtype: list
    """
    return [dict(zip([field.lower() for field in schema], row)) for row in rows]


def normalize_column_name(name):
    """Function to convert column names to snake case"""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def get_column_names(result_set_description):
    """
    Gets the columns names using cursor's description.
    :param result_set_description: Cursor description
    :type result_set_description: list
    """
    column_names = []
    for i, description in enumerate(result_set_description):
        column_name = description[0]
        if not column_name:
            column_name = f'COLUMN_{i}'
        
        column_names.append(normalize_column_name(column_name))
    
    return column_names


def fetch_rows(sql, in_args=()):
    """
    Returns a list of records.

    :param sql: SQL command
    :param in_args: Parameters to bind into the query
    :type sql: str
    :type in_args: list
    :return:
    :rtype: list
    """
    cursor = get_db().cursor()
    cursor.execute(sql, in_args)
    rows = cursor.fetchall()
    column_names = get_column_names(cursor.description)
    cursor.close()
    
    if len(rows) < 1:
        return ()
    
    return result_set_as_dicts(column_names, rows)


def fetch_row(sql, in_args=()):
    """
    Returns one record.

    :param sql: SQL command
    :param in_args: Parameters to bind into the query
    :type sql: str
    :type in_args: list
    :return:
    :rtype: dict or list or None
    """
    cursor = get_db().cursor()
    cursor.execute(sql, in_args)
    row = cursor.fetchone()
    column_names = get_column_names(cursor.description)
    cursor.close()
    
    if row is None:
        return None
    
    return result_as_dict(column_names, row)


def execute_sp(sp_name, in_args, out_arg=None, as_dict=True):
    """
    Executes a stored procedure and returns the result sets.
    The 0 index in the return value contains the value for the out_arg if an out_arg is specified.

    :param sp_name: Stored procedure name
    :param in_args: Dictionary of store procedure parameters and values
    :param out_arg: Output parameter
    :param as_dict: Return result set as a Dictionary
    :type sp_name: str
    :type in_args: dict
    :type out_arg: str or None
    :type as_dict: boolean
    :return: Stored procedure result sets and out argument
    :rtype: list
    """
    sql = ''
    
    if out_arg:
        out_arg = out_arg.lower()
        sql = f'DECLARE @{out_arg} INTEGER;'
        sql += f'EXEC @{out_arg} = {sp_name} '
    else:
        sql += f'EXEC {sp_name} '
    
    in_params = []
    for key in in_args:
        sql += f'@{key} = ?, '
        in_param = in_args[key]
        if in_param is not None:
            in_param = str(in_param)
        
        in_params.append(in_param)  # Convert all in args to string
    
    sql = sql.rstrip(', ')
    sql += f';'
    
    if out_arg is not None:
        sql += f'SELECT @{out_arg} AS {out_arg};'
    
    cursor = get_db().cursor()
    cursor.execute(sql, in_params)
    
    results = []
    
    while 1:
        try:
            result_set = cursor.fetchall()
            column_names = get_column_names(cursor.description)
            if len(column_names) > 0:
                if as_dict:
                    results.append(result_set_as_dicts(column_names, result_set))
                else:
                    results.append(column_names)
                    results.append(result_set)
            else:
                results.append(result_set)
        except pyodbc.ProgrammingError:
            pass
        
        if cursor.nextset() is not True:
            break
    
    cursor.close()
    
    # Validate out arg was provided from the SP call
    if out_arg:
        get_out_arg(results, out_arg)
    
    return results


def get_sp_result_set(results, index=0, out_arg=None):
    """
    Utility to return a specified result set from results from a SP call.
    :return:
    :rtype: list or bool
    """
    results_set_count = len(results)
    if results_set_count < 1:
        return False
    
    if results_set_count == 1 and out_arg and out_arg in results[0][0]:
        return False
    
    return results[index]


def get_sp_first_result_set(results, out_arg=None):
    """
    Returns the first result set for a SP call.
    :return:
    :rtype: list or bool
    """
    results = get_sp_result_set(results, 0, out_arg)
    if not results:
        return False
    
    if len(results) < 1:
        return False
    
    return results[0]


def get_out_arg(results, out_arg):
    """ Returns the out arguments from the result sets from a SP call. """
    out_arg = out_arg.lower()
    results_count = len(results)
    if results_count < 1 or out_arg not in results[results_count - 1][0]:
        raise pyodbc.ProgrammingError(
            f'The out argument "{out_arg}" was not captured from call to the stored procedure.'
        )
    
    return results[results_count - 1][0][out_arg]
