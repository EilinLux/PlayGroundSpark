import datetime
from pyspark.sql.functions import  col, concat_ws
from pyspark.sql.types import  StructField, StructType,  StringType
from utils.utils_lists import make_list_if_not

import subprocess
import inspect

from google.cloud.exceptions import NotFound


# String Operations

def strip_css(s, delimiter=','):
    """
    Trimming a comma separated string
    """
    return f"{delimiter}".join([item.strip() for item in s.split(delimiter)])



def if_tbl_exists(client, table_ref):
    """ check if a table exists. """
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False

def date_filter_paths(paths, start_date, end_date):
    """
        Description:
            Takes a list of paths that should end with this pattern '/year=*/month=*/day=*'
            and filters these path to include only the ones between start and end date
        Return:
            list of paths that exist between start and end date
    """
    splitted = [i.split('=')[-3:] for i in paths]
    splitted = [[y.split('/')[0] for y in i] for i in splitted]
    dates = [datetime.datetime.strptime('-'.join(i), '%Y-%m-%d') for i in splitted]
    in_between_dates = []
    for i,d in enumerate(dates):
        if d >= start_date and d < end_date:
            in_between_dates.append(paths[i])
    return in_between_dates

def get_structType(columns):
    """
        Creates StructType object with StringType for passed columns
    """
    return StructType(list(map(lambda column: StructField(column, StringType(), True), columns)))




# Dynamic Programming

def execute(statement, namespace):
    code = compile(statement, '<string>', 'exec')
    exec(code, namespace)


def get_source_code(func):
    lines = inspect.getsource(func)
    return lines


def get_name(var):
    """
    Retrieves variable name
    """
    callers_local_vars = inspect.currentframe().f_back.f_locals.items()
    return [var_name for var_name, var_val in callers_local_vars if var_val is var]


def exe_cmd(cmd):
    "Takes a terminal command excutes it and return it stdout in a list of line generated"
    return subprocess.run(cmd.split(" "), stdout=subprocess.PIPE).stdout.decode('utf-8').splitlines()


# Functional Programming
def apply_multiple(apply_to, funcs:list):
    """
    Takes a list on object and a list of functions and applies functions in order
    """
    funcs = make_list_if_not(funcs)
    for func in funcs:
        apply_to = func(apply_to)
    return apply_to

# pyspark dataframe operations


def shape(spark_df):
    '''
    get shape of pyspark dataframe
    '''
    return (spark_df.cache().count(), len(spark_df.columns))


def get_latest_snapshot(df, keys):
    '''
    1-order dataframe rows descending based on date
    2-drop duplicated old rows (with same keys, different dates) to keep only the latest snapshot on cell level
    '''
    date_cols =['year','month','day']
    df = df.withColumn('table_date',concat_ws('-',*date_cols).cast('date'))
    df = df.sort(df.table_date.desc()).coalesce(1).drop_duplicates(keys)

    df = df.drop('table_date')
    return df

