from datetime import timezone
import datetime
from pyspark.sql.functions import  col, from_utc_timestamp
import calendar

# Date/Time Operations


def filter_date(spark_df, date_column, start_date, end_date):


    """ filter a spark dataframe having a start and end date. 

    Raises:
        AttributeError: [description]

    Returns:
        spark df: filtered using start_date and end_date on date_column
    """
    if isinstance(start_date, datetime.date) & isinstance(end_date, datetime.date):
        spark_df = spark_df.filter(
            (col(date_column) >= start_date) &\
            (col(date_column) < end_date) 
        )
        return spark_df
    else:
        raise AttributeError("'date' should be date object")


def get_timestamp(format_='%y%m%d_%H%M%S_%f'):
        return datetime.datetime.utcnow().strftime(format_)

def add_months(date, months):
    months_count = date.month + months

    # Calculate the year
    year = date.year + int(months_count / 12)

    # Calculate the month
    month = (months_count % 12)
    if month == 0:
        month = 12

    # Calculate the day
    day = date.day
    last_day_of_month = calendar.monthrange(year, month)[1]
    if day > last_day_of_month:
        day = last_day_of_month

    new_date = datetime.datetime(year, month, day, date.hour, date.minute, date.second)
    return new_date


def get_date_period(start_date, end_date, period=None):
    
    """
    start_date: loaded data start date
    end_data: loaded data end date
    period: in case provided it 'start_date' will be ignored and will accept dicts specifying period to shift from end date
        e.g. periods = {'months':0, 'weeks':0, 'days':0, 'hours':0, 'minutes':0, 'seconds':0, 'microseconds':0, 'milliseconds':0}
    """
    try:
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    except:
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')

    try:
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S') if start_date else end_date
    except:
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d') if start_date else end_date   
    
    
    if period:
        months = period.pop('months', None)
        if months:
            start_date = add_months(end_date, -months)
        start_date = start_date - datetime.timedelta(**period)

    if start_date > end_date:
        raise ValueError("'start_date' can't be greater that 'end_date'")
    
    return start_date, end_date


def date_range(start_date, end_date):
    return [(start_date + datetime.timedelta(days=x)) for x in range((end_date - start_date).days+1)]



def UTC_to_LM(df, ts_col, LM):
    """
    convert UTC time to the Local timezone.
    inputs:
    - df: dataframe with the ts column
    - ts_col: column with UTC timestamp 
    - LM: string with initials of the LM
    """
    tz_dict = {
        "HU" : "Europe/Budapest",
        "DE" : "Europe/Berlin",
        "ES" : "Europe/Madrid",
        "IT" : "Europe/Rome",
        "PT" : "Europe/Lisbon",
        "RO" : "Europe/Bucharest"
    }
    return df.withColumn(ts_col,from_utc_timestamp(col(ts_col), tz_dict[LM]))


def get_UTC_time():

    now_utc = datetime.datetime.now(timezone.utc)
    now_utc = now_utc.strftime("%Y-%m-%d %H:%M:%S") + ' UTC'
    return now_utc





