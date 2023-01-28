from pyspark.sql.functions import col
import datetime

def filter_date(spark_df, date_column, start_date, end_date):
    """ filter a spark dataframe having a start and end date. """
    if isinstance(start_date, datetime.date) & isinstance(end_date, datetime.date):
        spark_df = spark_df.filter(
            (col(date_column) >= start_date) &\
            (col(date_column) < end_date) 
        )
        return spark_df
    else:
        raise AttributeError("'date' should be date object")


