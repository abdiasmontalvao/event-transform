from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, row_number, year, month, dayofmonth
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

class EventTransform:
  """
    Event transform, split and partitioned write utility
  """

  __UNIQUE_COLUMN      = 'event_id'
  __DATETIME_COLUMN    = 'timestamp'
  __EVENT_TYPE_COLUMNS = ['domain', 'event_type']
  __SPARK_SESSION_NAME = 'pismo_event_transformation'
  __RANK_COLUMN_NAME   = '_row_number'
  __PARTITION_BY       = {'year': year, 'month': month, 'day': dayofmonth}

  __spark_session: SparkSession
  __source_path: str
  __destination_path: str
  __write_mode: str

  df_source: DataFrame

  def __init__(self, source_path: str, destination_path: str, write_mode: str = 'append'):
    self.__source_path      = source_path
    self.__destination_path = destination_path
    self.__spark_session    = SparkSession.builder.appName(self.__SPARK_SESSION_NAME).getOrCreate()
    self.__write_mode       = write_mode
    self.df_source          = self.__spark_session.read.json(self.__source_path).withColumn('timestamp', col('timestamp').cast(TimestampType()))

  def remove_duplicateds(self, data_frame: DataFrame) -> DataFrame:
    """
      Remove duplicateds rows in a DataFrame preserving the latest event occurency for each event_id
    """
    window_rank = Window.partitionBy(self.__UNIQUE_COLUMN).orderBy(col(self.__DATETIME_COLUMN).desc())
    return data_frame\
      .withColumn(self.__RANK_COLUMN_NAME, row_number().over(window_rank))\
      .filter(col(self.__RANK_COLUMN_NAME) == 1)\
      .drop(col(self.__RANK_COLUMN_NAME))

  def filter_by_event_type(self, event_type: Row, data_frame: DataFrame) -> DataFrame:
    """
      Filter events in a DataFrame by derired type
    """
    filtered_df = data_frame
    for event_type_column in self.__EVENT_TYPE_COLUMNS:
      filtered_df = filtered_df.filter(col(event_type_column) == event_type[event_type_column])
    return filtered_df

  def add_partition_columns(self, data_frame: DataFrame) -> DataFrame:
    """
      Add partition columns in a DataFrame
    """
    df_with_partition_columns = data_frame
    for key, func in self.__PARTITION_BY.items():
      df_with_partition_columns = df_with_partition_columns.withColumn(key, func(col(self.__DATETIME_COLUMN)))
    return df_with_partition_columns

  def get_distinct_event_types(self, data_frame: DataFrame) -> DataFrame:
    """
      Retrive a list of distinct event types over a DataFrame
    """
    return data_frame\
      .select(*self.__EVENT_TYPE_COLUMNS)\
      .distinct()\
      .collect()

  def split_and_write(self):
    """
      Split and write events partitioned
    """
    distinct_event_type_list = self.get_distinct_event_types(self.df_source)

    for distinct_type in distinct_event_type_list:
      current_df = self.filter_by_event_type(distinct_type, self.df_source)
      current_df = self.remove_duplicateds(current_df)
      current_df = self.add_partition_columns(current_df)

      write_path = '/'.join([self.__destination_path, *distinct_type.asDict().values()])

      current_df.write\
        .mode(self.__write_mode)\
        .partitionBy(*self.__PARTITION_BY.keys())\
        .parquet(write_path)
