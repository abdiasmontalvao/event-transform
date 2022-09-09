import pathlib
from pyspark.sql import Row, SparkSession
from pismo.transform.event import EventTransform
from datetime import datetime

class TestEventTransform:
  
  __SOURCE_PATH = f'{pathlib.Path().resolve()}/test/ingest_mock/'
  __DESTINATION_PATH = f'{pathlib.Path().resolve()}/test/temp_output_bucket/'

  def test_if_is_filtering_by_type_correctly(self):
    event_transform = EventTransform(self.__SOURCE_PATH + 'mock-3-type-12-events.json', self.__DESTINATION_PATH)
    expected_quant = 4
    distinct_type_row = Row(domain='purchase', event_type='creation')
    filtered_df = event_transform.filter_by_event_type(distinct_type_row, event_transform.df_source)
    filtered_df_types = filtered_df.select('domain', 'event_type').collect()

    for filtered_df_type_row in filtered_df_types:
      assert filtered_df_type_row == distinct_type_row

    assert filtered_df.count() == expected_quant

  def test_if_get_distinct_event_types_works(self):
    event_transform = EventTransform(self.__SOURCE_PATH + 'mock-3-type-12-events.json', self.__DESTINATION_PATH)
    expected_distinct_types = [
      Row(domain='account', event_type='creation'),
      Row(domain='purchase', event_type='creation'),
      Row(domain='offer', event_type='creation')
    ]

    distinct_types_results = event_transform.get_distinct_event_types(event_transform.df_source)

    for distinct_type_result in distinct_types_results:
      assert distinct_type_result in expected_distinct_types

    assert len(expected_distinct_types) == len(distinct_types_results)

  def test_if_remove_duplicateds_works_and_preserves_the_latest_event(self):
    event_transform = EventTransform(self.__SOURCE_PATH + 'mock-repeateds.json', self.__DESTINATION_PATH)
    expected_unique_quant = 5
    expected_rows = [
      Row(domain='account', event_type='creation', event_id='6d7d0c85-94a2-4bf0-a570-72774e4fbc0d', timestamp=datetime.fromisoformat('2021-01-01T00:00:29')),
      Row(domain='account', event_type='creation', event_id='96579a99-7deb-4691-95d3-390178936615', timestamp=datetime.fromisoformat('2021-01-01T00:01:01')),
      Row(domain='account', event_type='creation', event_id='7b2480cc-50ed-4a57-8bd2-ace0d78da198', timestamp=datetime.fromisoformat('2021-01-01T00:01:37')),
      Row(domain='account', event_type='creation', event_id='abd543ca-ace2-490e-8757-214c69da76ab', timestamp=datetime.fromisoformat('2021-01-01 00:04:15')),
      Row(domain='account', event_type='creation', event_id='5d743c5e-a967-4118-b4f6-e9963affb3fe', timestamp=datetime.fromisoformat('2021-01-01T00:05:43'))
    ]

    deduplicated_df = event_transform.remove_duplicateds(event_transform.df_source)
    deduplicated_rows = deduplicated_df.select('domain', 'event_type', 'event_id', 'timestamp').collect()

    for row in deduplicated_rows:
      assert row in expected_rows

    assert deduplicated_df.count() == expected_unique_quant

  def test_if_remove_duplicateds_works_and_preserves_the_latest_event_2(self):
    event_transform = EventTransform(self.__SOURCE_PATH + 'mock-repeateds-2.json', self.__DESTINATION_PATH)
    expected_unique_quant = 4
    expected_rows = [
      Row(domain='offer', event_type='creation', event_id='63987632-f9f9-4de6-8256-0bdabbcc3f3b', timestamp=datetime.fromisoformat('2021-01-01T00:06:28')),
      Row(domain='offer', event_type='creation', event_id='0fd4c241-c32d-4578-b10e-fd94e5640363', timestamp=datetime.fromisoformat('2021-01-01 00:07:00')),
      Row(domain='offer', event_type='creation', event_id='4496e823-f525-4bd1-9390-2fa8b675f6f1', timestamp=datetime.fromisoformat('2021-01-01T00:07:13')),
      Row(domain='offer', event_type='creation', event_id='1723930d-5a7f-4cb6-8021-07214dc099e8', timestamp=datetime.fromisoformat('2021-01-01T00:09:51'))
    ]

    deduplicated_df = event_transform.remove_duplicateds(event_transform.df_source)
    deduplicated_rows = deduplicated_df.select('domain', 'event_type', 'event_id', 'timestamp').collect()

    for row in deduplicated_rows:
      assert row in expected_rows

    assert deduplicated_df.count() == expected_unique_quant

  def test_if_split_and_write_works_correctly(self):
    event_transform = EventTransform(self.__SOURCE_PATH + 'mock-repeateds-2-types.json', self.__DESTINATION_PATH, 'overwrite')
    expected_rows_account_creation_day_01 = [
      Row(domain='account', event_type='creation', event_id='96579a99-7deb-4691-95d3-390178936615', timestamp=datetime.fromisoformat('2021-01-01T00:01:01')),
      Row(domain='account', event_type='creation', event_id='7b2480cc-50ed-4a57-8bd2-ace0d78da198', timestamp=datetime.fromisoformat('2021-01-01T00:01:37')),
      Row(domain='account', event_type='creation', event_id='abd543ca-ace2-490e-8757-214c69da76ab', timestamp=datetime.fromisoformat('2021-01-01 00:04:15'))
    ]
    expected_rows_account_creation_day_02 = [
      Row(domain='account', event_type='creation', event_id='6d7d0c85-94a2-4bf0-a570-72774e4fbc0d', timestamp=datetime.fromisoformat('2021-01-02T00:00:29')),
      Row(domain='account', event_type='creation', event_id='5d743c5e-a967-4118-b4f6-e9963affb3fe', timestamp=datetime.fromisoformat('2021-01-02T00:05:43'))
    ]
    expected_rows_offer_creation_day_01   = [
      Row(domain='offer', event_type='creation', event_id='63987632-f9f9-4de6-8256-0bdabbcc3f3b', timestamp=datetime.fromisoformat('2021-01-01T00:06:28')),
      Row(domain='offer', event_type='creation', event_id='0fd4c241-c32d-4578-b10e-fd94e5640363', timestamp=datetime.fromisoformat('2021-01-01 00:07:00')),
      Row(domain='offer', event_type='creation', event_id='1723930d-5a7f-4cb6-8021-07214dc099e8', timestamp=datetime.fromisoformat('2021-01-01T00:09:51'))
    ]
    expected_rows_offer_creation_day_02   = [
      Row(domain='offer', event_type='creation', event_id='4496e823-f525-4bd1-9390-2fa8b675f6f1', timestamp=datetime.fromisoformat('2021-01-02T00:07:13'))
    ]

    event_transform.split_and_write()

    spark = SparkSession.builder.appName('read_test').getOrCreate()
    select_columns = ['domain', 'event_type', 'event_id', 'timestamp']

    account_creation_day_01 = spark.read.parquet(self.__DESTINATION_PATH + 'account/creation/year=2021/month=1/day=1').select(select_columns).collect()
    account_creation_day_02 = spark.read.parquet(self.__DESTINATION_PATH + 'account/creation/year=2021/month=1/day=2').select(select_columns).collect()
    offer_creation_day_01   = spark.read.parquet(self.__DESTINATION_PATH + 'offer/creation/year=2021/month=1/day=1').select(select_columns).collect()
    offer_creation_day_02   = spark.read.parquet(self.__DESTINATION_PATH + 'offer/creation/year=2021/month=1/day=2').select(select_columns).collect()

    for row in account_creation_day_01:
      assert row in expected_rows_account_creation_day_01
    assert len(account_creation_day_01) == len(expected_rows_account_creation_day_01)

    for row in account_creation_day_02:
      assert row in expected_rows_account_creation_day_02
    assert len(account_creation_day_02) == len(expected_rows_account_creation_day_02)

    for row in offer_creation_day_01:
      assert row in expected_rows_offer_creation_day_01
    assert len(offer_creation_day_01) == len(expected_rows_offer_creation_day_01)

    for row in offer_creation_day_02:
      assert row in expected_rows_offer_creation_day_02
    assert len(offer_creation_day_02) == len(expected_rows_offer_creation_day_02)
