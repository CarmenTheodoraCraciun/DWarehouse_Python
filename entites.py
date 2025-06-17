from datetime import datetime
from cassandra.cqlengine import columns, models

class Asset(models.Model):
    __table_name__ = 'asset'
    id = columns.Text(primary_key=True, partition_key=True)
    system_time = columns.DateTime(primary_key=True, clustering_order="DESC")
    name = columns.Text()
    description = columns.Text()
    attributes = columns.Map(columns.Text(), columns.Text())

class DataSource(models.Model):
    __table_name__ = 'data_source'
    id = columns.Text(primary_key=True, partition_key=True)
    system_time = columns.DateTime(primary_key=True, clustering_order="DESC")
    attributes = columns.Set(columns.Text())

class TimeSeriesData(models.Model):
    __table_name__ = 'time_series_data'
    asset_id = columns.Text(primary_key=True, partition_key=True)
    data_source_id = columns.Text(primary_key=True, partition_key=True)
    business_date_year = columns.Integer(primary_key=True, partition_key=True)
    business_date = columns.Date(primary_key=True, clustering_order="DESC")
    system_time = columns.DateTime(primary_key=True, clustering_order="DESC")
    # Changed from 'values' to 'data_values'
    data_values = columns.Map(columns.Text(), columns.Text())