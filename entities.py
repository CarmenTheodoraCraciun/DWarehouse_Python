from datetime import datetime
from cassandra.cqlengine import columns, models

# Tabele existente
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
    attributes = columns.Text()

class TimeSeriesData(models.Model):
    __table_name__ = 'time_series_data'
    asset_id = columns.Text(primary_key=True, partition_key=True)
    data_source_id = columns.Text(primary_key=True, partition_key=True)
    business_date_year = columns.Integer(primary_key=True, partition_key=True)
    business_date = columns.Date(primary_key=True, clustering_order="DESC")
    system_time = columns.DateTime(primary_key=True, clustering_order="DESC")
    data_values = columns.Map(columns.Text(), columns.Text())

# Adaugă la începutul fișierului
class Prediction(models.Model):
    __table_name__ = 'predictions'
    asset_id = columns.Text(primary_key=True)
    prediction_date = columns.Date(primary_key=True)
    prediction_time = columns.DateTime()
    predicted_close = columns.Float()
    model_name = columns.Text()