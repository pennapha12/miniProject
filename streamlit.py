import duckdb as dd 
import polars as pl
import pandas as pd
import dlt 
from datetime import datetime
import streamlit as st 

conn = dd.connect('C:\Data warehouse\Pipeline_project\miniproject\Tranformed\dim_fact.duckdb')

date_dim = pl.read_database(
    query="SELECT * FROM Dim_and_fact_stg.dim_date",
    connection=conn)
date_dim
em_dim = pl.read_database(
    query="SELECT * FROM Dim_and_fact_stg.dim_employees",
    connection=conn)
em_dim
cus_dim = pl.read_database(
    query="SELECT * FROM Dim_and_fact_stg.dim_customers", 
    connection=conn)
cus_dim
tr_dim= pl.read_database(
    query="SELECT * FROM Dim_and_fact_stg.dim_tracks",
    connection=conn)
tr_dim
orders_fact= pl.read_database(
    query="SELECT * FROM Dim_and_fact_stg.fact_orders",
    connection=conn)
orders_fact
playlist_dim=pl.read_database(
    query="SELECT * FROM Dim_and_fact_stg.dim_playlist",
    connection=conn)
playlist_dim
dd.close()

tr_dim
playlist_dim
tr_dim.columns
tr_dim2 = tr_dim.select([
    'tr_id', 
    'tr_name', 
    'al_id', 
    'med_id', 
    'ge_id', 
    'composer', 
    'milliseconds', 
    'bytes', 
    'unit_price', 
    'al_ti', 
    'ar_id', 
    'me_name', 
    'ge_name', 
    'ar_name'])

playlist_dim2 = playlist_dim.select(
    [
        'pl_id',
        'pl_name',
        pl.col('tr_id').cast(pl.Int64).alias('tr_id')
    ]
)
tr_dim = tr_dim2.join(playlist_dim2, on='tr_id', how='inner')
tr_dim.columns
tr_dim.to_pandas()
conn = dd.connect('C:\Data warehouse\Pipeline_project\miniproject\Tranformed\dim_fact.duckdb')


pipeline=dlt.pipeline(
    pipeline_name="Dimfact",destination='duckdb',
    dataset_name="dim_fact",
)
date_dim.to_pandas()
pipeline.run(date_dim,table_name='dim_date',write_disposition='append')
