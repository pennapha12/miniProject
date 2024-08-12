import duckdb as dd
import polars as pl
import pandas as pd
import dlt
import sqlite3
from datetime import datetime

#path
db_path='chinook.db'
db_path

# Connect to the SQLite database
conn = sqlite3.connect(db_path)

#show all Table 
def show_tables(connection):
  """Shows all tables in the connected database."""
  cursor = connection.cursor()
  cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
  tables = cursor.fetchall()
  for table in tables:
      print(table[0])
      
show_tables(conn)

#Extract
albums = pl.read_database(
   query= "SELECT * FROM albums",
   connection=conn )
sqlite_sequence = pl.read_database(
   query= "SELECT * FROM sqlite_sequence",
   connection=conn )
sqlite_sequence
artists = pl.read_database(
   query= "SELECT * FROM artists",
   connection=conn )
artists
customers = pl.read_database(
   query= "SELECT * FROM customers",
   connection=conn )
customers
employees = pl.read_database(
   query= "SELECT * FROM employees",
   connection=conn )
employees
genres = pl.read_database(
   query= "SELECT * FROM genres",
   connection=conn )
genres
invoice_items = pl.read_database(
   query= "SELECT * FROM invoice_items",
   connection=conn )
invoice_items
invoices = pl.read_database(
   query= "SELECT * FROM invoices",
   connection=conn )
invoices
media_types = pl.read_database(
   query= "SELECT * FROM media_types",
   connection=conn )
media_types
playlist_track = pl.read_database(
   query= "SELECT * FROM playlist_track",
   connection=conn )
playlist_track
playlists = pl.read_database(
   query= "SELECT * FROM playlists",
   connection=conn )
playlists
tracks = pl.read_database(
   query= "SELECT * FROM tracks",
   connection=conn )
tracks
sqlite_stat1 = pl.read_database(
    query= "SELECT * FROM sqlite_stat1",
    connection=conn )
sqlite_stat1
conn.close()

#create Function
def rename_col(df, colname_dict):
        for old_name, new_name in colname_dict.items():
            df = df.rename({old_name: new_name})
        return df

def add_timestamp(df, colname):
        current_timestamp = datetime.now()
        df_n = df.with_columns(pl.lit(current_timestamp).alias(colname))
        return df_n

def unique(df, colname):
        return df.unique(subset=[colname])

def sort(df, colname):
        return df.sort(by=colname)

def rename_col(df, colname_dict):
        for old_name, new_name in colname_dict.items():
            df = df.rename({old_name: new_name})
        return df

def exclude(df, colname):
        return df.select(pl.col('*').exclude(colname))


def convert_str_to_datetime(df, column_name, datetime_format='%Y-%m-%d %H:%M:%S'):
    return df.with_columns(
        pl.col(column_name).str.strptime(pl.Datetime, format=datetime_format).alias('Date_time'))


def group_by_and_sum(df: pl.DataFrame, groupby_col: str, sum_col: str) -> pl.DataFrame:
    return df.group_by(groupby_col).agg(pl.col(sum_col).sum().alias(f"{sum_col}_sum"))

def group_by_and_count(df: pl.DataFrame, groupby_col: str) -> pl.DataFrame:
    return df.group_by(groupby_col).agg(pl.count().alias('count'))


def split_month_year(df, datetime_col):
    # Ensure the datetime_col is in datetime format
    df = df.with_columns(
        pl.col(datetime_col).cast(pl.Datetime).alias(datetime_col))
    
    # Add new columns for month and year
    return df.with_columns([
        pl.col(datetime_col).dt.month().alias(f"{datetime_col}_Month"),
        pl.col(datetime_col).dt.year().alias(f"{datetime_col}_Year")])


#Tranform albums
albums
Al_f=(albums.pipe(rename_col,{"AlbumId":"Al_Id"})
      .pipe(rename_col,{"Title":"Al_Ti"})
      .pipe(rename_col,{"ArtistId":"Ar_Id"})
).to_pandas()
Al_f
 
#Tranform artists
artists
Ar_f=(
    artists.pipe(rename_col,{"ArtistId":"Ar_Id"})
    .pipe(rename_col,{"Name":"Ar_Name"})
).to_pandas()
Ar_f
 
#Tranform Customers 
customers.columns
Cus_f = (
    customers.pipe(rename_col, {"CustomerId": "Cus_Id"})  # เปลี่ยนชื่อคอลัมน์
      .pipe(rename_col, {"LastName": "Cus_L"})
      .pipe(rename_col, {"FirstName": "Cus_F"})
      .pipe(rename_col, {"Company": "Cus_Com"})
      .pipe(rename_col, {"Address": "Cus_Ad"})
      .pipe(rename_col, {"City": "Cus_City"})
      .pipe(rename_col, {"State": "Cus_State"})
      .pipe(rename_col, {"Country": "Cus_Contry"})
      .pipe(rename_col, {"PostalCode": "Cus_Pos"})
      .pipe(rename_col, {"Phone": "Cus_Phone"})
      .pipe(rename_col, {"Fax": "Cus_Fax"})
      .pipe(rename_col, {"Email": "Cus_Email"})
      .pipe(rename_col, {"SupportRepId": "Em_Id"})
).to_pandas()
Cus_f

# Transformation process using .pipe
Em_f = (
    employees.pipe(rename_col, {"EmployeeId": "Em_Id"})  # เปลี่ยนชื่อคอลัมน์
      .pipe(rename_col, {"LastName": "Em_L"})
      .pipe(rename_col, {"FirstName": "Em_F"})
      .pipe(rename_col, {"Title": "Em_Ti"})
      .pipe(rename_col, {"BirthDate": "Em_Birth"})
      .pipe(rename_col, {"HireDate": "Em_Hire"})
      .pipe(rename_col, {"Address": "Em_Ad"})
      .pipe(rename_col, {"City": "Em_City"})
      .pipe(rename_col, {"State": "Em_State"})
      .pipe(rename_col, {"Country": "Em_Contry"})
      .pipe(rename_col, {"PostalCode": "Em_Pos"})
      .pipe(rename_col, {"Phone": "Em_Phone"})
      .pipe(rename_col, {"Fax": "Em_Fax"})
      .pipe(rename_col, {"Email": "Em_Email"})
).to_pandas()
Em_f

#Tranform genres
genres
Ge_f=(
      genres.pipe(rename_col,{"GenreId":"Ge_Id"})
      .pipe(rename_col,{"Name":"Ge_Name"})
).to_pandas()
Ge_f

#Tranform invoices
invoices.columns
Inv_f=(
    invoices.pipe(rename_col, {"InvoiceId": "Inv_Id"})
    .pipe(rename_col, {"CustomerId": "Cus_Id"})
    .pipe(rename_col, {"InvoiceDate": "Inv_D"})
    .pipe(rename_col, {"BillingAddress": "B_Ad"})
    .pipe(rename_col, {"BillingCity": "B_Ci"})
    .pipe(rename_col, {"BillingState": "B_St"})
    .pipe(rename_col, {"BillingCountry": "B_Coun"})
    .pipe(rename_col, {"BillingPostalCode": "B_Pos"})
    .pipe(convert_str_to_datetime, 'Inv_D')  # Convert to datetime format
    .pipe(split_month_year,'Date_time')          # Split into month and year
   ).to_pandas()
Inv_f

#Tranform invoices_items
invoice_items
Invt_f=(
      invoice_items.pipe(rename_col,{"InvoiceLineId":"Invt_Id"})
      .pipe(rename_col,{"InvoiceId":"Inv_Id"})
      .pipe(rename_col,{"TrackId":"Tr_Id"})
).to_pandas()
Invt_f

#Tranform media_types
media_types
Med_f=(
      media_types.pipe(rename_col,{"MediaTypeId":"Med_Id"})
      .pipe(rename_col,{"Name":"Me_Name"})
).to_pandas()
Med_f

#Tranform playlists
playlists
Pl_f=(
      playlists.pipe(rename_col,{"PlaylistId":"Pl_Id"})
      .pipe(rename_col,{"Name":"Pl_Name"})
).to_pandas()
Pl_f

#Tranform playlist_tracks
playlist_track
PlT_f=(
      playlist_track.pipe(rename_col,{"PlaylistId":"Pl_Id"})
      .pipe(rename_col,{"TrackId":"Tr_Id"})
).to_pandas()
PlT_f

#Tranform Track
tracks
tracks.columns
Tr_f=(
      tracks.pipe(rename_col,{"TrackId":"Tr_Id"})
      .pipe(rename_col,{"Name":"Tr_Name"})
      .pipe(rename_col,{"AlbumId":"Al_Id"})
      .pipe(rename_col,{"MediaTypeId":"Med_Id"})
      .pipe(rename_col,{"GenreId":"Ge_Id"})   
).to_pandas()
Tr_f

pipeline=dlt.pipeline(
    pipeline_name="Operational",destination='duckdb',
    dataset_name="Project_stg"
)
pipeline.run(Al_f,table_name="stg_Albums",write_disposition='append')
pipeline.run(Ar_f,table_name="stg_Artists",write_disposition='append')
pipeline.run(Cus_f,table_name="stg_customers",write_disposition='append')
pipeline.run(Em_f,table_name="stg_employee",write_disposition='append')
pipeline.run(Ge_f,table_name="stg_Genres",write_disposition='append')
pipeline.run(Inv_f,table_name="stg_Invoices",write_disposition='append')
pipeline.run(Invt_f,table_name="stg_Invoices_items",write_disposition='append')
pipeline.run(Med_f,table_name="stg_Media_Types",write_disposition='append')
pipeline.run(Pl_f,table_name="stg_Playlist",write_disposition='append')
pipeline.run(PlT_f,table_name="stg_Playlist_Tracks",write_disposition='append')
pipeline.run(Tr_f,table_name="stg_Tracks",write_disposition='append')
 
import duckdb as dd
import polars as pl

# สร้างการเชื่อมต่อกับฐานข้อมูล DuckDB
db_path_op = 'operational.duckdb'
conn = dd.connect(db_path_op)

# ดึงข้อมูลจากตารางแต่ละตาราง
Al_f = pl.read_database(query="SELECT * FROM project_stg.stg_albums", connection=conn)
Ar_f = pl.read_database(query="SELECT * FROM project_stg.stg_artists", connection=conn)
Cus_f = pl.read_database(query="SELECT * FROM project_stg.stg_customers", connection=conn)
Em_f = pl.read_database(query="SELECT * FROM project_stg.stg_employee", connection=conn)
Ge_f = pl.read_database(query="SELECT * FROM project_stg.stg_genres", connection=conn)
Inv_f = pl.read_database(query="SELECT * FROM project_stg.stg_invoices", connection=conn)
Invt_f = pl.read_database(query="SELECT * FROM project_stg.stg_invoices_items", connection=conn)
Med_f = pl.read_database(query="SELECT * FROM project_stg.stg_media_Types", connection=conn)
Pl_f = pl.read_database(query="SELECT * FROM project_stg.stg_playlist", connection=conn)
PlT_f = pl.read_database(query="SELECT * FROM project_stg.stg_playlist_Tracks", connection=conn)
Tr_f = pl.read_database(query="SELECT * FROM project_stg.stg_tracks", connection=conn)

# แสดงตัวอย่างข้อมูลจากตารางหนึ่ง
print(Al_f.head())  # แสดง 5 แถวแรกของตาราง stg_Albums

# ปิดการเชื่อมต่อกับฐานข้อมูล DuckDB
conn.close()


#Dimensions 
Tr_f.columns
Al_f.columns
Med_f.columns
Ge_f.columns
Ar_f.columns
Pl_f.head()
PlT_f.head()

#dim_tracks
Tr_select=Tr_f.select(['tr_id', 'tr_name', 'al_id', 'med_id', 'ge_id', 'composer', 'milliseconds', 'bytes', 'unit_price'])
Al_select=Al_f.select(['al_id', 'al_ti', 'ar_id'])
Med_select=Med_f.select(['med_id', 'me_name'])
Ge_select=Ge_f.select(['ge_id', 'ge_name'])
Ar_select=Ar_f.select(['ar_id', 'ar_name'])

dim_tracks=(
      Tr_select.join(Al_select,on='al_id',how='left')\
      .join(Med_select,on='med_id',how='left')\
      .join(Ge_select,on='ge_id',how='left')\
      .join(Ar_select,on='ar_id',how='left')
).to_pandas()
dim_tracks


#check ว่า adress คือที่อยู่อันเดียวกันมั้ยระหว่าง invoice และ cus 
Inv_f.columns
Cus_f.columns
Inv_f_select=Inv_f.select(['cus_id','b_ci'])
Cus_f_select=Cus_f.select(['cus_id','cus_city'])
ch=(
      Inv_f_select.join(Cus_f_select,on='cus_id',how='right')
)
ch

'''
Cus_sel = Cus_f.select(['cus_id', 'cus_city', 'cus_state', 'cus_contry'])
Inv_sel = Inv_f.select(['inv_id', 'cus_id', 'total', 'date_time_year'])
Invt_sel = Invt_f.select(['inv_id'])
'''

Inv_select=Inv_f.select(['inv_id','inv_d','date_time', 'date_time_month', 'date_time_year'])
date_inv_id=Inv_f.select(['inv_id'])
date_inv_id

# สร้าง dim_date 
dim_date = (
    Inv_select
    .join(date_inv_id, on='inv_id', how='inner')  # เชื่อมข้อมูลกับ date_inv_id
    .select(['inv_id', 'date_time', 'date_time_month', 'date_time_year'])  # เลือกคอลัมน์ที่ต้องการ
    .with_columns([
        pl.when(pl.col('date_time_month').is_between(1, 3)).then(1)
        .when(pl.col('date_time_month').is_between(4, 6)).then(2)
        .when(pl.col('date_time_month').is_between(7, 9)).then(3)
        .otherwise(4).alias('quarter')  # เพิ่มคอลัมน์ไตรมาสตามเงื่อนไข
    ])
).to_pandas()
dim_date.columns

#dim_cus
dim_customers=Cus_f.to_pandas()
dim_customers

#dim_employees
dim_employees=Em_f.to_pandas()
dim_employees

#dim_playlist
Pl_f.columns
PlT_f.columns
pl_se=Pl_f.select(['pl_id', 'pl_name'])
plt_se=PlT_f.select(['pl_id', 'tr_id'])
dim_playlist=(pl_se.join(plt_se,on='pl_id',how='left')).to_pandas()
dim_playlist

#Fact
Inv_f.columns
Invt_f
Tr_f.columns
inv_se=Inv_f.select(['inv_id', 'cus_id', 'total'])
invt_se=Invt_f.select(['invt_id', 'inv_id', 'tr_id', 'unit_price', 'quantity'])
tr_se=Tr_f.select(['tr_id'])
date_sel=dim_date.select(['inv_id'])
cus=dim_customers.select(['cus_id','em_id'])

fact_ordaers=(inv_se.join(invt_se,on='inv_id',how='left')\
              .join(tr_se,on='tr_id',how='left')\
              .join(cus,on='cus_id',how='right')\
              .join(date_sel,on='inv_id',how='left')   
).to_pandas()
fact_ordaers


pipeline1=dlt.pipeline(
    pipeline_name="Dim_Fact",destination='duckdb',
    dataset_name="Dim_and_fact_stg"
)

pipeline1.run(dim_customers,table_name="dim_customers",write_disposition='append')
pipeline1.run(dim_employees,table_name="dim_employees",write_disposition='append')
pipeline1.run(dim_tracks,table_name="dim_tracks",write_disposition='append')
pipeline1.run(dim_date,table_name="dim_date",write_disposition='append')
pipeline1.run(fact_ordaers,table_name="fact_orders",write_disposition='append')
pipeline1.run(dim_playlist,table_name="dim_playlist",write_disposition='append')

# สร้างการเชื่อมต่อกับฐานข้อมูล DuckDB
db_path_op = 'dim_fact.duckdb'
conn1 = dd.connect(db_path_op)

date_dim = pl.read_database(query="SELECT * FROM Dim_and_fact_stg.dim_date", connection=conn1)
date_dim
em_dim = pl.read_database(query="SELECT * FROM Dim_and_fact_stg.dim_employees", connection=conn1)
em_dim
cus_dim = pl.read_database(query="SELECT * FROM Dim_and_fact_stg.dim_customers", connection=conn1)
cus_dim
tr_dim= pl.read_database(query="SELECT * FROM Dim_and_fact_stg.dim_tracks", connection=conn1)
tr_dim
orders_fact= pl.read_database(query="SELECT * FROM Dim_and_fact_stg.fact_orders", connection=conn1)
orders_fact
playlist_dim=pl.read_database(query="SELECT * FROM Dim_and_fact_stg.dim_playlist",connection=conn1)
playlist_dim
dd.close()

all_col=pd.DataFrame({
      'date_dim_col':pd.Series(list(date_dim.columns)),
      'tr_col':pd.Series(list(tr_dim.columns)),
      'em_col':pd.Series(list(em_dim.columns)),
      'cus_col':pd.Series(list(cus_dim.columns)),
      'order_col':pd.Series(list(orders_fact.columns)),
      'playlist_col':pd.Series(list(playlist_dim.columns))
})
all_col

#ยอดขายตาม dim_date
# 1 ยอดขายรวมต่อปี
yearly_sales = orders_fact.join(date_dim, on="inv_id").group_by("date_time_year").agg([
    pl.col("total").sum().alias("total_sales")
]).sort("date_time_year")
yearly_sales

# 2 ยอดขายรวมต่อไตรมาสในแต่ละปี
quater_sales = orders_fact.join(date_dim, on="inv_id").group_by(["date_time_year","quarter"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year","quarter"])
quater_sales

# 3 ยอดขายรวมต่อเดือนในแต่ละปี
monthly_sales = orders_fact.join(date_dim, on="inv_id").group_by(["date_time_year","quarter","date_time_month"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year","quarter", "date_time_month"])
monthly_sales

# 4 แต่ละเพลงขายได้เท่าไหร่ในแต่ละปี
yearly_sales_by_track_type = orders_fact.join(tr_dim, on="tr_id").join(date_dim, on="inv_id").group_by(["date_time_year", "tr_name"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year", "total_sales"], descending=[False, True])
yearly_sales_by_track_type

# 5 แต่ละเพลงขายได้เท่าไหร่ในไตรมาสของแต่ละปี
quarter_sales_by_track_type = orders_fact.join(tr_dim, on="tr_id").join(date_dim, on="inv_id").group_by(["date_time_year","quarter", "tr_name"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year","quarter", "total_sales"], descending=[False,False, True])
quarter_sales_by_track_type

# 6 ยอดขายรวมตามศิลปิน (Artist) ต่อปี
yearly_sales_by_artist = orders_fact.join(tr_dim, on="tr_id").join(date_dim, on="inv_id").group_by(["date_time_year", "ar_name"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year", "total_sales"], descending=[False, True])
yearly_sales_by_artist

# 7 ยอดขายรวมตามศิลปิน (Artist) ในแต่ละไตรมาสของแต่ละปี
quarter_sales_by_artist = orders_fact.join(tr_dim, on="tr_id").join(date_dim, on="inv_id").group_by(["date_time_year", "quarter","ar_name"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year","quarter", "total_sales"], descending=[False, False,True])
quarter_sales_by_artist

# 8 ยอดขายรวมตามลูกค้า (Customer) ต่อปี
yearly_sales_by_customer = orders_fact.join(cus_dim, on="cus_id").join(date_dim, on="inv_id").group_by(["date_time_year", "cus_f", "cus_l"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year", "total_sales"], descending=[False, True])
yearly_sales_by_customer

# 9 ยอดขายรวมตามลูกค้า (Customer) แต่ละไตรมาสของแต่ละปี
quarter_sales_by_customer = orders_fact.join(cus_dim, on="cus_id").join(date_dim, on="inv_id").group_by(["date_time_year","quarter", "cus_f", "cus_l"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year", "quarter","total_sales"], descending=[False,False, True])
quarter_sales_by_customer

# 10 ยอดขายรวมตามประเทศลูกค้า (Customer Country) ต่อปี
yearly_sales_by_country = orders_fact.join(cus_dim, on="cus_id").join(date_dim, on="inv_id").group_by(["date_time_year", "cus_contry"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year", "total_sales"], descending=[False, True])
yearly_sales_by_country

# 11 ยอดขายรวมตามประเทศลูกค้า (Customer Country) ในแต่ละไตรมาสของแต่ละปี
quarter_sales_by_country = orders_fact.join(cus_dim, on="cus_id").join(date_dim, on="inv_id").group_by(["date_time_year", "quarter","cus_contry"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year", "quarter","total_sales"], descending=[False,False, True])
quarter_sales_by_country

# 12 ยอดขายรวมตามพนักงาน (Employee) ต่อปี
yearly_sales_by_employee = orders_fact.join(em_dim, on="em_id").join(date_dim, on="inv_id").group_by(["date_time_year", "em_f", "em_l"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year", "total_sales"], descending=[False, True])
yearly_sales_by_employee

# 13 ยอดขายรวมตามพนักงาน (Employee) ต่อปี
quarter_sales_by_employees = orders_fact.join(em_dim, on="em_id").join(date_dim, on="inv_id").group_by(["date_time_year", "quarter","em_f", "em_l"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year", "quarter","total_sales"], descending=[False,False, True])
quarter_sales_by_employees

# 14 ยอดขายรวมตามประเภทสื่อ (Media Type) ต่อปี
yearly_sales_by_media_type = orders_fact.join(tr_dim, on="tr_id").join(date_dim, on="inv_id").group_by(["date_time_year", "med_id"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year", "total_sales"], descending=[False, True])
yearly_sales_by_media_type

# 15 ยอดขายรวมตามประเภทสื่อ (Media Type) แต่ละไตรมาสในแต่ละปี
quarter_sales_by_media_type = orders_fact.join(tr_dim, on="tr_id").join(date_dim, on="inv_id").group_by(["date_time_year","quarter", "me_name"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year", "quarter", "total_sales"], descending=[False,False, True])
quarter_sales_by_media_type

# 16 ยอดขายรวมตามแนวเพลง (genres) ในแต่ละปี
yearly_sales_by_genres_type = orders_fact.join(tr_dim, on="tr_id").join(date_dim, on="inv_id").group_by(["date_time_year", "ge_name"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year", "total_sales"], descending=[False,True])
yearly_sales_by_genres_type

# 17 ยอดขายรวมตามแนวเพลง (genres) แต่ละไตรมาสในแต่ละปี
quarter_sales_by_genres_type = orders_fact.join(tr_dim, on="tr_id").join(date_dim, on="inv_id").group_by(["date_time_year","quarter", "ge_name"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year", "quarter","total_sales"], descending=[False,False, True])
quarter_sales_by_genres_type

# 18 แนวเพลงที่ขายดีที่สุดในแต่ละปี
top_track_type_per_year = orders_fact.join(tr_dim, on="tr_id").join(date_dim, on="inv_id").group_by(["date_time_year", "ge_name"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year", "total_sales"], descending=[False, True]).group_by("date_time_year").agg([
    pl.col("ge_name").first().alias("top_track_type"),
    pl.col("total_sales").first().alias("max_sales")
])
top_track_type_per_year

# 19 Med_type ที่ขายดีที่สุดในแต่ละปี
top_media_type_per_year = orders_fact.join(tr_dim, on="tr_id").join(date_dim, on="inv_id").group_by(["date_time_year", "me_name"]).agg([
    pl.col("total").sum().alias("total_sales")
]).sort(["date_time_year", "total_sales"], descending=[False, True]).group_by("date_time_year").agg([
    pl.col("me_name").first().alias("top_media_type"),
    pl.col("total_sales").first().alias("max_sales")
])
top_media_type_per_year

# 20 พนักงานที่ทำยอดขายสูงสุดในแต่ละปี
top_employee_per_year = yearly_sales_by_employee.group_by("date_time_year").agg([
    pl.col("em_f").first().alias("top_employee_first_name"),
    pl.col("em_l").first().alias("top_employee_last_name"),
    pl.col("total_sales").first().alias("max_sales")
])
top_employee_per_year

# 21 ลูกค้าที่ซื้อของมากที่สุดในแต่ละปี
top_customer_per_year = orders_fact.join(cus_dim, on="cus_id").join(date_dim, on="inv_id").group_by(["date_time_year", "cus_f", "cus_l"]).agg([
    pl.col("total").sum().alias("total_purchases")
]).sort(["date_time_year", "total_purchases"], descending=[False, True]).group_by("date_time_year").agg([
    pl.col("cus_f").first().alias("top_customer_first_name"),
    pl.col("cus_l").first().alias("top_customer_last_name"),
    pl.col("total_purchases").first().alias("max_purchases")
])
top_customer_per_year

# 22 ศิลปินที่ทำยอดขายได้มากที่สุดในแต่ละปี
top_artist_per_year = orders_fact.join(tr_dim, on="tr_id").join(date_dim, on="inv_id").group_by(["date_time_year", "ar_name"]).agg([
    pl.col("total").sum().alias("total_purchases")
]).sort(["date_time_year", "total_purchases"], descending=[False, True]).group_by("date_time_year").agg([
    pl.col("ar_name").first().alias("top_customer_first_name"),
    pl.col("total_purchases").first().alias("max_purchases")
])
top_artist_per_year