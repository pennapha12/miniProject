import duckdb as dd
import dlt
import polars as pl
import pandas as pd
import numpy as np
from datetime import datetime 


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

# ปิดการเชื่อมต่อกับฐานข้อมูล DuckDB
conn.close()

#สำหรับดูว่าในแต่ละปีอัลบั้มไหนทำยอดขายได้ดีสุด 

# เลือกคอลัมน์ที่จำเป็นจากแต่ละ DataFrame
Ge_sel = Ge_f.select(['ge_id', 'ge_name'])
Al_sel = Al_f.select(['al_id', 'al_ti', 'ar_id'])
Ar_sel = Ar_f.select(['ar_id', 'ar_name'])
Invt_sel = Invt_f.select(['inv_id', 'tr_id'])
Inv_sel = Inv_f.select(['inv_id', 'total','date_time_year','date_time_month'])
Tr_sel = Tr_f.select(['tr_id', 'tr_name', 'ge_id', 'al_id'])

Top_al= (Al_sel.join(Tr_sel, on='al_id', how='inner') \
           .join(Ge_sel, on='ge_id', how='inner') \
           .join(Ar_sel, on='ar_id', how='inner') \
           .join(Invt_sel, on='tr_id', how='inner') \
           .join(Inv_sel, on='inv_id', how='inner') \
           .sort(by='total', descending=True) \
           .group_by('date_time_year', 'date_time_month')  # แบ่งกลุ่มตามปีและเดือน \
           .agg([ \
               pl.col('total').sum().alias('total_sum'),  # ผลรวมของ total ในแต่ละกลุ่ม \
               pl.count().alias('count'),  # จำนวนรายการในแต่ละกลุ่ม \
               pl.col('al_ti').sort_by('total', descending=True).first().alias('best_selling_album'),  # ชื่ออัลบั้มที่ขายดีที่สุด
               pl.col('tr_name').sort_by('total', descending=True).first().alias('best_selling_track'),  # ชื่อเพลงที่ขายดีที่สุด
               pl.col('ar_name').first().alias('astists_name')
           ])\
           .sort(by=['date_time_year', 'date_time_month'])  # เรียงลำดับตามปีและเดือน
)
Top_al.head()

#ลูกค้าส่วนใหย่อยู่ในประเทศไหน รัฐไหน เมืองไหน และมียอดขายรวมต่อปีเป็นเท่าไหร่

df = pd.DataFrame({
    'Employees': pd.Series(list(Em_f.columns)),
    'Customers': pd.Series(list(Cus_f.columns)),
    'Albums': pd.Series(list(Al_f.columns)),
    'Artists': pd.Series(list(Ar_f.columns)),
    'Genres': pd.Series(list(Ge_f.columns)),
    'Media_types': pd.Series(list(Med_f.columns)),
    'Playlists': pd.Series(list(Pl_f.columns)),
    'Tracks': pd.Series(list(Tr_f.columns)),
    'Invoices': pd.Series(list(Inv_f.columns)),
    'Invoice_items': pd.Series(list(Invt_f.columns))
})
df


# เลือกคอลัมน์ที่จำเป็นจากแต่ละ DataFrame
Cus_sel = Cus_f.select(['cus_id', 'cus_city', 'cus_state', 'cus_contry'])
Inv_sel = Inv_f.select(['inv_id', 'cus_id', 'total', 'date_time_year'])
Invt_sel = Invt_f.select(['inv_id'])

# เชื่อมโยง DataFrames ต่าง ๆ เข้าด้วยกัน
Top_cus_total = (Inv_sel.join(Cus_sel, on='cus_id', how='inner')
           .join(Invt_sel, on='inv_id', how='inner')
           .group_by(['cus_contry', 'cus_state', 'cus_city', 'date_time_year'])  # จัดกลุ่มตามประเทศ รัฐ เมือง และปี
           .agg([
               pl.col('total').sum().alias('total_sum'),  # ผลรวมของยอดขายในแต่ละกลุ่ม
               pl.count().alias('count')  # จำนวนรายการในแต่ละกลุ่ม
           ])
           .sort(by=['total_sum','cus_contry', 'cus_state', 'cus_city', 'date_time_year'],descending=True)  
)

# แสดงผลลัพธ์
Top_cus_total

top_cus_country=(Inv_sel.join(Cus_sel, on='cus_id', how='inner')
           .join(Invt_sel, on='inv_id', how='inner')
           .group_by(['cus_contry', 'cus_state', 'cus_city', 'date_time_year'])  # จัดกลุ่มตามประเทศ รัฐ เมือง และปี
           .agg([
               pl.col('total').sum().alias('total_sum'),  # ผลรวมของยอดขายในแต่ละกลุ่ม
               pl.count().alias('count')  # จำนวนรายการในแต่ละกลุ่ม
           ])
           .sort(by=['count'],descending=True)  
)
top_cus_country
