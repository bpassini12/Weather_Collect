# %%
#needed to make web requests
import requests
#store the data we get as a dataframe
import pandas as pd
#for storing and pulling data
import bp_sql as bp
#mathematical operations on lists
import numpy as np
#parse the datetimes we get from NOAA
import datetime
from datetime import timedelta
import os
import sqlite3

# %%
tday = datetime.datetime.today().replace(hour=0,minute=0,second=0,microsecond=0)
tday_str = tday.strftime('%Y-%m-%d')


db_name = 'weather.db'
conn = bp.create_connection(db_name)

cwd = os.getcwd()

download_fldr = os.path.join(cwd, 'downloads')


sql_create_awsmcs_wban = ''' Create Table if not exists stations (
                                    AWSMSC text,
                                    WBAN text,
                                    NAME text,
                                    STATION text
                                    );
                                '''


bp.create_table(db_name, sql_create_awsmcs_wban)

sql_create_hrly_wx = ''' Create Table if not exists hrly_wx (
                                    STATION text,
                                    STATION_NAME text,
                                    DATE text,
                                    REPORT_TYPE text,
                                    SOURCE integer,
                                    HOURLYALTIMETERSETTING real,
                                    HOURLYDEWPOINTTEMPERATURE integer,
                                    HOURLYDRYBULBTEMPERATURE integer,
                                    HOURLYPRECIPITATION real,
                                    HOURLYPRESSURECHANGE real,
                                    HOURLYPRESSURETENDENCY integer,
                                    HOURLYRELATIVEHUMIDITY integer,
                                    HOURLYSEALEVELPRESSURE real,
                                    HOURLYSTATIONPRESSURE real,
                                    HOURLYVISIBILITY real,
                                    HOURLYWETBULBTEMPERATURE integer,
                                    HOURLYWINDDIRECTION integer,
                                    HOURLYWINDGUSTSPEED integer,
                                    HOURLYWINDSPEED integer
                                    );
                                '''

bp.create_table(db_name, sql_create_hrly_wx)

# creates 
bp.create_table(db_name, 'CREATE UNIQUE INDEX if not exists ux_hrly_wban_wthr ON hrly_wx(STATION, STATION_NAME, DATE)')

# creates 
bp.create_table(db_name, 'CREATE INDEX if not exists idx_hrly_wban_wthr ON hrly_wx(STATION, STATION_NAME, DATE)')

# pull from database table or else repull from website and load into db
try:
    station_df = pd.read_sql_query(con=conn, sql='Select* from stations')

except sqlite3.OperationalError:


    # sites
    stations_link = 'https://www.itl.nist.gov/div898/winds/asos-wx/WBAN-MSC.TXT'

    # read in html byte
    i = requests.get(stations_link)
    content = i.content
    decoded_content  = str(content,'UTF-8')

    end_list = []

    #skip first 6 rows since it doesn't contain actual data
    #read cols in first then create list with all data
    #Couldnt pd.read_csv bc some of the names are 3+ words and it would skip the >2+ NAMEs
    for n,i in enumerate(decoded_content.splitlines()[6:]):
        if n ==0:
            cols = list(i.split())
        else:
            nums = i.split()[0:2]
            names = " ".join(i for i in i.split()[2:])
            
            nums.append(names)

            end_list.append(nums)

    station_df = pd.DataFrame(end_list, columns=cols)
    station_df = station_df[station_df['NAME'].notna()]
    sl_data = pd.DataFrame({'AWSMSC':['720637'],'WBAN':['00223'],'NAME':'SUGAR LAND REGIONAL AIRPORT'})
    station_df = pd.concat([station_df, sl_data])
    station_df['STATION'] = station_df['AWSMSC'] + station_df['WBAN']

    station_df.to_sql(name='stations', con=conn, if_exists='append', index=False)

# %%
max_dt = pd.read_sql_query(con=conn, sql='''Select max(date) MAX_DATE, station 
                                            from hrly_wx
                                            group by station ''')
# %% 
station_df = station_df.merge(max_dt, how='left')
station_df.MAX_DATE.fillna('01-01-2000',inplace=True)
station_df['MAX_DATE'] = pd.to_datetime(station_df['MAX_DATE'])
station_df['MAX_DATE_STR'] = station_df['MAX_DATE'].dt.strftime('%Y-%m-%d')

# %%
# %%
hou_stations_df = station_df[(station_df.NAME.str.contains('HOUSTON','SUGAR LAND')) |(station_df.STATION=='72063700223')].reset_index(drop=True)
hou_stations_df

# %%
pull_tups = [tuple(r) for r in hou_stations_df[['STATION','MAX_DATE_STR']].to_numpy()]

# %%
def hrly_station_wx(station, strt_dte, end_dte):
   '''Pulls hourly weather from NOAA Api'''

   try:
      os.makedirs(download_fldr)
   except FileExistsError:
      # directory already exists
      pass

   vars_dict = {'HourlyAltimeterSetting': 'float',
         'HourlyDewPointTemperature': 'int',
         'HourlyDryBulbTemperature': 'int',
         'HourlyPrecipitation': 'float',
         'HourlyPressureChange': 'float',
         'HourlyPressureTendency': 'int',
         'HourlyRelativeHumidity': 'int',
         'HourlySeaLevelPressure': 'float',
         'HourlyStationPressure': 'float',
         'HourlyVisibility': 'float',
         'HourlyWetBulbTemperature': 'int',
         'HourlyWindDirection': 'int',
         'HourlyWindGustSpeed': 'int',
         'HourlyWindSpeed': 'int'}

   hrly_vars = list(vars_dict.keys())

   hrly_vars_str = '&dataTypes='.join(hrly_vars)

   noaa_api_call = f'''https://www.ncei.noaa.gov/access/services/data/v1?dataset=local-climatological-data&dataTypes={hrly_vars_str}&stations={station}&startDate={strt_dte}&endDate={end_dte}'''
   print(noaa_api_call)
   station_name = station_df[station_df.STATION==station]['NAME'].iloc[0]

   filename = f'{station_name}_{strt_dte}_{end_dte}.csv'
   with open(os.path.join(download_fldr, filename), 'wb') as file:
      response = requests.get(noaa_api_call, allow_redirects=True)
      file.write(response.content)

   # get filepath and readin csv
   filepath = os.path.join(download_fldr,filename) 

   df = pd.read_csv(filepath)

   # column formatting
   df['DATE'] = pd.to_datetime(df.DATE).round('60min')


   # get a list of all non-main columns that are also object datatypes
   # all should be numeric but sometimes they have random strings in them, so the str needs to be removed
   object_var_cols = list(set(df.select_dtypes(include=['object']).columns).intersection(set(hrly_vars)))

   # remove any strings from these variable columns because they shouldnt have any strings in them
   df[object_var_cols] = df[object_var_cols].replace(r'[^\d.]+', '',regex=True)

   #fill NaNs and change to ints
   df[hrly_vars] = df[hrly_vars].fillna(0)
   df[hrly_vars] = df[hrly_vars].replace('','0')
   df = df.astype(vars_dict)

   df.insert(1,'STATION_NAME', station_name)
   df.columns = df.columns.str.lower()

   # sort and keep the last
   df = df[df.report_type.isin(['FM-12','FM-15','FM-16'])].reset_index(drop=True)
   df['source'] = df.source.astype(str)
   df.sort_values(by=['date','source'], ascending=True, inplace=True)
   df.drop_duplicates(keep='last', subset=['station','date'],inplace=True)

   return df
# %%
comb_df = pd.concat([hrly_station_wx(station=tup[0], strt_dte=tup[1], end_dte=tday_str) for tup in pull_tups[0:1]])
# %%

# %%
hrly_station_wx('72242953910','2023-10-01', '2023-10-10')
# %%
for tup in pull_tups[0:1]:
    print(tup)
    print(tup[0],tup[1])
# %%

comb_df = pd.DataFrame()
for tup in pull_tups:
    print(tup)
    dwnld_df = hrly_station_wx(station=tup[0], strt_dte=tup[1], end_dte=tday_str)
    comb_df = pd.concat([comb_df,dwnld_df])
#%%