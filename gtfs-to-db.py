
import pandas as pd
import os
from controller.db import DBController

import time

database = DBController()


#directory of all gtfs-files
directory = 'D:\Dev\PythonVDV\GTFS-DB\gtfs'
obj_list = []


#for converting 25:00:00 to 01:00:00
def convert_hours(hour):

    
    if str(hour).startswith("24"):
        return hour.replace('24:','00:')
    elif str(hour).startswith("25"):
        return hour.replace('25:','01:')
    elif str(hour).startswith("26"):
        return hour.replace('26:','02:')
    elif str(hour).startswith("27"):
        return hour.replace('27:','03:')
    elif str(hour).startswith("28"):
        return hour.replace('28:','04:')
    elif str(hour).startswith("29"):
        return hour.replace('29:','05:')
    elif str(hour).startswith("30"):
        return hour.replace('30:','06:')
    else:
        return hour



def work(file, filename):
    filename = f'{filename}'
    df = pd.read_csv(file)



   



    #replace = replace ALL data, append = add data
    df.to_sql(filename, con=database.my_conn,  if_exists='replace',chunksize = 1000, index=True)
    #database.clear_table(filename)




for filename in os.listdir(directory):
    
    f = os.path.join(directory, filename)
    if os.path.isfile(f):
        start_time = time.time()
        table = os.path.splitext(filename)[0]
        print(f'doing {filename}')
        work(f, table)