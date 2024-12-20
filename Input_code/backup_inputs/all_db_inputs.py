import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pymysql
from sqlalchemy import create_engine
from configs import *
from table_def import *
from inputs import create_table_fk_meesho_master
from input_vertical import create_table_fk_meesho_vertical_master
from input_mapping import create_table_fk_meesho_mapping
from input_shopsy_master import create_table_sy_meesho

print('Inserting all data...')

today_date = DATE

# list_of_db = [fk_meesho_master_mapping_db,fk_meesho_vertical_master_db,fk_meesho_mapping_db,sy_meesho_db]
list_of_db = [fk_meesho_mapping_db]

for i in list_of_db:
    if i == "fk_meesho_master_mapping":
        try:
            print("Process Running for : ",fk_meesho_master_mapping_db)
            create_table_fk_meesho_master(fk_meesho_master_mapping_db)
        except Exception as e:
            print("Error for create table in ",i)
    elif i == "fk_meesho_vertical_master":
        try:
            print("Process Running for : ",fk_meesho_vertical_master_db)
            create_table_fk_meesho_vertical_master(fk_meesho_vertical_master_db)
        except Exception as e:
            print("Error for create table in ",i)
    elif i == "fk_meesho_mapping":
        try:
            print("Process Running for : ",fk_meesho_mapping_db)
            create_table_fk_meesho_mapping(fk_meesho_mapping_db)
        except Exception as e:
            print("Error for create table in ",i)
    elif i == "sy_meesho":
        try:
            print("Process Running for : ",sy_meesho_db)
            create_table_sy_meesho(sy_meesho_db)
        except Exception as e:
            print("Error for create table in ",i)
    else:
        print("Please check Database.....")