import traceback
from datetime import datetime
from configs import *
import pandas as pd
import pymysql
# from slack_sdk import WebClient
from table_def import *
from sqlalchemy import create_engine
import os


def create_table_sy_meesho(database):
    print("Make a connection...")
    con = pymysql.connect(host=db_host,
                          user=db_user,
                          password=db_password,
                          db=database)
    crsr = con.cursor()

    crsr.execute(create_sy_meesho_template_table)
    con.commit()
    print("Start Reading Process...")
    try:
        try:
            # noinspection PyTypeChecker
            df = pd.read_excel(
                r"Z:\shopsy_master\sy_meesho_Master.xlsx",
                dtype=str,
                usecols=[
                    "vertical", "Analytic_super_category", "analytic_category", "Unique_ID", "Combo_Status", "Combo_Value",
                    "fk_FSN", "Product_Url_fk", "Product_Url_MEESHO", "SKU_id_MEESHO", "SKU_id_fk", "Pincode", "City"
                ],
            )
        except:
            # noinspection PyTypeChecker
            df = pd.read_excel(
                r"C:\insert_master\sy_meesho_Master.xlsx",
                dtype=str,
                usecols=[
                    "vertical", "Analytic_super_category", "analytic_category", "Unique_ID", "Combo_Status", "Combo_Value",
                    "fk_FSN", "Product_Url_fk", "Product_Url_MEESHO", "SKU_id_MEESHO", "SKU_id_fk", "Pincode", "City"
                ],
            )

        df['Combo_Value'] = df['Combo_Value'].fillna("N/A")
        # Define the mapping
        mapping = {'False': 0, 'True': 1}

        # Apply the mapping to the specified columns
        df['Combo_Status'] = df['Combo_Status'].map(mapping)


        def get_meesho_pid(url):
            url = url.split("?")[0].strip("/")
            url = url.split("/")[-1]
            return url.strip("/").strip("\\")


        df['SKU_id_MEESHO'] = df['Product_Url_MEESHO'].apply(get_meesho_pid)

        # print(df)

        new_con_alchemy = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{sy_meesho_db}")
        print("Going to insert data in template...")
        print("inserted: ", df.to_sql(
            name=f"template_{DATE}",
            con=new_con_alchemy,
            if_exists="append",
            index=False
        ))

        crsr.execute(create_sy_link_table)
        print("Going to insert data in SY link...")
        insert_into_fk_link_table = f"""
        INSERT IGNORE INTO `sy_product_links_{DATE}` (sy_url) 
        SELECT DISTINCT(`Product_Url_FK`) FROM `template_{DATE}`;
        """

        flipkart_count = crsr.execute(insert_into_fk_link_table)
        print("SY Links Inserted: ", flipkart_count)
        con.commit()
        #

        crsr.execute(create_sy_meesho_link_table)
        print("Going to insert data in Meeshho link...")
        insert_into_meesho_table = f"""
        INSERT IGNORE INTO `product_links_{DATE}` (`meesho_pid`) select DISTINCT(sku_id_meesho) FROM `template_{DATE}`;
        """

        meesho_count = crsr.execute(insert_into_meesho_table)
        print("Meesho Links Inserted: ", meesho_count)
        con.commit()

        # os.system(rf"python D:\deep\meesho\meesho\create_parts.py {meesho_count}")
        # os.system(rf"python D:\flipkart_new\flipkart\create_parts.py {flipkart_count}")

        return {"Databse": database, "Meesho Links Inserted": meesho_count, "FK Links Inserted": flipkart_count}

    except Exception as e:
        print(e)
        tb_str = traceback.format_exc()
        error_message = f"An error occurred in shopsy-meesho master:\n```\n{tb_str}\n```"
        print(error_message)
        # send_error_to_slack(error_message) #HJ

        return {"Databse":database,"Error Message": f"An error occurred in shopsy-meesho master:\n```\n{tb_str}\n```"}