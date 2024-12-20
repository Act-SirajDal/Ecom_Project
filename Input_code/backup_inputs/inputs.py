import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pymysql
from sqlalchemy import create_engine
from configs import *
from table_def import *

today_date = DATE


def create_table_fk_meesho_master(database):
    print("Make a connection...")
    con = pymysql.connect(host=db_host, user=db_user, password=db_password,database=database)
    crsr = con.cursor()

    crsr.execute(create_master_template_table)
    con.commit()
    print("Start Reading Process...")
    try:
        # noinspection PyTypeChecker
        df = pd.read_excel(
            r"//172.28.151.201/kshk-fk-meesho/FK Master data/fk_meesho_master_mapping.xlsx",
            dtype=str,
            usecols=[
                "BU", "SC", "vertical", "Seller id", "analytic_super_category", "Seller_Status", "Combo_Status", "Combo_Value",
                "FK_FSN", "FK_BRAND", "Product_Url_FK", "Product_Url_MEESHO", "SKU_id_MEESHO", "SKU_id_FK", "Status",
                "Data_identifier", "app_fw_flag", "portfolio", "product_id", "supplier_id", "sscat_id", "sscat",
                "input_image_url"], engine='calamine'
            )
    except:
        # noinspection PyTypeChecker
        df = pd.read_excel(
            io=r"C:\insert_master\fk_meesho_master_mapping.xlsx",
            dtype=str,
            usecols=[
                "BU", "SC", "vertical", "Seller id", "analytic_super_category", "Seller_Status", "Combo_Status",
                "Combo_Value", "FK_FSN", "FK_BRAND", "Product_Url_FK", "Product_Url_MEESHO", "SKU_id_MEESHO", "SKU_id_FK", "Status",
                "Data_identifier", "app_fw_flag", "portfolio", "product_id", "supplier_id", "sscat_id", "sscat",
                "input_image_url"
            ],

        )
    df = df[df["Status"].isna()]

    del df['Status']

    # Define the mapping
    mapping = {'False': 0, 'True': 1}

    # Apply the mapping to the specified columns
    df['Seller_Status'] = df['Seller_Status'].map(mapping)
    df['Combo_Status'] = df['Combo_Status'].map(mapping)


    def process_url(url):
        if type(url) == float:
            return np.nan, np.nan
        url = url.split("?", 1)[0].split("meesho.com/")[-1]
        url = "https://www.meesho.com/" + url
        url_len = len(url)
        url = url.split("/")
        sku = url[-1]
        if url_len > 255:
            url[3] = "s"
        url = "/".join(url)
        return url.strip(), sku.lower().strip()


    df[['Product_Url_MEESHO', 'SKU_id_MEESHO']] = df['Product_Url_MEESHO'].apply(lambda x: pd.Series(process_url(x)))

    # # db_name = "fk_meesho_master_mapping"
    new_con_alchemy = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{database}")
    print("Going to insert data in template...")
    for i in [["560001", "BANGALORE"]]:
        df["Pincode"] = i[0]
        df["City"] = i[1]
        print("inserted: ", df.to_sql(
            name=f"template_{today_date}",
            con=new_con_alchemy,
            if_exists="append",
            index=False
        ))

    crsr.execute(create_fk_link_table)

    print("Going to insert data in FK link...")
    insert_into_fk_link_table = f"""
    INSERT IGNORE INTO {fk_meesho_master_mapping_db}.`fk_product_links_{today_date}` (fk_url )
    SELECT DISTINCT(`Product_Url_FK`) FROM `fk_meesho_master_mapping`.`template_{today_date}`;
    """

    flipkart_count = crsr.execute(insert_into_fk_link_table)
    print("FK Links Inserted: ", flipkart_count)
    con.commit()

    crsr.execute(create_meesho_link_table)


    print("Going to insert data in Meesho Link...")
    insert_into_meesho_table = f"""
    INSERT IGNORE INTO {fk_meesho_master_mapping_db}.`product_links_{today_date}` (`meesho_pid`)
    SELECT DISTINCT(sku_id_meesho) FROM {fk_meesho_master_mapping_db}.`template_{today_date}`;
    """

    meesho_count = crsr.execute(insert_into_meesho_table)
    print("Meesho Links Inserted: ", meesho_count)
    con.commit()

    # os.system(rf"python D:\siraj\meesho\meesho\create_parts.py {meesho_count}")
    # os.system(rf"python D:\flipkart_new\flipkart\create_parts.py {flipkart_count}")

    return {"Databse":database,"Meesho Links Inserted": meesho_count,"FK Links Inserted":flipkart_count}
