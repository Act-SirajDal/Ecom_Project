import os
from datetime import datetime
from configs import *
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from table_def import *


def create_table_sy_meesho_vertical_master(database):
    print("Make a connection...")
    con = pymysql.connect(host=db_host, user=db_user, password=db_password, db=database)
    crsr = con.cursor()

    crsr.execute(create_sy_meesho_vertical_master_template_table)
    con.commit()
    print("Start Reading Process...")
    try:
        # noinspection PyTypeChecker
        df = pd.read_excel(
            "//172.28.151.201/kshk-fk-meesho/shopsy_master/vertical_shopsy_master.xlsx",
            dtype=str,
            usecols=[
                "vertical", "Unique_id", "Combo_Status", "Combo_Value", "FK_FSN", "Product_Url_FK", "Product_Url_MEESHO",
                "Pincode", "City", "SKU_id_MEESHO", "SKU_id_FK", "Search Term", "position", "mtrusted"
            ],
        )
    except:
        # noinspection PyTypeChecker
        df = pd.read_excel(
            "D:/Meesho/tfiles/vertical_shopsy_master_1848.xlsx",
            dtype=str,
            usecols=[
                "vertical", "Unique_id", "Combo_Status", "Combo_Value", "FK_FSN", "Product_Url_FK",
                "Product_Url_MEESHO",
                "Pincode", "City", "SKU_id_MEESHO", "SKU_id_FK", "Search Term", "position", "mtrusted"
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
    df['Product_Url_FK'] = "https://www.shopsy.in/p/p/i?pid=" + df['FK_FSN']

    new_con_alchemy = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{database}")
    print("Going to insert data in template...")
    print("inserted: ", df.to_sql(
        name=f"template_{DATE}",
        con=new_con_alchemy,
        if_exists="append",
        index=False
    ))

    crsr.execute(create_sy_link_table)
    print("Going to insert data in FK link...")
    insert_into_fk_link_table = f"""
    INSERT IGNORE INTO `sy_product_links_{DATE}` (sy_url)
    SELECT DISTINCT(`Product_Url_FK`) FROM `template_{DATE}`;
    """

    flipkart_count = crsr.execute(insert_into_fk_link_table)
    print("FK Links Inserted: ", flipkart_count)
    con.commit()

    crsr.execute(create_sy_meesho_link_table)
    print("Going to insert data in Meesho Link...")
    insert_into_meesho_table = f"""
    INSERT IGNORE INTO `product_links_{DATE}` (`meesho_pid`) select DISTINCT(sku_id_meesho) FROM `template_{DATE}`;
    """

    meesho_count = crsr.execute(insert_into_meesho_table)
    print("Meesho Links Inserted: ", meesho_count)
    con.commit()

    os.system(command=f"python D:/flipkart_new/flipkart/create_parts.py {flipkart_count}")
    os.system(command=f"python D:/deep/meesho/meesho/create_parts_shopsy.py {meesho_count}")

    return {"Databse": database, "Meesho Links Inserted": meesho_count, "SY Links Inserted": flipkart_count}