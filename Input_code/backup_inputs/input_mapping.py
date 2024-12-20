import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import pymysql
from table_def import *
from configs import *


# today_date = datetime.now().strftime("%Y%m%d")
# today_date = '20240915'

# file_path = r"D:\Project_files\provided\Flipkart\Priority_data_3rd_slot_18_DEC_2491_DONE.xlsx"
file_path = "C:\insert_master\Priority_data_3rd_slot_18_DEC_2491_DONE.xlsx"


def create_table_fk_meesho_mapping(database):
    print("Make a connection...")
    con = pymysql.connect(host=db_host, user=db_user, password=db_password,database=database)
    crsr = con.cursor()

    crsr.execute(f"use {database};")

    crsr.execute(create_fk_meesho_mapping_template_table)
    con.commit()

    print("Start Reading Process...")
    df = pd.read_excel(file_path, dtype=str)
    df.fillna("N/A", inplace=True)

    # Define the mapping
    mapping = {'False': 0, 'True': 1}

    # Apply the mapping to the specified columns
    # df['Seller_Status'] = df['Seller_Status'].map(mapping)
    df['Combo_Status'] = df['Combo_Status'].map(mapping)


    def process_url(url):

        url = url.split("?", 1)[0].split("meesho.com/")[-1]
        url = "https://www.meesho.com/" + url
        url_len = len(url)
        url = url.split("/")
        sku = url[-1]
        if url_len > 255:
            url[3] = "s"
        url = "/".join(url)
        return url, sku.lower()


    df[['Product_Url_MEESHO', 'SKU_id_MEESHO']] = df['Product_Url_MEESHO'].apply(lambda x: pd.Series(process_url(x)))

    new_con_alchemy = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{database}")
    print("Going to insert data in template...")
    print("inserted: ", df.to_sql(
        name=f"template_{DATE}",
        con=new_con_alchemy,
        if_exists="append",
        index=False
    ))

    print("Created the FK Link Table:", crsr.execute(create_fk_link_table))
    print("Going to insert data in FK link...")
    insert_data_fk = f"""
    INSERT IGNORE INTO `fk_product_links_{DATE}` (fk_url ) SELECT DISTINCT(`Product_Url_FK`) FROM `template_{DATE}`;
    """
    flipkart_count = crsr.execute(insert_data_fk)
    print("Inserted Link Count: ", flipkart_count)
    con.commit()

    crsr.execute(f"use `{database}`;")
    con.commit()

    print("Created the Meesho Link Table:", crsr.execute(create_meesho_link_table))
    print("Going to insert data in Meesho link...")
    insert_meesho_data = f"""
    INSERT IGNORE INTO `{database}`.`product_links_{DATE}` (`meesho_pid`) SELECT DISTINCT(sku_id_meesho) FROM `{database}`.`template_{DATE}`;
    """

    meesho_count = crsr.execute(insert_meesho_data)
    print("meehso data inserted", meesho_count)
    con.commit()

    # os.system(rf"python D:\siraj\Flipkart_Meesho\meesho_mapping\meesho\create_parts.py {meesho_count}")
    # os.system(rf"python D:\flipkart_new\flipkart\create_parts.py {flipkart_count}")

    return {"Databse": database, "Meesho Links Inserted": meesho_count, "FK Links Inserted": flipkart_count}
