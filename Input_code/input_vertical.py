import os
from table_def import *
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import pymysql
from configs import *

# today_date = datetime.now().strftime("%Y%m%d")
today_date = DATE

# file_path = "G:/My Drive/Flipkart/Meesho/Vertical_master_data.xlsx"
file_path = "C:\insert_master\Vertical_master_data.xlsx"

def create_table_fk_meesho_vertical_master(database):
    print("Make a connection...")
    con = pymysql.connect(host=db_host, user=db_user, password=db_password,database=database)
    crsr = con.cursor()

    crsr.execute(f"use {fk_meesho_vertical_master_db};")

    crsr.execute(create_vertical_master_template_table)
    con.commit()
    print("Start Reading Process...")
    df = pd.read_excel(file_path, dtype=str)
    df.fillna("N/A", inplace=True)

    # Define the mapping
    mapping = {'False': 0, 'True': 1}


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


    df[['product_url_meesho', 'sku_id_meesho']] = df['product_url_meesho'].apply(lambda x: pd.Series(process_url(x)))

    new_con_alchemy = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{database}")
    print("Going to insert data in template...")
    print("inserted: ", df.to_sql(
        name=f"template_{today_date}",
        con=new_con_alchemy,
        if_exists="append",
        index=False
    ))


    print("Created the FK Link Table:", crsr.execute(create_fk_link_table))
    print("Going to insert data in FK link...")
    insert_data_fk = f"""
    INSERT IGNORE INTO `fk_product_links_{today_date}` (fk_url ) SELECT DISTINCT(`Product_Url_FK`) FROM `template_{today_date}`;
    """
    flipkart_count = crsr.execute(insert_data_fk)
    print("Inserted Link Count: ", flipkart_count)
    con.commit()


    crsr.execute(f"use `{fk_meesho_vertical_master_db}`;")
    con.commit()

    print("Created the Meesho Link Table:", crsr.execute(create_meesho_link_table))
    print("Going to insert data in Meesho link...")
    insert_meesho_data = f"""
    INSERT IGNORE INTO `{fk_meesho_vertical_master_db}`.`product_links_{today_date}` (`meesho_pid`) SELECT DISTINCT(sku_id_meesho) FROM `{fk_meesho_vertical_master_db}`.`template_{today_date}`;
    """

    meesho_count = crsr.execute(insert_meesho_data)
    print("meehso data inserted", meesho_count)
    con.commit()

    os.system(rf"python D:\deep\meesho\meesho\create_parts.py {meesho_count} vertical")
    os.system(rf"python D:\flipkart_new\flipkart\create_parts.py {flipkart_count} vertical")

    return {"Databse":database,"Meesho Links Inserted": meesho_count,"FK Links Inserted":flipkart_count}
