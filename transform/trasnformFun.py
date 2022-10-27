from datetime import datetime
from transform.tranform_promotions import transformPromotions
from transform.transform_Channels import transformChannels
from transform.transform_countries import transfromCountries
from transform.transform_customers import transformCustomers
from transform.transform_products import transfromProducts
from transform.transform_sales import transfromSales
from transform.transform_times import transformTimes
from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd

def transforms():
    try:
        
        ID = getID();
        transformChannels(ID)
        transformCustomers(ID)
        transformPromotions(ID)
        transfromProducts(ID)
        transformTimes(ID)
        transfromCountries(ID)
        transfromSales(ID)
        
        return ID;
        
        
        
    except:
        traceback.print_exc()
    finally:
        pass
    
def getID():
    type= getProperty("TYPE")
    host= getProperty("HOST")
    port= getProperty("PORT")
    user= getProperty("USER")
    pwd=  getProperty("PASSWORD")
    db=   getProperty("DBSTG")
        
    con_db = Db_Connection(type,host,port,user,pwd,db)
    ses_db = con_db.start()
    
    process_dict = {
            "date_process":[]
        }
    
    process_dict["date_process"].append(datetime.now())
    
    df_process = pd.DataFrame(process_dict)
    df_process.to_sql('process_etl',ses_db,if_exists='append',index=False)
    
    table_process = pd.read_sql('SELECT ID FROM process_etl ORDER by ID DESC LIMIT 1', ses_db)
    
    if(not table_process.empty):
        id = table_process['ID'][0]
    else:
        id = None;
    
    return id;
    
    