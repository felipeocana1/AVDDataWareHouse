from util.db_connection import Db_Connection
from util.properties import getProperty
from transform.transformations import obt_date

import traceback
import pandas as pd


def transfromSales(ID):
    try:
                
        type= getProperty("TYPE")
        host= getProperty("HOST")
        port= getProperty("PORT")
        user= getProperty("USER")
        pwd=  getProperty("PASSWORD")
        db=   getProperty("DBSTG")
        
        con_db = Db_Connection(type,host,port,user,pwd,db)
        ses_db = con_db.start()
        
        
        #Dictionary for values of chanels
        salesDictionary = {
            "PROD_ID":[],
            "CUST_ID":[],
            "TIME_ID":[],
            "CHANNEL_ID":[],
            "PROMO_ID":[],
            "QUANTITY_SOLD":[],
            "AMOUNT_SOLD":[],
            "ID_PROCESS":[]
            
        }
        
        #Reading the csv file
        salesCsv = pd.read_sql('SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM sales_ext', ses_db)
        
        if not salesCsv.empty:
            for prodID,custID,timeID,channelID,promoID,quantySolD,amountSold in zip(
                salesCsv["PROD_ID"],
                salesCsv["CUST_ID"],
                salesCsv["TIME_ID"],
                salesCsv["CHANNEL_ID"],
                salesCsv["PROMO_ID"],
                salesCsv["QUANTITY_SOLD"],
                salesCsv["AMOUNT_SOLD"],
                
                ):
                
                salesDictionary["PROD_ID"].append(prodID)
                salesDictionary["CUST_ID"].append(custID)
                salesDictionary["TIME_ID"].append(obt_date(timeID))
                salesDictionary["CHANNEL_ID"].append(channelID)
                salesDictionary["PROMO_ID"].append(promoID)
                salesDictionary["QUANTITY_SOLD"].append(quantySolD)
                salesDictionary["AMOUNT_SOLD"].append(amountSold)
                salesDictionary["ID_PROCESS"].append(ID)
                
                
                
        if salesDictionary["PROD_ID"]:
            dfSales = pd.DataFrame(salesDictionary)
            dfSales.to_sql('sales_tra',ses_db,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass