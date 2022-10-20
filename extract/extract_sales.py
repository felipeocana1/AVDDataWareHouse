from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd


def extractSales():
    try:
                
        type= getProperty("TYPE")
        host= getProperty("HOST")
        port= getProperty("PORT")
        user= getProperty("USER")
        pwd=  getProperty("PASSWORD")
        db=   getProperty("DBSTG")
        
        con_db = Db_Connection(type,host,port,user,pwd,db)
        ses_db = con_db.start()
        
        pathSales_csv = getProperty("SALES")
        
        #Dictionary for values of chanels
        salesDictionary = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[],
            
        }
        
        #Reading the csv file
        salesCsv = pd.read_csv(pathSales_csv)
        
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
                
                salesDictionary["prod_id"].append(prodID)
                salesDictionary["cust_id"].append(custID)
                salesDictionary["time_id"].append(timeID)
                salesDictionary["channel_id"].append(channelID)
                salesDictionary["promo_id"].append(promoID)
                salesDictionary["quantity_sold"].append(quantySolD)
                salesDictionary["amount_sold"].append(amountSold)
                
                
        if salesDictionary["prod_id"]:
            ses_db.connect().execute('TRUNCATE TABLE sales')
            dfSales = pd.DataFrame(salesDictionary)
            dfSales.to_sql('sales',ses_db,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass