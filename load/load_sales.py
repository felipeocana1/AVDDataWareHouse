from util.db_connection import Db_Connection
from util.properties import getProperty
from transform.transformations import obt_date

import traceback
import pandas as pd


def loadsSales(ID):
    try:
                
        type= getProperty("TYPE")
        host= getProperty("HOST")
        port= getProperty("PORT")
        user= getProperty("USER")
        pwd=  getProperty("PASSWORD")
        dbSTG=   getProperty("DBSTG")
        dbSOR=   getProperty("DBSOR")
        
        
        con_dbSTG = Db_Connection(type,host,port,user,pwd,dbSTG)
        ses_dbStg = con_dbSTG.start()
        
        con_dbSOR = Db_Connection(type,host,port,user,pwd,dbSOR)
        ses_dbSor = con_dbSOR.start()
        
        
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
        salesCsv = pd.read_sql(f'SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM sales_tra WHERE ID_PROCESS = {ID}', ses_dbStg)
        
        sub_keys_prod = pd.read_sql_query('SELECT ID, PROD_ID FROM products', ses_dbSor).set_index('PROD_ID').to_dict()['ID']
        salesCsv['PROD_ID']= salesCsv['PROD_ID'].apply(lambda key: sub_keys_prod[key])

        sub_keys_cust = pd.read_sql_query('SELECT ID, CUST_ID FROM customers', ses_dbSor).set_index('CUST_ID').to_dict()['ID']
        salesCsv['CUST_ID'] = salesCsv['CUST_ID'].apply(lambda key: sub_keys_cust[key])

        sub_keys_time = pd.read_sql_query('SELECT ID, TIME_ID FROM times', ses_dbSor).set_index('TIME_ID').to_dict()['ID']
        salesCsv['TIME_ID'] = salesCsv['TIME_ID'].apply(lambda key: sub_keys_time[key])

        sub_keys_channels = pd.read_sql_query('SELECT ID, CHANNEL_ID FROM channels', ses_dbSor).set_index('CHANNEL_ID').to_dict()['ID']
        salesCsv['CHANNEL_ID'] = salesCsv['CHANNEL_ID'].apply(lambda key: sub_keys_channels[key])

        sub_keys_promotions = pd.read_sql_query('SELECT ID, PROMO_ID FROM promotions', ses_dbSor).set_index('PROMO_ID').to_dict()['ID']
        salesCsv['PROMO_ID'] = salesCsv['PROMO_ID'].apply(lambda key: sub_keys_promotions[key])
        
        
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
                salesDictionary["TIME_ID"].append(timeID)
                salesDictionary["CHANNEL_ID"].append(channelID)
                salesDictionary["PROMO_ID"].append(promoID)
                salesDictionary["QUANTITY_SOLD"].append(quantySolD)
                salesDictionary["AMOUNT_SOLD"].append(amountSold)
                salesDictionary["ID_PROCESS"].append(ID)
                
                
                
        if salesDictionary["PROD_ID"]:
            dfSales_tra = pd.DataFrame(salesDictionary)
            dfSales_sor = pd.read_sql(f'SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD,ID_PROCESS FROM sales', ses_dbSor)
            if dfSales_sor.empty:
                dfSales_tra.to_sql('sales',ses_dbSor,if_exists='append',index=False)
            else:
                df_merge=dfSales_tra.merge(dfSales_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
                df_merge.to_sql('sales',ses_dbSor, if_exists="append",index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass