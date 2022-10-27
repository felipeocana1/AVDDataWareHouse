from transform.transformations import obt_date
from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd


def transformPromotions(ID):
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
        promotionsDictionary = {
            "PROMO_ID":[],
            "PROMO_NAME":[],
            "PROMO_COST":[],
            "PROMO_BEGIN_DATE":[],
            "PROMO_END_DATE":[],
            "ID_PROCESS":[]
        }
        
        #Reading the csv file
        promotionsCsv = pd.read_sql('SELECT PROMO_ID,PROMO_NAME,PROMO_COST,PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions_ext', ses_db)
        
        if not promotionsCsv.empty:
            for promotionID,name,promCost,promBeginDate,promEndDate in zip(
                promotionsCsv["PROMO_ID"],
                promotionsCsv["PROMO_NAME"],
                promotionsCsv["PROMO_COST"],
                promotionsCsv["PROMO_BEGIN_DATE"],
                promotionsCsv["PROMO_END_DATE"]
                ):
                
                promotionsDictionary["PROMO_ID"].append(promotionID)
                promotionsDictionary["PROMO_NAME"].append(name)
                promotionsDictionary["PROMO_COST"].append(promCost)
                promotionsDictionary["PROMO_BEGIN_DATE"].append(obt_date(promBeginDate))
                promotionsDictionary["PROMO_END_DATE"].append(obt_date(promEndDate))
                promotionsDictionary["ID_PROCESS"].append(ID)
                
                
        if promotionsDictionary["PROMO_ID"]:
            dfPromotions = pd.DataFrame(promotionsDictionary)
            dfPromotions.to_sql('promotions_tra',ses_db,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass