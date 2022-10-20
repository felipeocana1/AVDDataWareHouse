from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd


def extractPromotions():
    try:
                
        type= getProperty("TYPE")
        host= getProperty("HOST")
        port= getProperty("PORT")
        user= getProperty("USER")
        pwd=  getProperty("PASSWORD")
        db=   getProperty("DBSTG")
        
        con_db = Db_Connection(type,host,port,user,pwd,db)
        ses_db = con_db.start()

        pathPromotions_csv = getProperty("PROMOTIONS")
        
        #Dictionary for values of chanels
        promotionsDictionary = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[]
        }
        
        #Reading the csv file
        promotionsCsv = pd.read_csv(pathPromotions_csv)
        
        if not promotionsCsv.empty:
            for promotionID,name,promCost,promBeginDate,promEndDate in zip(
                promotionsCsv["PROMO_ID"],
                promotionsCsv["PROMO_NAME"],
                promotionsCsv["PROMO_COST"],
                promotionsCsv["PROMO_BEGIN_DATE"],
                promotionsCsv["PROMO_END_DATE"]
                ):
                
                promotionsDictionary["promo_id"].append(promotionID)
                promotionsDictionary["promo_name"].append(name)
                promotionsDictionary["promo_cost"].append(promCost)
                promotionsDictionary["promo_begin_date"].append(promBeginDate)
                promotionsDictionary["promo_end_date"].append(promEndDate)
                
        if promotionsDictionary["promo_id"]:
            ses_db.connect().execute('TRUNCATE TABLE promotions')
            dfPromotions = pd.DataFrame(promotionsDictionary)
            dfPromotions.to_sql('promotions',ses_db,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass