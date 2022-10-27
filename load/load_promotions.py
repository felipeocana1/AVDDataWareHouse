from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd


def loadPromotions(ID):
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
        promotionsDictionary = {
            "PROMO_ID":[],
            "PROMO_NAME":[],
            "PROMO_COST":[],
            "PROMO_BEGIN_DATE":[],
            "PROMO_END_DATE":[],
            "ID_PROCESS":[]
        }
        
        #Reading the csv file
        promotionsCsv = pd.read_sql(f'SELECT PROMO_ID,PROMO_NAME,PROMO_COST,PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions_tra WHERE ID_PROCESS = {ID}', ses_dbStg)
        
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
                promotionsDictionary["PROMO_BEGIN_DATE"].append(promBeginDate)
                promotionsDictionary["PROMO_END_DATE"].append(promEndDate)
                promotionsDictionary["ID_PROCESS"].append(ID)
                
                
        if promotionsDictionary["PROMO_ID"]:
            dfPromotions_tra = pd.DataFrame(promotionsDictionary)
            dfPromotions_sor= pd.read_sql(f'SELECT PROMO_ID,PROMO_NAME,PROMO_COST,PROMO_BEGIN_DATE,PROMO_END_DATE,ID_PROCESS FROM promotions ', ses_dbSor)
            if dfPromotions_sor.empty:
                dfPromotions_tra.to_sql('promotions',ses_dbSor,if_exists='append',index=False)
            else:
                df_merge=dfPromotions_tra.merge(dfPromotions_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
                df_merge.to_sql('promotions',ses_dbSor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass