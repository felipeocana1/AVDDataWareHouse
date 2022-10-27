from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd


def loadCountries(ID):
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
        countryDictionary = {
            "COUNTRY_ID":[],
            "COUNTRY_NAME":[],
            "COUNTRY_REGION":[],
            "COUNTRY_REGION_ID":[],
            "ID_PROCESS":[]
        }
        
        
        countryCsv = pd.read_sql(f'SELECT COUNTRY_ID,COUNTRY_NAME,COUNTRY_REGION,COUNTRY_REGION_ID FROM countries_tra WHERE ID_PROCESS = {ID}', ses_dbStg)
        
        if not countryCsv.empty:
            for contryID,conName,region,regionID in zip(
                countryCsv["COUNTRY_ID"],
                countryCsv["COUNTRY_NAME"],
                countryCsv["COUNTRY_REGION"],
                countryCsv["COUNTRY_REGION_ID"]
                ):
                
                countryDictionary["COUNTRY_ID"].append(contryID)
                countryDictionary["COUNTRY_NAME"].append(conName)
                countryDictionary["COUNTRY_REGION"].append(region)
                countryDictionary["COUNTRY_REGION_ID"].append(regionID)
                countryDictionary["ID_PROCESS"].append(ID)
                
                
        if countryDictionary["COUNTRY_ID"]:
            dfContries_tra = pd.DataFrame(countryDictionary)
            dfContries_sor = pd.read_sql_query('SELECT COUNTRY_ID,COUNTRY_NAME,COUNTRY_REGION,COUNTRY_REGION_ID,ID_PROCESS FROM countries', ses_dbSor)
            if dfContries_sor.empty: 
                dfContries_tra.to_sql('countries',ses_dbSor,if_exists='append',index=False)
            else:
                df_merge=dfContries_tra.merge(dfContries_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
                df_merge.to_sql('contries',ses_dbSor, if_exists="append",index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass