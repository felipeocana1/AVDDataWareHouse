from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd


def transfromCountries(ID):
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
        countryDictionary = {
            "COUNTRY_ID":[],
            "COUNTRY_NAME":[],
            "COUNTRY_REGION":[],
            "COUNTRY_REGION_ID":[],
            "ID_PROCESS":[]
        }
        
        #Reading the csv file
        countryCsv = pd.read_sql('SELECT COUNTRY_ID,COUNTRY_NAME,COUNTRY_REGION,COUNTRY_REGION_ID FROM countries_ext', ses_db)
        
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
            dfContries = pd.DataFrame(countryDictionary)
            dfContries.to_sql('countries_tra',ses_db,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass