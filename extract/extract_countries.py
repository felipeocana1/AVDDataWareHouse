from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd


def extractCountries():
    try:
        
        type= getProperty("TYPE")
        host= getProperty("HOST")
        port= getProperty("PORT")
        user= getProperty("USER")
        pwd=  getProperty("PASSWORD")
        db=   getProperty("DBSTG")
        
        con_db = Db_Connection(type,host,port,user,pwd,db)
        ses_db = con_db.start()
        
        pathCoutries_csv = getProperty("COUNTRIES")
        
        #Dictionary for values of chanels
        countryDictionary = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[]
        }
        
        #Reading the csv file
        countryCsv = pd.read_csv(pathCoutries_csv)
        
        if not countryCsv.empty:
            for contryID,conName,region,regionID in zip(
                countryCsv["COUNTRY_ID"],
                countryCsv["COUNTRY_NAME"],
                countryCsv["COUNTRY_REGION"],
                countryCsv["COUNTRY_REGION_ID"]
                ):
                
                countryDictionary["country_id"].append(contryID)
                countryDictionary["country_name"].append(conName)
                countryDictionary["country_region"].append(region)
                countryDictionary["country_region_id"].append(regionID)
                
        if countryDictionary["country_id"]:
            ses_db.connect().execute('TRUNCATE TABLE countries')
            dfContries = pd.DataFrame(countryDictionary)
            dfContries.to_sql('countries',ses_db,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass