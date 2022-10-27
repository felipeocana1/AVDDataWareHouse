from transform.transformations import join_2_strings, obt_gender
from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd


def loadCustomers(ID):
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
        customersDictionary = {
            "CUST_ID":[],
            "CUST_FULL_NAME":[],
            "CUST_GENDER":[],
            "CUST_YEAR_OF_BIRTH":[],
            "CUST_MARITAL_STATUS":[],
            "CUST_STREET_ADDRESS":[],
            "CUST_POSTAL_CODE":[],
            "CUST_CITY":[],
            "CUST_STATE_PROVINCE":[],
            "COUNTRY_ID":[],
            "CUST_MAIN_PHONE_NUMBER":[],
            "CUST_INCOME_LEVEL":[],
            "CUST_CREDIT_LIMIT":[],
            "CUST_EMAIL":[],
            "ID_PROCESS":[]   
        }
        
        #Reading the csv file
        customersCsv = pd.read_sql(f'SELECT CUST_ID,CUST_FULL_NAME,CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_NUMBER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL FROM customers_tra WHERE ID_PROCESS = {ID} ', ses_dbStg)
        
        sub_keys = pd.read_sql_query('SELECT ID, COUNTRY_ID FROM countries', ses_dbSor).set_index('COUNTRY_ID').to_dict()['ID']
        
        customersCsv['COUNTRY_ID'] = customersCsv['COUNTRY_ID'].apply(lambda key: sub_keys[key])
        
        if not customersCsv.empty:
            for customersID,name,gender,yearBirth,maritalStatus,address,postalCode,city,province,counrtyID,phone,income,credit,email in zip(
                customersCsv["CUST_ID"],
                customersCsv["CUST_FULL_NAME"],
                customersCsv["CUST_GENDER"],
                customersCsv["CUST_YEAR_OF_BIRTH"],
                customersCsv["CUST_MARITAL_STATUS"],
                customersCsv["CUST_STREET_ADDRESS"],
                customersCsv["CUST_POSTAL_CODE"],
                customersCsv["CUST_CITY"],
                customersCsv["CUST_STATE_PROVINCE"],
                customersCsv["COUNTRY_ID"],
                customersCsv["CUST_MAIN_PHONE_NUMBER"],
                customersCsv["CUST_INCOME_LEVEL"],
                customersCsv["CUST_CREDIT_LIMIT"],
                customersCsv["CUST_EMAIL"]
                ):
                
                customersDictionary["CUST_ID"].append(customersID)
                customersDictionary["CUST_FULL_NAME"].append(name)
                customersDictionary["CUST_GENDER"].append(obt_gender(gender))
                customersDictionary["CUST_YEAR_OF_BIRTH"].append(yearBirth)
                customersDictionary["CUST_MARITAL_STATUS"].append(maritalStatus)
                customersDictionary["CUST_STREET_ADDRESS"].append(address)
                customersDictionary["CUST_POSTAL_CODE"].append(postalCode)
                customersDictionary["CUST_CITY"].append(city)
                customersDictionary["CUST_STATE_PROVINCE"].append(province)
                customersDictionary["COUNTRY_ID"].append(counrtyID)
                customersDictionary["CUST_MAIN_PHONE_NUMBER"].append(phone)
                customersDictionary["CUST_INCOME_LEVEL"].append(income)
                customersDictionary["CUST_CREDIT_LIMIT"].append(credit)
                customersDictionary["CUST_EMAIL"].append(email)
                customersDictionary["ID_PROCESS"].append(ID)
                
                
        if customersDictionary["CUST_ID"]:
            dfCustomers_tra = pd.DataFrame(customersDictionary)
            dfCustomers_sor = pd.read_sql(f'SELECT CUST_ID,CUST_FULL_NAME,CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_NUMBER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL,ID_PROCESS FROM customers', ses_dbSor)
            
            if dfCustomers_sor.empty:
                dfCustomers_tra.to_sql('customers',ses_dbSor,if_exists='append',index=False)
            else:
                df_merge=dfCustomers_tra.merge(dfCustomers_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
                df_merge.to_sql('customers',ses_dbSor, if_exists="append",index=False)
            
                
    except:
        traceback.print_exc()
    finally:
        pass