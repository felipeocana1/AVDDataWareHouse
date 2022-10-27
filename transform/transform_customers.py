from transform.transformations import join_2_strings, obt_gender
from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd


def transformCustomers(ID):
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
        customersCsv = pd.read_sql('SELECT CUST_ID,CUST_FIRST_NAME,CUST_LAST_NAME,CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_NUMBER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL FROM customers_ext', ses_db)
        
        if not customersCsv.empty:
            for customersID,name,lastname,gender,yearBirth,maritalStatus,address,postalCode,city,province,counrtyID,phone,income,credit,email in zip(
                customersCsv["CUST_ID"],
                customersCsv["CUST_FIRST_NAME"],
                customersCsv["CUST_LAST_NAME"],
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
                customersDictionary["CUST_FULL_NAME"].append(join_2_strings(name,lastname))
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
            dfCustomers = pd.DataFrame(customersDictionary)
            dfCustomers.to_sql('customers_tra',ses_db,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass