from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd


def extractCustomers():
    try:
                
        type= getProperty("TYPE")
        host= getProperty("HOST")
        port= getProperty("PORT")
        user= getProperty("USER")
        pwd=  getProperty("PASSWORD")
        db=   getProperty("DBSTG")
        
        con_db = Db_Connection(type,host,port,user,pwd,db)
        ses_db = con_db.start()
        
        pathCustomers_csv = getProperty("CUSTOMERS")
        
        #Dictionary for values of chanels
        customersDictionary = {
            "cust_id":[],
            "cust_first_name":[],
            "cust_last_name":[],
            "cust_gender":[],
            "cust_year_of_birth":[],
            "cust_marital_status":[],
            "cust_street_address":[],
            "cust_postal_code":[],
            "cust_city":[],
            "cust_state_province":[],
            "country_id":[],
            "cust_main_phone_number":[],
            "cust_income_level":[],
            "cust_credit_limit":[],
            "cust_email":[]
        }
        
        #Reading the csv file
        customersCsv = pd.read_csv(pathCustomers_csv)
        
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
                
                customersDictionary["cust_id"].append(customersID)
                customersDictionary["cust_first_name"].append(name)
                customersDictionary["cust_last_name"].append(lastname)
                customersDictionary["cust_gender"].append(gender)
                customersDictionary["cust_year_of_birth"].append(yearBirth)
                customersDictionary["cust_marital_status"].append(maritalStatus)
                customersDictionary["cust_street_address"].append(address)
                customersDictionary["cust_postal_code"].append(postalCode)
                customersDictionary["cust_city"].append(city)
                customersDictionary["cust_state_province"].append(province)
                customersDictionary["country_id"].append(counrtyID)
                customersDictionary["cust_main_phone_number"].append(phone)
                customersDictionary["cust_income_level"].append(income)
                customersDictionary["cust_credit_limit"].append(credit)
                customersDictionary["cust_email"].append(email)
                
        if customersDictionary["cust_id"]:
            ses_db.connect().execute('TRUNCATE TABLE customers_ext')
            dfCustomers = pd.DataFrame(customersDictionary)
            dfCustomers.to_sql('customers_ext',ses_db,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass