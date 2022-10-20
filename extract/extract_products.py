from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd


def extractProducts():
    try:
                
        type= getProperty("TYPE")
        host= getProperty("HOST")
        port= getProperty("PORT")
        user= getProperty("USER")
        pwd=  getProperty("PASSWORD")
        db=   getProperty("DBSTG")
        
        con_db = Db_Connection(type,host,port,user,pwd,db)
        ses_db = con_db.start()
        
        pathProducts_csv = getProperty("PRODUCTS")
        
        #Dictionary for values of chanels
        productsDictionary = {
            "prod_id":[],
            "prod_name":[],
            "prod_desc":[],
            "prod_category":[],
            "prod_category_id":[],
            "prod_category_desc":[],
            "prod_weight_class":[],
            "supplier_id":[],
            "prod_status":[],
            "prod_list_price":[],
            "prod_min_price":[]
        }
        
        #Reading the csv file
        productsCsv = pd.read_csv(pathProducts_csv)
        
        if not productsCsv.empty:
            for productID,name,prodDesc,prodCategoty,prodCateID,prodCateDesc,prodWeightClass,supliID,prodStatus,prodListPrice,prodMinPrice in zip(
                productsCsv["PROD_ID"],
                productsCsv["PROD_NAME"],
                productsCsv["PROD_DESC"],
                productsCsv["PROD_CATEGORY"],
                productsCsv["PROD_CATEGORY_ID"],
                productsCsv["PROD_CATEGORY_DESC"],
                productsCsv["PROD_WEIGHT_CLASS"],
                productsCsv["SUPPLIER_ID"],
                productsCsv["PROD_STATUS"],
                productsCsv["PROD_LIST_PRICE"],
                productsCsv["PROD_MIN_PRICE"]
                ):
                
                productsDictionary["prod_id"].append(productID)
                productsDictionary["prod_name"].append(name)
                productsDictionary["prod_desc"].append(prodDesc)
                productsDictionary["prod_category"].append(prodCategoty)
                productsDictionary["prod_category_id"].append(prodCateID)
                productsDictionary["prod_category_desc"].append(prodCateDesc)
                productsDictionary["prod_weight_class"].append(prodWeightClass)
                productsDictionary["supplier_id"].append(supliID)
                productsDictionary["prod_status"].append(prodStatus)
                productsDictionary["prod_list_price"].append(prodListPrice)
                productsDictionary["prod_min_price"].append(prodMinPrice)
                
        if productsDictionary["prod_id"]:
            ses_db.connect().execute('TRUNCATE TABLE products')
            dfProdutcs = pd.DataFrame(productsDictionary)
            dfProdutcs.to_sql('products',ses_db,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass