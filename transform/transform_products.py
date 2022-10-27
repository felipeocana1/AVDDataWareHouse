from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd


def transfromProducts(ID):
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
        productsDictionary = {
            "PROD_ID":[],
            "PROD_NAME":[],
            "PROD_DESC":[],
            "PROD_CATEGORY":[],
            "PROD_CATEGORY_ID":[],
            "PROD_CATEGORY_DESC":[],
            "PROD_WEIGHT_CLASS":[],
            "SUPPLIER_ID":[],
            "PROD_STATUS":[],
            "PROD_LIST_PRICE":[],
            "PROD_MIN_PRICE":[],
            "ID_PROCESS":[]
        }
        
        #Reading the csv file
        productsCsv = pd.read_sql('SELECT PROD_ID,PROD_NAME,PROD_DESC,PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE FROM products_ext', ses_db)
        
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
                
                productsDictionary["PROD_ID"].append(productID)
                productsDictionary["PROD_NAME"].append(name)
                productsDictionary["PROD_DESC"].append(prodDesc)
                productsDictionary["PROD_CATEGORY"].append(prodCategoty)
                productsDictionary["PROD_CATEGORY_ID"].append(prodCateID)
                productsDictionary["PROD_CATEGORY_DESC"].append(prodCateDesc)
                productsDictionary["PROD_WEIGHT_CLASS"].append(prodWeightClass)
                productsDictionary["SUPPLIER_ID"].append(supliID)
                productsDictionary["PROD_STATUS"].append(prodStatus)
                productsDictionary["PROD_LIST_PRICE"].append(prodListPrice)
                productsDictionary["PROD_MIN_PRICE"].append(prodMinPrice)
                productsDictionary["ID_PROCESS"].append(ID)
                
                
        if productsDictionary["PROD_ID"]:
            dfProdutcs = pd.DataFrame(productsDictionary)
            dfProdutcs.to_sql('products_tra',ses_db,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass