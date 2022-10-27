from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd


def transformChannels(ID):
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
        channelsDictionary = {
            "CHANNEL_ID":[],
            "CHANNEL_DESC":[],
            "CHANNEL_CLASS":[],
            "CHANNEL_CLASS_ID":[],
            "ID_PROCESS":[]
        }
        
        #Reading the csv file
        channelCsv = pd.read_sql('SELECT CHANNEL_ID,CHANNEL_DESC,CHANNEL_CLASS,CHANNEL_CLASS_ID FROM channels_ext', ses_db)
        
        if not channelCsv.empty:
            for chaID,desc,classParam,classID in zip(
                channelCsv["CHANNEL_ID"],
                channelCsv["CHANNEL_DESC"],
                channelCsv["CHANNEL_CLASS"],
                channelCsv["CHANNEL_CLASS_ID"]
                ):
                
                channelsDictionary["CHANNEL_ID"].append(chaID)
                channelsDictionary["CHANNEL_DESC"].append(desc)
                channelsDictionary["CHANNEL_CLASS"].append(classParam)
                channelsDictionary["CHANNEL_CLASS_ID"].append(classID)
                channelsDictionary["ID_PROCESS"].append(ID)
                
                
        if channelsDictionary["CHANNEL_ID"]:
            dfChannels = pd.DataFrame(channelsDictionary)
            dfChannels.to_sql('channels_tra',ses_db,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass