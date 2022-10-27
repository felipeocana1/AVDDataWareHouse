from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd


def extractChannels():
    try:

        type= getProperty("TYPE")
        host= getProperty("HOST")
        port= getProperty("PORT")
        user= getProperty("USER")
        pwd=  getProperty("PASSWORD")
        db=   getProperty("DBSTG")
        
        con_db = Db_Connection(type,host,port,user,pwd,db)
        ses_db = con_db.start()

        pathChannels_csv = getProperty("CHANNELS")
        
        #Dictionary for values of chanels
        channelsDictionary = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[]
        }
        
        #Reading the csv file
        channelCsv = pd.read_csv(pathChannels_csv)
        
        if not channelCsv.empty:
            for chaID,desc,classParam,classID in zip(
                channelCsv["CHANNEL_ID"],
                channelCsv["CHANNEL_DESC"],
                channelCsv["CHANNEL_CLASS"],
                channelCsv["CHANNEL_CLASS_ID"]
                ):
                
                channelsDictionary["channel_id"].append(chaID)
                channelsDictionary["channel_desc"].append(desc)
                channelsDictionary["channel_class"].append(classParam)
                channelsDictionary["channel_class_id"].append(classID)
                
        if channelsDictionary["channel_id"]:
            ses_db.connect().execute('TRUNCATE TABLE channels_ext')
            dfChannels = pd.DataFrame(channelsDictionary)
            dfChannels.to_sql('channels_ext',ses_db,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass