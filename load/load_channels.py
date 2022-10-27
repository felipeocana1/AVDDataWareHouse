from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd


def loadChannels(ID):
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
        channelsDictionary = {
            "CHANNEL_ID":[],
            "CHANNEL_DESC":[],
            "CHANNEL_CLASS":[],
            "CHANNEL_CLASS_ID":[],
            "ID_PROCESS":[]
        }
        
       
        channelCsv = pd.read_sql(f'SELECT CHANNEL_ID,CHANNEL_DESC,CHANNEL_CLASS,CHANNEL_CLASS_ID FROM channels_tra WHERE ID_PROCESS = {ID}', ses_dbStg)
        
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
            dfChannels_tra = pd.DataFrame(channelsDictionary)
            dfChannels_sor = pd.read_sql_query('SELECT CHANNEL_ID,CHANNEL_DESC,CHANNEL_CLASS,CHANNEL_CLASS_ID,ID_PROCESS FROM channels', ses_dbSor)
            if(dfChannels_sor.empty):
                dfChannels_tra.to_sql('channels',ses_dbSor,if_exists='append',index=False)
            else:
                df_merge=dfChannels_tra.merge(dfChannels_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
                df_merge.to_sql('channels',ses_dbSor, if_exists="append",index=False)
            
                
    except:
        traceback.print_exc()
    finally:
        pass