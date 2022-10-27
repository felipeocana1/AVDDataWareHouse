from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd


def loadTimes(ID):
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
        timesDictionary = {
            "TIME_ID":[],
            "DAY_NAME":[],
            "DAY_NUMBER_IN_WEEK":[],
            "DAY_NUMBER_IN_MONTH":[],
            "CALENDAR_WEEK_NUMBER":[],
            "CALENDAR_MONTH_NUMBER":[],
            "CALENDAR_MONTH_DESC":[],
            "END_OF_CAL_MONTH":[],
            "CALENDAR_MONTH_NAME":[],
            "CALENDAR_QUARTER_DESC":[],
            "CALENDAR_YEAR":[],
            "ID_PROCESS":[]
        }
        
        #Reading the csv file
        timesCsv = pd.read_sql(f'SELECT TIME_ID,DAY_NAME,DAY_NUMBER_IN_WEEK,DAY_NUMBER_IN_MONTH,CALENDAR_WEEK_NUMBER,CALENDAR_MONTH_NUMBER,CALENDAR_MONTH_DESC,END_OF_CAL_MONTH,CALENDAR_MONTH_NAME,CALENDAR_QUARTER_DESC,CALENDAR_YEAR FROM times_tra WHERE ID_PROCESS = {ID}', ses_dbStg)
        
        if not timesCsv.empty:
            for timeID,dayName,dayNumberWe,dayNumberMon,calendarWeekNum,calendarMonthNum,calendarMonthDes,endCalMon,calendarQuarterDes,calendarYear in zip(
                timesCsv["TIME_ID"],
                timesCsv["DAY_NAME"],
                timesCsv["DAY_NUMBER_IN_WEEK"],
                timesCsv["DAY_NUMBER_IN_MONTH"],
                timesCsv["CALENDAR_WEEK_NUMBER"],
                timesCsv["CALENDAR_MONTH_NUMBER"],
                timesCsv["CALENDAR_MONTH_DESC"],
                timesCsv["END_OF_CAL_MONTH"],
                timesCsv["CALENDAR_QUARTER_DESC"],
                timesCsv["CALENDAR_YEAR"]
                ):
                
                timesDictionary["TIME_ID"].append(timeID)
                timesDictionary["DAY_NAME"].append(dayName)
                timesDictionary["DAY_NUMBER_IN_WEEK"].append(dayNumberWe)
                timesDictionary["DAY_NUMBER_IN_MONTH"].append(dayNumberMon)
                timesDictionary["CALENDAR_WEEK_NUMBER"].append(calendarWeekNum)
                timesDictionary["CALENDAR_MONTH_NUMBER"].append(calendarMonthNum)
                timesDictionary["CALENDAR_MONTH_DESC"].append(calendarMonthDes)
                timesDictionary["END_OF_CAL_MONTH"].append(endCalMon)
                timesDictionary["CALENDAR_MONTH_NAME"].append(calendarMonthNum)
                timesDictionary["CALENDAR_QUARTER_DESC"].append(calendarQuarterDes)
                timesDictionary["CALENDAR_YEAR"].append(calendarYear)
                timesDictionary["ID_PROCESS"].append(ID)
                
                
                
        if timesDictionary["TIME_ID"]:
            dfTimes_tra = pd.DataFrame(timesDictionary)
            dfTimes_sor = pd.read_sql(f'SELECT TIME_ID,DAY_NAME,DAY_NUMBER_IN_WEEK,DAY_NUMBER_IN_MONTH,CALENDAR_WEEK_NUMBER,CALENDAR_MONTH_NUMBER,CALENDAR_MONTH_DESC,END_OF_CAL_MONTH,CALENDAR_MONTH_NAME,CALENDAR_QUARTER_DESC,CALENDAR_YEAR,ID_PROCESS FROM times', ses_dbSor);
            if dfTimes_sor.empty:
                dfTimes_tra.to_sql('times',ses_dbSor,if_exists='append',index=False)
            else:
                df_merge=dfTimes_tra.merge(dfTimes_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
                df_merge.to_sql('times',ses_dbSor, if_exists="append",index=False)  
                
    except:
        traceback.print_exc()
    finally:
        pass