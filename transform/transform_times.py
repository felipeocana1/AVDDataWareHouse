from util.db_connection import Db_Connection
from util.properties import getProperty
from transform.transformations import obt_date,obt_month_number

import traceback
import pandas as pd


def transformTimes(ID):
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
        timesCsv = pd.read_sql('SELECT TIME_ID,DAY_NAME,DAY_NUMBER_IN_WEEK,DAY_NUMBER_IN_MONTH,CALENDAR_WEEK_NUMBER,CALENDAR_MONTH_NUMBER,CALENDAR_MONTH_DESC,END_OF_CAL_MONTH,CALENDAR_QUARTER_DESC,CALENDAR_YEAR FROM times_ext', ses_db)
        
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
                
                timesDictionary["TIME_ID"].append(obt_date(timeID))
                timesDictionary["DAY_NAME"].append(dayName)
                timesDictionary["DAY_NUMBER_IN_WEEK"].append(dayNumberWe)
                timesDictionary["DAY_NUMBER_IN_MONTH"].append(dayNumberMon)
                timesDictionary["CALENDAR_WEEK_NUMBER"].append(calendarWeekNum)
                timesDictionary["CALENDAR_MONTH_NUMBER"].append(calendarMonthNum)
                timesDictionary["CALENDAR_MONTH_DESC"].append(calendarMonthDes)
                timesDictionary["END_OF_CAL_MONTH"].append(obt_date(endCalMon))
                timesDictionary["CALENDAR_MONTH_NAME"].append(obt_month_number(calendarMonthNum))
                timesDictionary["CALENDAR_QUARTER_DESC"].append(calendarQuarterDes)
                timesDictionary["CALENDAR_YEAR"].append(calendarYear)
                timesDictionary["ID_PROCESS"].append(ID)
                
                
                
        if timesDictionary["TIME_ID"]:
            dfTimes = pd.DataFrame(timesDictionary)
            dfTimes.to_sql('times_tra',ses_db,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass