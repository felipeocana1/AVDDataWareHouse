from util.db_connection import Db_Connection
from util.properties import getProperty

import traceback
import pandas as pd


def extractTimes():
    try:
                
        type= getProperty("TYPE")
        host= getProperty("HOST")
        port= getProperty("PORT")
        user= getProperty("USER")
        pwd=  getProperty("PASSWORD")
        db=   getProperty("DBSTG")
        
        con_db = Db_Connection(type,host,port,user,pwd,db)
        ses_db = con_db.start()

        pathTimes_csv = getProperty("TIMES")
        
        #Dictionary for values of chanels
        timesDictionary = {
            "time_id":[],
            "day_name":[],
            "day_number_in_week":[],
            "day_number_in_month":[],
            "calendar_week_number":[],
            "calendar_month_number":[],
            "calendar_month_desc":[],
            "end_of_cal_month":[],
            "calendar_quarter_desc":[],
            "calendar_year":[],
        }
        
        #Reading the csv file
        timesCsv = pd.read_csv(pathTimes_csv)
        
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
                
                timesDictionary["time_id"].append(timeID)
                timesDictionary["day_name"].append(dayName)
                timesDictionary["day_number_in_week"].append(dayNumberWe)
                timesDictionary["day_number_in_month"].append(dayNumberMon)
                timesDictionary["calendar_week_number"].append(calendarWeekNum)
                timesDictionary["calendar_month_number"].append(calendarMonthNum)
                timesDictionary["calendar_month_desc"].append(calendarMonthDes)
                timesDictionary["end_of_cal_month"].append(endCalMon)
                timesDictionary["calendar_quarter_desc"].append(calendarQuarterDes)
                timesDictionary["calendar_year"].append(calendarYear)
                
                
        if timesDictionary["time_id"]:
            ses_db.connect().execute('TRUNCATE TABLE times')
            dfTimes = pd.DataFrame(timesDictionary)
            dfTimes.to_sql('times',ses_db,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass