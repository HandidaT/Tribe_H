import io
from datetime import date
import pandas as pd,numpy as np
import math
import sqlalchemy
import pymysql
import zipfile,re
import os

path = 'dataout'

if os.path.isdir(path) == False:
   print("Creating dataout folder")
   os.makedirs("dataout")

def col_to_index(col):
    return sum((ord(c) - 64) * 26**i for i, c in enumerate(reversed(col.upper()))) - 1

def empirical(filename,sheet):
   data0=pd.read_excel(filename,sheet_name=sheet)
   #actual
   Str="Unnamed: "+str(col_to_index("F")-1)
   index = data0.index
   indx=index[data0[Str] == "Max"].tolist()[0]
   data=data0.iloc[indx-1:indx+2,4:6].values 
   #print(data)
   actual=data[0][1]
   #Max
   Max=data[1][1]
   #min
   Min=data[2][1]
   return actual,Max,Min
   
engine = sqlalchemy.create_engine('mysql+pymysql://root:@localhost:3306/AnalystData')#,fast_executemany=True)
engine2 = sqlalchemy.create_engine('mysql+pymysql://root:@localhost:3306/Data')#,fast_executemany=True)
   
def regression(filename,sheet):
   data1=pd.read_excel(filename,sheet_name=sheet)
   Str="Unnamed: "+str(col_to_index("R")-1)
   index = data1.index
   indx=index[data1[Str] == "Max "].tolist()[0]#data1[Str].strip()
   data=data1.iloc[indx-1:indx+2,16:18].values
   actual=data[0][1]
   #Max
   Max=data[1][1]
   #min
   Min=data[2][1]
   d=data1.iloc[indx-1:indx,2:4].values[0][0]
   year="20"+d[2:]
   quarter=d[:2]
   return quarter,year,actual,Max,Min
   
def Data(filename,tablename,sheet):
   data1=pd.read_excel(filename,sheet_name=sheet)
   latestmonth = data1.iloc[[-1]]['Date'].values[0]
   data2 = data1[data1["Date"] == latestmonth]
   data3=data2.loc[:,data2.columns.intersection(['Date', 'FacilityType', 'BedSize', 'Region', 'Manufacturer', 'Ticker', 'Group', 'Therapy', 'Anatomy','SubAnatomy', 'ProductCategory', 'Quantity', 'AvgPrice', 'TotalSpend'])]
   data3.to_sql(name=tablename,con=engine2,if_exists='append')

def print_df():
  print(csvdf)

def parse(filename,ticker,dfrowinx=0):
   Type=None;quarter=None;year=None
   estimated_ts=None;estimated_smax=None
   estimated_smin=None;forecast_actual=None
   forecast_max=None;forecast_min=None
   f1=pd.read_excel(filename,sheet_name=None)
   flag1=0;er_count=0;emp_returnlen=3;reg_returnlen=5
   emp_arr=[];reg_arr=[];emp_type=[];reg_type=[]
   pattern1=re.compile(r'Empirical Model');pattern2=re.compile(r'Regression Model');pattern3=re.compile(r'Data')
   for key in f1.keys():
      print(key)
      for j in pattern1.finditer(key):
         if '-' in key:
            temp1=key.split('-')
            temp2=temp1[1]
            temp3=temp2.strip()
            emp_type.append(temp3)
         sheet=key
         er_count+=1
         print(sheet)
         estimated_ts,estimated_smax,estimated_smin=empirical(filename,sheet)
         emp_arr.append(estimated_ts)
         emp_arr.append(estimated_smax)
         emp_arr.append(estimated_smin)
      for j in pattern2.finditer(key):
         if '-' in key:
            temp1=key.split('-')
            temp2=temp1[1]
            temp3=temp2.strip()
            reg_type.append(temp3)
         sheet=key
         print(sheet)
         quarter,year,forecast_actual,forecast_max,forecast_min=regression(filename,sheet)
         reg_arr.append(quarter)
         reg_arr.append(year)
         reg_arr.append(forecast_actual)
         reg_arr.append(forecast_max)
         reg_arr.append(forecast_min)
      for j in pattern3.finditer(key):
         print(j)
         temp1=filename.split(".")[0]
         tablename=temp1.split('/')[1]
         Data(filename,tablename,key)
         print(tablename)
      #er_count*2
   for i in range(er_count):
      if not emp_type:
         print("####################### type None #############)
         Type=None
      else:
         print(emp_type)
         if emp_type[i]!=reg_type[i]:
            print("################ types not equal #############")
         else:
            print("############# types equal ##############")
            Type=emp_type[i]

csvdf.loc[dfrowinx]=[currentdate,ticker,Type,reg_arr[i],reg_arr[i+1],emp_arr[i],emp_arr[i+1],emp_arr[i+2],reg_arr[i+2],reg_arr[i+3],reg_arr[i+4]]
      csvdf.to_sql(name='outputdata',con=engine,index=False,if_exists='append')
      csvdf.to_csv(path+'/'+filename.split('/')[1].split('.')[0]+'.csv')
      dfrowinx+=1
   print_df()



#with zipfile.ZipFile("ETL Datasheet-20210628T180058Z-001.zip","r") as my_zip:
with zipfile.ZipFile("ETL Datasheett.zip","r") as my_zip:
   csvdf = pd.DataFrame(columns=['Date', 'Ticker', 'Type','Quarter','Year','Estimated_Total_Sold','Estimated_Sold_Max'\
                          ,'Estimated_Sold_Min','Forecast_wo_SA_Actual','Forecast_wo_Max','Forecast_wo_Min'])
   #t()
   dfrowinx=0
   for i in my_zip.namelist():
      my_zip.extract(i)
      spamInfo = my_zip.getinfo(i)
      print(i,spamInfo.file_size)
      ticker=0
      pattern=re.compile(r'/[A-Z]{2,4}')
      for j in pattern.finditer(i):
         ticker=i[j.start()+1:j.end()]
         print(ticker)
      currentdate=date.today().strftime("%Y/%m/%d")
      #ticker= already initialized
      parse(i,ticker,dfrowinx)








