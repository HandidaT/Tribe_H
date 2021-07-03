#import openpyxl
import pandas as pd


def xlsx_extractto_csv(filename,startrow,endrow,startcol,endcol,output_csv):
   file=filename
   data=pd.ExcelFile(file)
   print(data.sheet_names)
   df=data.parse(data.sheet_names[0])#("Informatics")
   print(df.info,"\n",df.head(10))
   df1=df.iloc[startrow:endrow,startcol:endcol]
   print("\n\n",df1.head(18))
   df1.to_csv(output_csv)


fname="ABMD - EndNov2018_Send.xlsx"

xlsx_extractto_csv(fname,1,18,2,357,"csv_file1.csv")


'''
ps=openpyxl.load_workbook("ABMD - EndNov2018_Send.xlsx")
sheet=ps["Informatics"]
print(sheet.max_row)'''
