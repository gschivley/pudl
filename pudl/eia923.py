# Helper functions & other objects to ingest & process Energy Information
# Administration (EIA) Form 923 data.

import numpy as np
import pandas as pd
import os



"""Retrieve data from EIA Form 923 for analysis.

For now this pulls from the published Excel spreadsheets. The same data
may also be available in a more machine readable form via the EIA's bulk
JSON download facility, but those files will require parsing.

@author: alana for Catalyst Cooperative
This code is for use analyzing EIA Form 923 data, years 2008-2016 
Current version is for years 2014-2016, which have standardized naming convetions and file formatting
"""

os.chdir(os.path.join('C:\\','Users','alana','Dropbox','Catalyst_Coop'))
os.getcwd()
MainFolder=r"C:\Users\alana\Dropbox\Catalyst_Coop"

#Select years for analysis
#def get_eia923(years=[2015,]):

year_list = range(2014,2017) #years for which data is analyzed: 2008-2016 (python range is up to but not including 2017)
#file2008= 'eia923December2008.xls' #include if using 2008 file: it has non-standard naming convention so requires manual import



# the top block finds folders inside MainFolder. 
#whatever you want to do for each folder should be inside this folderItem in listFolder loop

def get_eia923(years=[2014,2015,2016]):
    myFileList=[] #Empty list to add xcel files to
    #sheetnames=[] #Empty list to gather tab names from each spreadsheet
    listFolder=[x[0] for x in os.walk(MainFolder)] #lists all folders inside MainFolder
    for folderItem in listFolder:   #folderItem is individual folder within listFolder
        print (folderItem)
        for filename in os.listdir(folderItem): #filename is individual spreadsheet within folderItem
            fileList=[] #fileList is list of all files in folderItem
            fileList.append(filename) #adds each file name to the list
            filepath = os.path.join(folderItem, filename) #creates a full path (as string) to file
            for fileItem in fileList:
                if '2_3_4' in fileItem: #2_3_4 is the string that all desired spreadsheets have in common for years 2009-2016
                    for year in years:
                        if str(year) in filepath:
                            xlsx_file = pd.ExcelFile(filepath) #imports excel file using pandas
                            myFileList.append(xlsx_file) #Saves files in fileList that fit the 'if' terms to the myFileList outside of loop
    #            if (fileItem.title()).lower()==file2008.lower(): 
    #                print (fileItem.title())
    #                for year in years:
    #                    if str(year) in filepath:
    #                        xlsx_file = pd.ExcelFile(filepath) #imports excel file using pandas
    #                        myFileList.append(xlsx_file) #Saves files in fileList that fit the 'if' terms to the myFileList outside of loop
    
    dfP1GenerationAndFuelData = pd.DataFrame()
    dfP2Stocks = pd.DataFrame()
    dfP3BoilerFuel = pd.DataFrame()
    dfP4Generator = pd.DataFrame()
    dfP5FuelReceiptsAndCosts = pd.DataFrame()
    dfP6PlantFrame = pd.DataFrame()
    
    count=0
    
    for f in myFileList:
        data = pd.read_excel(f, sheetname=0,skiprows=5)
        dfP1GenerationAndFuelData = dfP1GenerationAndFuelData.append(data)
        dfP1GenerationAndFuelData = dfP1GenerationAndFuelData.reset_index(drop=True)
        ##Page 2 does not have a 'Year' field, so it needs to be added
        data=pd.read_excel(f,sheetname=1,skiprows=5)
        count= count+1
        if count==1:
            data['Year']='2014'
        if count==2:
            data['Year']='2015'
        if count==3:
            data['Year']='2016' 
        dfP2Stocks = dfP2Stocks.append(data)
        dfP2Stocks = dfP2Stocks.reset_index(drop=True)
        data = pd.read_excel(f, sheetname=2,skiprows=5)
        dfP3BoilerFuel = dfP3BoilerFuel.append(data)
        dfP3BoilerFuel = dfP3BoilerFuel.reset_index(drop=True)
        data = pd.read_excel(f, sheetname=3,skiprows=5)
        dfP4Generator = dfP4Generator.append(data)
        dfP4Generator = dfP4Generator.reset_index(drop=True)
        data = pd.read_excel(f, sheetname=4,skiprows=4)
        dfP5FuelReceiptsAndCosts = dfP5FuelReceiptsAndCosts.append(data)
        dfP5FuelReceiptsAndCosts = dfP5FuelReceiptsAndCosts.reset_index(drop=True)
        data = pd.read_excel(f, sheetname=5,skiprows=4)
        dfP6PlantFrame = dfP6PlantFrame.append(data)
        dfP6PlantFrame = dfP6PlantFrame.reset_index(drop=True)
        
    return dfP1GenerationAndFuelData

get_eia923()



def demo_func(tabname):
    doc string to explain acceptable use of function (e.g. x possible things that can be passedin)
    tabmap = {"boiler_fuel": 0,
              "generators":1,
              "fuel_delivered":2}

    rowskip = {"boiler_fuel": 5}
    
    df = pd.read_excel("myfile.xlsx", ?sheetname=?tabmap[tabname], skiprows = rowskip[tabname])
    
    
demo_func("boiler_fuel")
    
