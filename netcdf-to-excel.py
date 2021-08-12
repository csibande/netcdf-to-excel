#!usr/bin/python3
'''
Created on Apr 5, 2020

@author: Cogs
'''

import datetime
import netCDF4
import numpy as np
import pandas as pd
import os
import time
import xlsxwriter
from xlsxwriter import worksheet

def main():
    def processingTime(start_time): # calculate processing time
        mins = int((time.time() - start_time)/60)
        secs = round((((time.time() - start_time)/60 - mins) * 60), 2)
        
        return {"mins": mins, "secs": secs}
    
    print("Process started at {}\n".format(datetime.datetime.now().strftime("%H:%M:%S %p on %B %d, %Y")))
    
    start_time = time.time() # record start processing time
    
    # input and output directories to be used for processing
    input_dir = "C:\\Climate Files\\Input\\"
    output_dir = "C:\\Climate Files\\Output\\"
     
    # Create or open an Excel workbook
    workbook = xlsxwriter.Workbook('{}Forecasted Climate Data.xlsx'.format(output_dir))
    file_count = 0
     
    # set the desired location coordinates in decimal degrees
    longitude = 33.639
    latitude = -13.935
    
    # Initialise other variables
    grand_start_date = datetime.datetime.strptime("01 Jan 2006 12:00:00", "%d %b %Y %H:%M:%S") #
    grand_end_date = datetime.datetime.strptime("31 Dec 2090 12:00:00", "%d %b %Y %H:%M:%S") 
    variable_dates = {}
    nc_var = None
    header_rows_count = 2
    
    # initialize date formats
    ws_date_format = "%d-%b-%Y" # date format to be used in worksheet
    pd_date_format = "%Y-%m-%d %H:%M:%S" # data format used by pandas' datetime object
       
    # create a dictionary that will keep possible variable cases in the input folder
    variables = {
        "pr" : {"header": "Precipitation", "conv_string": "*86400"},
        "tasmin" : {"header": "Min Temp", "conv_string": "-273.15"},
        "tasmax" : {"header": "Max Temp", "conv_string": "-273.15"},
        "sfcWind" : {"header": "Wind Speed", "conv_string": "*1"},
        "hurs" : {"header": "Relative Humidity", "conv_string": "*1"},
        "sr" : {"header": "Solar Radiation", "conv_string": "*1"},
    }
    for var in variables: # create models dictionary item in all variables
        variables[var]["models"] = []
      
    # check if the netCDF variable is different from the last file looped through
    def ncVariableHasChanged(old_var, new_var):
        if old_var == new_var:
            return False
        else:
            return True
          
    def prepareWorksheet(var_name):
        # set default values
        header = variables[var_name]["header"]
          
        # use variable worksheet if it already exists, otherwise create a new one
        if header in workbook.sheetnames:
            worksheet = workbook.get_worksheet_by_name(header)
        else:
            worksheet = workbook.add_worksheet(header)
            worksheet.write(1, 0, "Date") # write(row, column, header)
             
            total_cols = 4
            # write name of model as header and merge cells
            merge_format = workbook.add_format({'bold': True, 'align': 'center'})
            worksheet.merge_range(0,0,0,total_cols, header, merge_format) # make variable name header
             
            # increase default width of all columns
            worksheet.set_column(0,total_cols,12)
             
        worksheet.set_row(1, 15, workbook.add_format({'bold': True})) # make variable name bold
          
        return worksheet
        
    # function to find array index to nearest point to desired coordinate location
    def near(array, value):
        index = (np.abs(array - value)).argmin()
        return index
    
    for subdir, dirs, files in os.walk(input_dir):
        print("Reading {} folders..\n".format(len(dirs) + 1))
        
        for file in files:                                      # loop through all files in directory
            filepath = subdir + os.sep + file                   # create a full path to the netCDF file       
            if filepath.endswith(".nc"):                        # check if the file is netCDF file or not                
                pt_start = time.time() # record start file processing time
                print("#{}. Reading: {}".format(file_count + 1, file))
                              
                nc = netCDF4.Dataset(filepath, "r")             # read netCDF file and save data to variable            
                    
                nc.variables    # initialize this to make the succeeding calls faster I think
                  
                # create arrays from the netCDF values for each dimension 
                latitude_array = nc.variables['rlat'][:]
                longitude_array = nc.variables['rlon'][:]
                     
                # convert netCDF time values to datetime objects before entering them into an array
                times = nc.variables['time']
                dates = netCDF4.num2date(times[:],times.units)
                
                # save starting and ending dates of data
                min_date = np.amin(dates)
                max_date = np.amax(dates)
                
                if max_date >= grand_start_date and min_date <= grand_end_date: # make sure the dataset data is within the range wanted
                    # find the nearest data point to the desired location
                    lat_index = near(latitude_array, latitude)
                    lon_index = near(longitude_array, longitude)
                       
                    # find out what modeled variable the netCDF file contains
                    count = 0
                    for v in variables:
                        count += 1
                        if v in nc.variables.keys():
                            var_name = v # set the name of the netCDF variable
                            header = variables[var_name]["header"]
                               
                            # register that this model includes this variable
                            if nc.model_id not in variables[var_name]["models"]:                    
                                variables[var_name]["models"].append(nc.model_id)
                                   
                            model_count = len(variables[var_name]["models"])
                                
                            if header in variable_dates:
                                if min_date < variable_dates[header]["start_date"]:
                                    variable_dates[header]["start_date"] = min_date
                                    
                                if max_date > variable_dates[header]["end_date"]:
                                    variable_dates[header]["end_date"] = max_date   
                            else:
                                variable_dates[header] = {
                                    "start_date": min_date,
                                    "end_date": max_date
                                }
                                
                            if nc_var == None or ncVariableHasChanged(nc_var, var_name):
                                worksheet = prepareWorksheet(var_name)
                                    
                                #update global variables
                                nc_var = var_name
                            else:
                                worksheet = workbook.get_worksheet_by_name(header)
                               
                            worksheet.write(1, model_count, nc.model_id)
                               
                            break
                          
                        if count == len(variables):
                            raise ValueError("No modeled variable found.") # throw an error if no variable is found 
                        
                    # make sure the date in the data column corresponds to the date in the data to be written
                    sd = variable_dates[header]["start_date"]
                    ed = variable_dates[header]["end_date"]
                    
                    # create a date range array containing all dates in this variable's data set
                    daterange = pd.date_range(sd.strftime(pd_date_format), ed.strftime(pd_date_format))
    
                    # get all time records of variable [var_name] at indices [lat_index,lon_index]
                    var = nc.variables[var_name]
                    data = var[:, lat_index, lon_index]
                
                    conversion_string = str(variables[var_name]["conv_string"]) # get the string to be used to convert data to proper units
                            
                    # Iterate over the data and write it out row by row.
                    i = 0
                    while i < len(daterange):
                        date = daterange[i]
                        if date >= grand_start_date and date >= min_date and date <= grand_end_date and date <= max_date: # make sure the dataset data is within the range wanted                           
                            # prepare indexes
                            row = (date - grand_start_date).days
                            data_index = (date - min_date).days
                            
                            # prepare data
                            row_date = date.strftime(ws_date_format) # format date
                            row_data = float("{:.3f}".format(eval(str(data[data_index]) + conversion_string))) 
                                  
                            # write dates and data
                            worksheet.write(row + header_rows_count, 0, row_date) # write date in new row     
                            worksheet.write(row + header_rows_count, model_count, row_data)
                        i += 1         
                    nc.close()
                      
                file_count += 1
                 
                file_pt = processingTime(pt_start)
                print("    ...\n    Done! File Processing Time: {} minutes and {} seconds\n".format(file_pt["mins"], file_pt["secs"]))
    workbook.close()
    
    pt = processingTime(start_time)
    print("OPERATION SUCCESSFUL!   Total Processing time:", "{} minutes and {} seconds.".format(pt["mins"], pt["secs"]))
  
if __name__ == '__main__': main()