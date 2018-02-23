# 13-11-2017, Rens Meerhoff
# Re-write time-series file to only include a subset of time.
#
# Ideas
# - add warning overwriting file
# - add warning missing file
# - add warning different file configuration (different headers)
# - add warning different framerate

import csv
import numpy as np
from numpy import *
import pdb; #pdb.set_trace()
import math
import os
from warnings import warn
import CSVimportAsColumns
import identifyDuplHeader

if __name__ == '__main__':
    # filenameOriginal: Filename original file (including path)
    # firstframe: First frame (in milliseconds) that is included in the excerpt
    # window: How many milliseconds after firstframe the excerpt should end
    execFunction(filenameOriginal,folder,firstframe,window)
    # RETURNS:
    # - newFilename (without .csv extension)

def execFunction(filenameOriginal,folder,firstframe,window): 
    # Window in numbers (and round to nearest 100)
    if float(window) < 500:
        window = 500
    tMin = int(round(float(firstframe),-2))
    tMax = tMin + int(round(float(window),-2))

    # Add csv to filename if necessary
    if filenameOriginal[-4:] != '.csv':
        filenameOriginal = filenameOriginal + '.csv'

    # Generate new filename
    newFilename = filenameOriginal[:-4] + '_' + str(tMin) + 'ms_' + str(tMax) + 'ms' + '.csv'

    # Import data
    myData,cols = CSVimportAsColumns.readPosData(folder + filenameOriginal,"ImportAll")
    colsTSidentified = identifyDuplHeader.idTS(myData,cols.copy(),filenameOriginal)
    # Identify TsMS
    for idx,val in enumerate(colsTSidentified):
        if val == 'TsMS':
            TsMS = myData[idx]

    # Create new folder if necessary
    if folder[-12:] == 'CSVexcerpts\\':
        newFolder = folder
    else:
        newFolder = folder + 'CSVexcerpts\\'
    if not os.path.exists(newFolder):
        os.makedirs(newFolder)

    # Open the new CSV file (NB, this won't work if it's still in use by another application)
    myFile = open(newFolder + newFilename,'w',newline = '')  

    ncols = len(myData) # Number of columns
    perRow = []
    dataToWrite = []

    # Copy the header
    for j in range(ncols):
        perRow.append(cols[j])
    dataToWrite.append(perRow)

    # Create data to write to csv based on window only
    for i,val in enumerate(TsMS):      # For every ith time stamp in milliseconds
        if float(TsMS[i]) >= tMin and float(TsMS[i]) <= tMax:        # Look for all TsMS in that range
            perRow = []
            for j in range(ncols):
                perRow.append(myData[j][i])
            dataToWrite.append(perRow)
    if float(TsMS[-1]) < tMax:
        warn('\nPart of window out of reach. \
            \nHighest time found was %s\n' %TsMS[-1])
    if float(TsMS[0]) > tMin:
        warn('\nPart of window out of reach. \
            \nSmallest time found was %s\n' %TsMS[0])        
                
    # Write to CSV
    with myFile:  
       writer = csv.writer(myFile)
       writer.writerows(dataToWrite)
    myFile.close()

    return(newFilename)