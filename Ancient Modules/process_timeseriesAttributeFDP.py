# 15-11-2017, Rens Meerhoff

#########################################
####### CODE TO ALWAYS BE INCLUDED ######
import sys
import subprocess
cmd_folder = str("C:\\Users\\rensm\\Dropbox\\PYTHON\\LibraryRENS")
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder) 
cmd_subfolder = str("C:\\Users\\rensm\\Dropbox\\PYTHON\\LibraryRENS\\FDP")
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

## Uncomment this line to open the function in the editor (matlab's ctrl + d)
# subprocess.call(cmd_folder + "\\callThisFunction.py", shell=True)

#########################################
#########################################

# Now you can import any function in the library folder
import csv
import pdb; #pdb.set_trace()
import numpy as np
from os.path import isfile, join, isdir
from os import listdir
from warnings import warn

# From my own library:
import plotSnapshot
import CSVexcerpt
import CSVimportAsColumns
import identifyDuplHeader
import LoadOrCreateCSVexcerpt
import individualAttributes
import plotTimeseries
import dataToDict
import dataToDict2
import safetyWarning

#########################
# USER INPUT ############
#########################

filename = 'FirstHalf_startingLineUp.csv'

firstFrameTimeseries = 453120 # in ms # if you want to change this to another unit, then also change the funtion to find the correct window (firstFrameTimeseries;  startCorrection; endCorrection )
windowTimeseries = 12000 # in ms ####### This is used to access TsMS. If you change this input, you need to match this to the window extraction below (i.e., now the code is looking for the same time in milliseconds in "TsMS", you could make it look for the same time in seconds in "TsS") * Ts = timestamp
# framerateTimeseries = 1 # in Hz (IDEA: Make it possible to have framerate higher than data framerate?)

frameRateData = 10 # in Hz (in data, possibly check it)
timeUnitData = 1/1000 # /1000 = ms, /1 = s etc.
folder = 'C:\\Users\\rensm\\Documents\\PostdocLeiden\\wetransfer-fc4772\\'
tmpFigFolder = 'C:\\Users\\rensm\\Documents\\PostdocLeiden\\wetransfer-fc4772\\Figs\\Temp\\'
mypath = folder + 'CSVexcerpts\\' # path where CSVexcerpts are stored

readRawDataCols = ['Timestamp','Timestamp','X','Y','Name','Name','PlayerRole'] # String input (case-sensitive) of headers you want to import


# Match the headers with the data type
timeHeader_Ts = 'Date/Time'
timeHeader_TsMS = 'Timestamp'
timeHeader_TsS = None

entityHeader_player = 'Name'
entityHeader_team = 'Name'
entityHeader_role = 'PlayerRole'
locationHeader = ('X','Y')

readRawDataCols = {'Time':{'Ts':timeHeader_Ts,'TsMS':timeHeader_TsMS,'TsS':timeHeader_TsS}, \
'Entity':{'PlayerID':entityHeader_player,'TeamID':entityHeader_team,'PlayerRole':entityHeader_role}, \
'Location':locationHeader}

cols2read = [timeHeader_Ts,timeHeader_TsMS,timeHeader_TsS,entityHeader_player,entityHeader_team,entityHeader_role,locationHeader[0],locationHeader[1]]


# # Match the headers with the data type
# timeHeader = 'Timestamp'
# entityHeader_player = 'Name'
# entityHeader_team = 'Name'
# entityHeader_role = 'PlayerRole'
# locationHeader = ('X','Y')

# readRawDataCols = {'Time':{timeHeader}, \
# 'Entity':{'PlayerID':{entityHeader_player},'TeamID':{entityHeader_team},'PlayerRole':{entityHeader_role}}, \
# 'Location':{locationHeader}}

# cols2read = [timeHeader,entityHeader_player,entityHeader_team,entityHeader_role,locationHeader[0],locationHeader[1]]


#########################
# END INPUT ############
#########################

# Necessary correction
firstFrameTimeseries = np.round(firstFrameTimeseries,-2) # '-2' depends on timeUnitData and frameRateData and firstFrameTimeseries (+ window)

########################################################################################
##### Load data from CSV #############################################################
########################################################################################
# 001 Create some relevant excerpts (if necessary)
tmin, tmax, filename = LoadOrCreateCSVexcerpt.idTS(filename,folder,firstFrameTimeseries,windowTimeseries)

# 002 Load data
rawData = CSVimportAsColumns.readPosData(mypath + filename,cols2read)

# 003 Identify Timestamps
# note that rawDataCols is a reference to an object, and not a hard copy of readRawDataCols
rawData = identifyDuplHeader.idTS(rawData,rawData[-2][:],filename) # From here onward, Timestamp => TsMS for time in ms or TsIM for time in inmotio time (also in ms, but not starting at 0ms)
rawData = identifyDuplHeader.idName(rawData,rawData[-1][:])

# print('Ts = ',rawData[0][0:5])
# print('TsMS = ',rawData[1][0:5])
# print('TsS = ',rawData[2][0:5])
# print('PlayerID = ',rawData[3][0:5])
# print('TeamID = ',rawData[4][0:5])
# print('PlayerRole = ',rawData[5][0:5])
# print('X = ',rawData[6][0:5])
# print('Y = ',rawData[7][0:5])
# print('dupl = ',rawData[8][0:5])
# print('(cols in data) unrequested dupl = ',rawData[-2][0:5])
# print('(cols in data) requested dupl = ',rawData[-1][:])
print('---End Loading Data---\n----------------------')

########################################################################################
####### Import existing attributes and targets #########################################
########################################################################################

# 004 Import existing attributes
readAttributeCols = ['Speed','Area','Perimeter','centroid X','centroid Y',\
'Avg dist group','LPW Ratio','Group length','Group Width']
attributeData = CSVimportAsColumns.readPosData(mypath + filename,readAttributeCols)
attributeDataCols = readAttributeCols

# Allocate 'rawData'-variables from CSV to local variables
# To do: change 'readRawDataCols' and 'readAttributeCols' into dictionary.
rawDict = dataToDict2.rawData(cols2read,rawData,readRawDataCols)

# Allocate 'attribute'-variables from CSV to local variables
# attributeDict = dataToDict.attrData(attributeDataCols,attributeData,readAttributeCols)
attributeDict = dataToDict2.attrData(readAttributeCols,attributeData)

safetyWarning.checkWindow(rawDict,firstFrameTimeseries,windowTimeseries)
print('---End Importing Attributes/Targets ---\n---------------------------------------')

########################################################################################
####### Compute new attributes #########################################################
########################################################################################

# Compute some individual attributes
attributeDict.update(individualAttributes.vNorm(rawDict))

####### CONTINUE HERE WITH ATTRIBUTE CREATION
#attributeDict.update(individualAttributes.distToGoal(rawDict)) # UNFINISHED
# TO DO: Create more individual based attributes
# TO DO: Create team based attributes
# TO DO: Compare pre-computed centroid and my own centroid

print('rawData = ',rawDict['Time'].keys(),'\n')
print('attributes = ',attributeDict.keys(),'\n')
print('---End Computing Attributes/Targets ---\n---------------------------------------')

########################################################################################
####### Export (visualization, model, data, etc.) ######################################
########################################################################################

# Export indices corresponding to player and range of interest
inds = individualAttributes.PlayerInds(rawDict,firstFrameTimeseries,windowTimeseries)

# Compare vNorm & Speed
xstring = 'Time (s)'
ystring = 'Speed (m/s)'
print(rawDict['Time']['TsS'])
plotTimeseries.PairwisePerPlayer(tmin,tmax,inds,rawDict['Time']['TsS'],attributeDict['Speed'],inds,rawDict['Time']['TsS'],attributeDict['vNorm'],xstring,ystring,tmpFigFolder)

print('---End Export ---\n-----------------')

