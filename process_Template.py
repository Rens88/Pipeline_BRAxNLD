# DRAFT
# 26-01-2018, Rens Meerhoff
# This template can be copied to use the BRAxNLD pipeline.

# Look for the lines starting with '## CHANGE THIS' to see which lines of code need to be amended to use the code locally

#########################################
####### CODE TO ALWAYS BE INCLUDED ######
import sys

import subprocess
## CHANGE THIS to the folder where you've stored the library
cmd_folder = str("C:\\Users\\rensm\\Dropbox\\PYTHON\\LibraryRENS")
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder) 
cmd_subfolder = str(cmd_folder+"\\FDP")
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)
cmd_subfolder = str(cmd_folder+"\\NP")
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
from os import listdir, path
from warnings import warn

# From my LibraryRENS:
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
import countExistingEvents
import exportCSV
import spatialAggregation
import temporalAggregation
import importTimeseriesData
import csv
#########################
# USER INPUT ############
#########################
## CHANGE THIS all these variables until 'END USER INPUT'
# String representing the different teams
TeamAstring = 'Team A'
TeamBstring = 'Team B'
# Folder that contains the data
folder = 'C:\\Users\\rensm\\Documents\\PostdocLeiden\\Nonlinear Pedagogy\\Data\\'
# Folder where any new figures will be saved
tmpFigFolder = 'C:\\Users\\rensm\\Documents\\PostdocLeiden\\Nonlinear Pedagogy\\Figs\\Temp\\'
# input of raw data, indicate at least one timestamp, entity info and Location info
timestampString = 'Video time (s) '
PlayerIDstring = 'jersey n.'
TeamIDstring = 'Team' # Optional
XPositionString = 'x'
YPositionString = 'y'
# Case-sensitive string headers of attribute columns that already exist in the data (optional)
readAttributeCols = ['Run', 'Goal ', 'Possession/Turnover ', 'Pass']
# Indicate some parameters for temporal aggregation
firstFrameTimeseries = 0
windowTimeseries = None
conversionToMeter = 111111 # https://gis.stackexchange.com/questions/8650/measuring-accuracy-of-latitude-and-longitude/8674#8674
#########################
# END USER INPUT ########
#########################

# Load all CSV files
dataFiles = [f for f in listdir(folder) if isfile(join(folder, f)) if '.csv' in f]

# Preparing the dictionary of the raw data
headers = {'Ts': timestampString,\
'PlayerID': PlayerIDstring,\
'TeamID': TeamIDstring,\
'Location': (XPositionString,YPositionString) }

attStringDict = {'Description': [],'Aggregate':[],'Window':[],'Lag':[],'Event':[]}
attStringDict['Description'][readAttributeCols[0]] = 'this should describe Run'
xstring = 'Time (s)'

if not firstFrameTimeseries == None:
	tmin = str(firstFrameTimeseries)
else:
	firstFrameTimeseries = 0
	tmin = 'start'
if not windowTimeseries == None:
	tmax = str(firstFrameTimeseries+windowTimeseries)
else:
	tmax = 'End'

for fname in dataFiles:
	###########################################
	rawDict = importTimeseriesData.rawData(fname,folder,headers,conversionToMeter)
	attributeDict = importTimeseriesData.existingAttributes(fname,folder,readAttributeCols)
	###########################################

	exportData = [str(fname[9:13]), fname[14:17]]# Always 3 letters? Always same format?
	exportDataString = ['expGroupID','expTest']
	exportFullExplanation = ['Identifier of the experimental group','Name of the type of trial (pre = pre-test, pos = post-test, tra = transfer test, ret = retention test)']

	# The first step of the analysis:
	# Prepare the variables to be exported and include some known events (NPdata specific)
	exportData,exportDataString,exportFullExplanation,targetEvents = countExistingEvents.process(rawDict,attributeDict,TeamAstring,TeamBstring,fname,folder,exportData,exportDataString,exportFullExplanation)

	########################################################################################
	####### Compute new attributes #########################################################
	########################################################################################

	# Compute some individual attributes
	attributeDict.update(individualAttributes.vNorm(rawDict))
	attributeDict = individualAttributes.correctVNorm(rawDict,attributeDict) # only necessary if Time is not continuous

	# Compute some team level attributes
	attributeDict.update(spatialAggregation.process(rawDict,tempTeamAstring,tempTeamBstring))

	# Aggregate over time. Simple does it per match. 
	exportData,exportDataString,exportFullExplanation = temporalAggregation.simple(rawDict,attributeDict,exportData,exportDataString,exportFullExplanation,tempTeamAstring,tempTeamBstring)
	# Here you can build the ijk-algorithm



	############## EXPORT THIS BEAUTY TO CSV
	outputFilename = folder + 'output.csv'
	exportCSV.newOrAdd(outputFilename,exportDataString,exportData)	
	outputFilename = folder + 'outputDescription.txt'
	exportCSV.varDescription(outputFilename,exportDataString,exportFullExplanation)


	####### Visualization
	# Plot individual attributes
	inds = individualAttributes.PlayerInds(rawDict,firstFrameTimeseries,windowTimeseries)
	XtoPlot = rawDict['Time']['TsS']
	# Plot Speed
	ystring = 'Speed (m/s)'
	stringToPlot = 'vNorm' ##### Change this to plot any other (existing) individual attribute
	stringOut = '_' + stringToPlot + '_' + fname[9:17]
	plotTimeseries.PerPlayer(tmin,tmax,inds,XtoPlot,attributeDict[stringToPlot],xstring,ystring,tmpFigFolder,stringOut)

	# Plot team level variables
	## CONTINUE HERE TO CREATE inds FOR TEAM (necessary to plot specific window for team variables)
	# inds = individualAttributes.TeamInds(rawDict,firstFrameTimeseries,windowTimeseries) # probably better to put these somewhere else

	# Plot Spread
	ystring = 'Spread (m)'
	stringToPlot = 'Spread'
	stringOut = '_' + stringToPlot + '_' + fname[9:17]
	Y1 = attributeDict[stringToPlot + 'A']
	Y2 = attributeDict[stringToPlot + 'B']	
	plotTimeseries.PairwisePerTeam(tmin,tmax,XtoPlot,Y1,Y2,xstring,ystring,tmpFigFolder,stringOut,rawDict,attributeDict)
	# Plot stdSpread
	ystring = 'stdSpread (m)'
	stringToPlot = 'stdSpread'
	stringOut = '_' + stringToPlot + '_' + fname[9:17]
	Y1 = attributeDict[stringToPlot + 'A']
	Y2 = attributeDict[stringToPlot + 'B']	
	plotTimeseries.PairwisePerTeam(tmin,tmax,XtoPlot,Y1,Y2,xstring,ystring,tmpFigFolder,stringOut,rawDict,attributeDict)
	
	# Plot Width
	ystring = 'Width (m)'
	stringToPlot = 'Width'
	stringOut = '_' + stringToPlot + '_' + fname[9:17]
	Y1 = attributeDict[stringToPlot + 'A']
	Y2 = attributeDict[stringToPlot + 'B']	
	plotTimeseries.PairwisePerTeam(tmin,tmax,XtoPlot,Y1,Y2,xstring,ystring,tmpFigFolder,stringOut,rawDict,attributeDict)

	# Plot Length
	ystring = 'Length (m)'
	stringToPlot = 'Length'
	stringOut = '_' + stringToPlot + '_' + fname[9:17]
	Y1 = attributeDict[stringToPlot + 'A']
	Y2 = attributeDict[stringToPlot + 'B']	
	plotTimeseries.PairwisePerTeam(tmin,tmax,XtoPlot,Y1,Y2,xstring,ystring,tmpFigFolder,stringOut,rawDict,attributeDict)
	
	# Plot Surface
	ystring = 'Surface (m^2)'
	stringToPlot = 'Surface'
	stringOut = '_' + stringToPlot + '_' + fname[9:17]
	Y1 = attributeDict[stringToPlot + 'A']
	Y2 = attributeDict[stringToPlot + 'B']	
	plotTimeseries.PairwisePerTeam(tmin,tmax,XtoPlot,Y1,Y2,xstring,ystring,tmpFigFolder,stringOut,rawDict,attributeDict)

	# Plot Surface
	ystring = 'sumVertices (m)'
	stringToPlot = 'sumVertices'
	stringOut = '_' + stringToPlot + '_' + fname[9:17]
	Y1 = attributeDict[stringToPlot + 'A']
	Y2 = attributeDict[stringToPlot + 'B']	
	plotTimeseries.PairwisePerTeam(tmin,tmax,XtoPlot,Y1,Y2,xstring,ystring,tmpFigFolder,stringOut,rawDict,attributeDict)

	# Plot Surface
	ystring = 'ShapeRatio (dimensionless)'
	stringToPlot = 'ShapeRatio'
	stringOut = '_' + stringToPlot + '_' + fname[9:17]
	Y1 = attributeDict[stringToPlot + 'A']
	Y2 = attributeDict[stringToPlot + 'B']	
	plotTimeseries.PairwisePerTeam(tmin,tmax,XtoPlot,Y1,Y2,xstring,ystring,tmpFigFolder,stringOut,rawDict,attributeDict)
