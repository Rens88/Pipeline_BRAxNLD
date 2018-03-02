# If you want to edit something in the code and you're not sure where it is, 
# just ask.
# Also, if you want to add something to the code and you're not sure where, 
# it's probably going to be somewhere in spatialAggregation. But you can always ask.

# 08-02-2018 Rens Meerhoff
# Updated code to be able to indicate the window for temporal aggregation.
# It currently uses some attributes from the data (if available).
# It should always be able to do 'full' (aggregates whole trial)
# The information is imported in << targetEvents >>
# targetEvents can hold information about 'Goals', 'Passes', and 'Possession' in a dictionary with the same names.
# One could add events to this variable manually, most goals has the format (<time of event>,<team>), 
# passes (<time of event>,<team>,<nth possession it occurs in>) and for possession (<tstart>,<tEnd>,<team>)
#
#
# To do:
# - Verify outcomes (by looking at counts)
# - Double check all warnings
# - Adapt code for easy manual incorporation of targetEvents
# - Run code for LPM data
# - Clean script (and go through to dos, ideas)
# - Make folder string indepedent of location
#
# - re-incorporate create excerpt

# 26-01-2018, Rens Meerhoff
# This template can be copied to use the BRAxNLD pipeline.

# Look for the lines starting with 
# '## CHANGE THIS'
# '## TO DO'
# '## Idea'
# to see which lines of code need to be amended to use the code locally


#########################
# USER INPUT ############
#########################
## CHANGE THIS all these variables until 'END USER INPUT'

# String representing the different teams
TeamAstring = 'AA1001'
TeamBstring = 'AA1003'
# This folder should contain a folder with 'Data' and will be used to export the results (including figures)
# NB: Data in 'Data' folder will first be cleaned. If data is already clean, put the cleaned data in 'Data\\Cleaned'
# TO do: better explain folder hierarchy
folder = 'C:\\Users\\rensm\\Documents\\PostdocLeiden\\BRAxNLD repository\\'
"""
TO DO FLORIS: Insert folder with cleaned data by enabling DataFrame to csv output after cleaning
"""
# Input of raw data, indicate at least timestamp, entity and Location info
timestampString = 'enter the string in the header of the column that represents TIMESTAMP' 	# 'Video time (s)'
PlayerIDstring = 'enter the string in the header of the column that represents PLAYERID' 	# 'jersey n.'
TeamIDstring = 'enter the string in the header of the column that represents TEAMID' 			# Optional
XPositionString = 'enter the string in the header of the column that represents X-POSITION'			# 'x'
YPositionString = 'enter the string in the header of the column that represents Y-POSITION'			# 'y'
# Case-sensitive string headers of attribute columns that already exist in the data (optional). NB: String also sensitive for extra spaces.
readAttributeCols = ['Here', 'you', 'can', 'proivde a list of strings that represent existing attributes.']

# Indicate some parameters for temporal aggregation: 'Full' aggregates over the whole file, any other event needs to be specified with the same string as in the header of the CSV file.
aggregateEvent = 'Goals' # Event that will be used to aggregate over (verified for 'Goals' and for 'Possession')
aggregateWindow = 10 # in seconds #NB: still need to write warning in temporal aggregation in case you have Goals in combination with None.
aggregateLag = 0 # in seconds

conversionToMeter = 111111 # https://gis.stackexchange.com/questions/8650/measuring-accuracy-of-latitude-and-longitude/8674#8674
# To do:
# - add option to automatically detect a specific event
# - add option to insert events manually
# - add possibility to pre-define aggregation method? (avg, std, etc)

Visualization = True # True = includes visualization, False = skips visualization

# ALSO:
# see 'fname' in the main for loop. It is used to obtain the metadata. This is project specific and may need to be changed.

#########################
# END USER INPUT ########
#########################






#########################
# PREPARATION ###########
#########################

# Gerenal Python modules
import csv
import pdb; #pdb.set_trace()
import numpy as np
from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs
from warnings import warn
import sys, inspect
import subprocess
import pandas as pd

####### Adding the library ######
# The folder and relevant subfolders where you store the python library with all the custom modules.
current_folder = path.realpath(path.abspath(path.split(inspect.getfile( inspect.currentframe() ))[0]))
library_folder = current_folder + str("\\LibraryRENS")
library_subfolder1 = library_folder + str("\\FDP") # FDP = Football Data Project

if library_folder not in sys.path:
	sys.path.insert(0, library_folder) 
if library_subfolder1 not in sys.path:
	sys.path.insert(0, library_subfolder1) # idea: could loop over multiple subfolders and enter as a list

## Uncomment this line to open the function in the editor (matlab's ctrl + d)
# subprocess.call(cmd_folder + "\\callThisFunction.py", shell=True)
#########################################

# From LibraryRENS:

import CSVexcerpt
import CSVimportAsColumns
import identifyDuplHeader
import LoadOrCreateCSVexcerpt
import individualAttributes # obsolete?
import plotTimeseries
import dataToDict # obsolete?
import dataToDict2
import safetyWarning
import countExistingEvents # obsolete?
import exportCSV
import spatialAggregation
import temporalAggregation
import importTimeseriesData
import csv
import cleanupData
import importEvents
import CSVtoDF
import plotSnapshot

if folder[-1:] != '\\':
	warn('\n<folder> did not end with <\\\\>. \nOriginal input <%s>\nReplaced with <%s>' %(folder,folder+'\\'))
	folder = folder + '\\'

dataFolder = folder + 'Data\\'
tmpFigFolder = folder + 'Figs\\Temp\\' + aggregateEvent + '\\'
outputFolder = folder + 'Output\\' # Folder where tabular output will be stored (aggregated spatially and temporally)
cleanedFolder = dataFolder + 'Cleaned\\'    

# Verify if folders exists
if not exists(dataFolder):
	warn('\nWARNING: dataFolder not found.')
	exit()
if not exists(outputFolder):
	makedirs(outputFolder)
if not exists(tmpFigFolder):
	makedirs(tmpFigFolder)
if not exists(cleanedFolder):
	makedirs(cleanedFolder)

# Load all CSV files
if len(listdir(cleanedFolder)) == 0:# no cleaned data created, so let's create it
	# NB: cleanupData currently only adjusted for NP data. Fixes are quite specific and may not easily transfer to different datasets.
	DirtyDataFiles = [f for f in listdir(dataFolder) if isfile(join(dataFolder, f)) if '.csv' in f]
	cleanupData.NP(DirtyDataFiles,dataFolder,cleanedFolder,TeamAstring,TeamBstring)
	warn('\nCleaned the data with cleanupData.py. NB: May need revision.')
else:
	warn('\nContinued with previously cleaned data.\nIf problems exist with data consistency, consider writing a function in cleanupData.py.')

dataFiles = [f for f in listdir(cleanedFolder) if isfile(join(cleanedFolder, f)) if '.csv' in f]
# Preparing the dictionary of the raw data
headers = {'Ts': timestampString,\
'PlayerID': PlayerIDstring,\
'TeamID': TeamIDstring,\
'Location': (XPositionString,YPositionString) }

xstring = 'Time (s)'
aggregateLevel = (aggregateEvent,aggregateWindow,aggregateLag)

#########################
# ANALYSIS (file by file)
#########################
for fname in dataFiles:
	print('\nFILENAME: << %s >>' %fname)

	# Prepare metadata of aggregated data to be exported:
	# NB: Filetype specific. Needs to be generalized
	School = fname[0:4]
	Class = fname[5:8]
	Group = fname[9:12]
	Test = fname[13:16]

	exportData = [School, Class, Group, Test]
	exportDataString = ['School', 'Class', 'Group', 'Test']
	exportFullExplanation = ['School experiment was held at','Class the participants were from','Identifier groups that played each other','Name of the type of trial (PRE = pre-test, POS = post-test, TRA = transfer test, RET = retention test)']

	########################################################################################
	####### Import existing data ###########################################################
	########################################################################################
	# NB: I might have to update this script to deal with varying timestamps and players that are nog on the court for every timestamp
	"""
	The 3 lines below read and clean the csv file with LPM data as a pandas DataFrame and save the result again as a csv file. 
	"""
	RawPos_df = CSVtoDF.LoadPosData(fname)
	outputFilename = outputFolder + 'output_' + aggregateLevel[0] + '.csv'
	RawPos_df.to_csv(outputFilename)
	
	rawDict,timestampIssues = importTimeseriesData.rawData(fname,cleanedFolder,headers,conversionToMeter)
	if timestampIssues:
		skippedData = True
		outputFilename = outputFolder + 'output_' + aggregateLevel[0] + '.csv'
		exportCSV.newOrAdd(outputFilename,exportDataString,exportData,skippedData)	
		continue
	attributeDict,attributeLabel = importTimeseriesData.existingAttributes(fname,cleanedFolder,readAttributeCols)

	# TO DO: Check 'CHECK THIS' in importEvents, I think it's slow and possibly not even correct
	targetEvents = importEvents.process(rawDict,attributeDict,TeamAstring,TeamBstring)
	# TO DO: Write checklist to make sure data includes: (use safetyWarning.py)
	# - TsS
	# - PlayerIDs and TeamIDs (specifically in the format of being empty or not)
	# - verify terminology ragetEvents corresponds to terminology aggregateEvent (and thus aggregateLevel)

	########################################################################################
	####### Compute new attributes #########################################################
	########################################################################################
	# Spatial aggregation, both at individual and team-level
	attributeDict,attributeLabel = spatialAggregation.process(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)
	# Idea: Could write a script to automatically detect targetEVents?

	###########################################
	# Temporal aggregation, where aggregated data is stored in exportData (to export to CSV later)
	# TO DO: seperate aggregation method
	# Idea: Here you can build the ijk-algorithm
	# TO DO: I'm not yet exporting the averages of team centroid (perhaps not informative)
	exportData,exportDataString,exportFullExplanation = \
	temporalAggregation.process(targetEvents,aggregateLevel,rawDict,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring)
	
	########################################################################################
	####### EXPORT to CSV #########################################################
	########################################################################################
	## TO DO: include a better way to skip data when necessary
	skippedData = False
	outputFilename = outputFolder + 'output_' + aggregateLevel[0] + '.csv'
	exportCSV.newOrAdd(outputFilename,exportDataString,exportData,skippedData)	
	outputFilename = outputFolder + 'outputDescription_' + aggregateLevel[0] + '.txt'
	exportCSV.varDescription(outputFilename,exportDataString,exportFullExplanation)
	# TO DO
	# - Solve skipped data issue --> so far: put skipped data as 'None' and replace with 'NaN' before exporting to CSV.
	# - Check some obvious errors with identifying events (and exporting characteristics, e.g., negative duration)
	# - clean up template
	# - generalize export to have filename outside of for loop and write new filename unless specified to overwrite

	if not Visualization: # stop early if visualization is FALSE
		continue
	########################################################################################
	####### Visualization  #################################################################
	########################################################################################
	# To do:
	# - make the plots pretty
	# - add a legend
	# - find a better way to formate labels and title
	# - allow for plotting individual variables
	printTheseAttributes =[('TeamCentXA','TeamCentXB'),('SpreadA','SpreadB')] # teams that need to be compared as tuple
	plotTimeseries.PairwisePerTeam2(printTheseAttributes,aggregateLevel,targetEvents,rawDict,attributeDict,attributeLabel,tmpFigFolder,fname[:-4])