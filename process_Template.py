# If you want to edit something in the code and you're not sure where it is, 
# just ask. l.a.meerhoff@liacs.leidenuniv.nl
# Also, if you want to add something to the code and you're not sure where, 
# it's probably going to be somewhere in spatialAggregation. But you can always ask.

# 02-03-2018 Rens Meerhoff
# Updated the code to now have relative folders to load the library.
# Working on incorporating panda dataframes instead of lists of lists.
# - left it at 'cleanupData' where I'm splitting 'Naam'
# on 05-03 I will continue with further incorporating pandas.

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
# "FPD" or or "NP" --> so far, only used to call the right cleanup script. Long term goal would be to have a generic cleanup script
dataType =  "FDP"

# This folder should contain a folder with 'Data' and will be used to export the results (including figures)
# NB: Data in 'Data' folder will first be cleaned. If data is already clean, put the cleaned data in 'Data\\Cleaned'
# TO do: better explain folder hierarchy
folder = 'C:\\Users\\rensm\\Documents\\PostdocLeiden\\BRAxNLD repository\\'
"""
TO DO FLORIS: Insert folder with cleaned data by enabling DataFrame to csv output after cleaning
"""

# String representing the different teams
TeamAstring = 'Provide the string that represents one team' # NB: not necessary for FDP (and other datasets where teamstring can be read from the filename, should be done in discetFilename.py)
TeamBstring = 'Provide the string that represents the other team'

# Input of raw data, indicate at least timestamp, entity and Location info
timestampString = 'Timestamp' 						#'enter the string in the header of the column that represents TIMESTAMP' 	# 'Video time (s)'
PlayerIDstring = 'Naam' 							#'enter the string in the header of the column that represents PLAYERID' 	# 'jersey n.'
TeamIDstring = None 								#'enter the string in the header of the column that represents TEAMID' 			# Optional
XPositionString = 'X' 								#'enter the string in the header of the column that represents X-POSITION'			# 'x'
YPositionString = 'Y' 								#'enter the string in the header of the column that represents Y-POSITION'			# 'y'
# Case-sensitive string rawHeaders of attribute columns that already exist in the data (optional). NB: String also sensitive for extra spaces.
readAttributeCols = ['Snelheid','Acceleration'] 	#['Here', 'you', 'can', 'proivde a list of strings that represent existing attributes.']
attrLabel = {readAttributeCols[0]: 'Speed (m/s)',readAttributeCols[1]: 'Acceleration (m/s^2)'} 

# Indicate some parameters for temporal aggregation: 'Full' aggregates over the whole file, any other event needs to be specified with the same string as in the header of the CSV file.
aggregateEvent = 'Full' # Event that will be used to aggregate over (verified for 'Goals' and for 'Possession')
aggregateWindow = 10 # in seconds #NB: still need to write warning in temporal aggregation in case you have Goals in combination with None.
aggregateLag = 0 # in seconds

conversionToMeter = 1 #111111 # https://gis.stackexchange.com/questions/8650/measuring-accuracy-of-latitude-and-longitude/8674#8674
# To do:
# - add option to automatically detect a specific event
# - add option to insert events manually
# - add possibility to pre-define aggregation method? (avg, std, etc)

Visualization = False # True = includes visualization, False = skips visualization

# ALSO:
# see 'cleanFname' in the main for loop. It is used to obtain the metadata. This is project specific and may need to be changed.

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
import re

####### Adding the library ######
# The folder and relevant subfolders where you store the python library with all the custom modules.
current_folder = path.realpath(path.abspath(path.split(inspect.getfile( inspect.currentframe() ))[0]))
library_folder = current_folder + str("\\LibraryRENS")
library_subfolders = [library_folder + str("\\FDP")] # FDP = Football Data Project
library_subfolders.append(library_folder + str("\\VPcontributions")) # Contributions of a Bachelor student
library_subfolders.append(library_folder + str("\\LTcontributions")) # Contributions of a Bachelor student

if library_folder not in sys.path:
	sys.path.insert(0, library_folder) 
for subf in library_subfolders:
	if subf not in sys.path:
		sys.path.insert(0, subf) # idea: could loop over multiple subfolders and enter as a list

## Uncomment this line to open the function in the editor (matlab's ctrl + d)
# subprocess.call(cmd_folder + "\\callThisFunction.py", shell=True)
#########################################

# From LibraryRENS:
# To do: I'm pretty sure these functions can be loaded automatically using _init_.py
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
import disectFilename
import importTimeseries_aspanda

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

# Preparing the dictionary of the raw data
rawHeaders = {'Ts': timestampString,\
'PlayerID': PlayerIDstring,\
'TeamID': TeamIDstring,\
'Location': (XPositionString,YPositionString) }

readAttributeCols = ['Ts'] + readAttributeCols # This makes sure that timeStamp is also imported in attribute cols, necessary for pivoting etc.
attrLabel.update({'Ts': 'Time (s)'})

xstring = 'Time (s)'
aggregateLevel = (aggregateEvent,aggregateWindow,aggregateLag)

#########################
# ANALYSIS (file by file)
#########################
# Load all (not yet cleaned) files
DirtyDataFiles = [f for f in listdir(dataFolder) if isfile(join(dataFolder, f)) if '.csv' in f]
for dirtyFname in DirtyDataFiles:
	print('\nFILE: << %s >>' %dirtyFname[:-4])

	# Prepare metadata of aggregated data to be exported:
	exportData, exportDataString, exportDataFullExplanation,cleanFname,TeamAstring,TeamBstring = \
	disectFilename.process(dirtyFname,dataType,TeamAstring,TeamBstring)

	# Clean cleanFname (if necessary)
	cleanedFolder = \
	cleanupData.process(dirtyFname,cleanFname,dataType,dataFolder,cleanedFolder,TeamAstring,TeamBstring,rawHeaders,readAttributeCols)
	# From now onward:
	#  'Ts' --> Timestamp
	#  'X' --> X-position
	#  'Y' --> Y-position
	#  'PlayerID' --> Player identification. NB: Ball-rows should be 'ball' and Match-rows should be 'groupRow' (to indicate CentroidTeamA)
	#  'TeamID' --> Team idenfitification

	########################################################################################
	####### Import existing data ###########################################################
	########################################################################################
	# NB: I might have to update this script to deal with varying timestamps and players that are nog on the court for every timestamp
	rawPanda = importTimeseries_aspanda.rawData(cleanFname,cleanedFolder)
	attrPanda = importTimeseries_aspanda.existingAttributes(cleanFname,cleanedFolder,readAttributeCols)

	###### Work in progress ##########
	attrPanda,attrLabel = spatialAggregation.process(rawPanda,attrPanda,attrLabel,TeamAstring,TeamBstring)
	###### Work in progress ##########
	print(rawPanda.keys())
	print(attrPanda.keys())
	pdb.set_trace()

	"""
	The 3 lines below read and clean the csv file with LPM data as a pandas DataFrame and save the result again as a csv file. 
	"""
	RawPos_df = CSVtoDF.LoadPosData(cleanedFolder + cleanFname)
	outputFilename = outputFolder + 'output_' + aggregateLevel[0] + '.csv'
	RawPos_df.to_csv(outputFilename)
	
	rawDict,timestampIssues = importTimeseriesData.rawData(cleanFname,cleanedFolder,rawHeaders,conversionToMeter)
	if timestampIssues:
		skippedData = True
		outputFilename = outputFolder + 'output_' + aggregateLevel[0] + '.csv'
		exportCSV.newOrAdd(outputFilename,exportDataString,exportData,skippedData)	
		continue
	attributeDict,attributeLabel = importTimeseriesData.existingAttributes(cleanFname,cleanedFolder,readAttributeCols)

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
	plotTimeseries.PairwisePerTeam2(printTheseAttributes,aggregateLevel,targetEvents,rawDict,attributeDict,attributeLabel,tmpFigFolder,cleanFname[:-4])