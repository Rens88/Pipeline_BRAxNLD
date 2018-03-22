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
# Here, you provide the string name of the student folder that you want to include.
studentFolder = 'XXcontributions' 

# Temporary inputs (whilst updating to using pandas)
debuggingMode = True # whether yo want to print the times that each script took

# dataType is used for dataset specific parts of the analysis (in the preparation phase only)
dataType =  "NP" # "FPD" or or "NP" --> so far, only used to call the right cleanup script. Long term goal would be to have a generic cleanup script

# This folder should contain a folder with 'Data'. The tabular output and figures will be stored in this folder as well.
folder = 'C:\\Users\\rensm\\Documents\\PostdocLeiden\\NP repository\\'

# String representing the different teams
# NB: not necessary for FDP (and other datasets where teamstring can be read from the filename, should be done in discetFilename.py)
TeamAstring = 'Team A'
TeamBstring = 'Team B'

# Input of raw data, indicate at least timestamp, entity and Location info
timestampString = 'Video time (s)'					#'enter the string in the header of the column that represents TIMESTAMP' 	# 'Video time (s)'
PlayerIDstring = 'jersey n.'						#'enter the string in the header of the column that represents PLAYERID' 	# 'jersey n.'
TeamIDstring = 'Team' 								#'enter the string in the header of the column that represents TEAMID' 			# Optional
XPositionString = 'x' 								#'enter the string in the header of the column that represents X-POSITION'			# 'x'
YPositionString = 'y' 								#'enter the string in the header of the column that represents Y-POSITION'			# 'y'

# Case-sensitive string rawHeaders of attribute columns that already exist in the data (optional). NB: String also sensitive for extra spaces.
readAttributeCols = []
attrLabel = {} 

# When event columns exist in the raw data, they can be read to export an event file
readEventColumns = ['Run', 'Goal', 'Possession/Turnover', 'Pass']

# If the raw data is not given in meters, provide the conversion.
conversionToMeter = 111111 # https://gis.stackexchange.com/questions/8650/measuring-accuracy-of-latitude-and-longitude/8674#8674

## -- work in progress -- 
# Indicate some parameters for temporal aggregation: 'Full' aggregates over the whole file, any other event needs to be specified with the same string as in the header of the CSV file.
aggregateEvent = 'Goals' # Event that will be used to aggregate over ('full' denotes aggregating over the whole file. 'None' denotes skipping the temporal aggregation)
aggregateWindow = 10 # in seconds #NB: still need to write warning in temporal aggregation in case you have Goals in combination with None.
aggregateLag = 0 # in seconds

# This (simple) visualization plots every outcome variable for the given window for the temporal aggregation
Visualization = False # True = includes visualization, False = skips visualization

# Key events (TO DO)
# - Load existing events
# - Include modules to compute events
## -- \work in progress -- 

# Parts of the pipeline can be skipped
skipCleanup = True # Only works if cleaned file exists
skipSpatAgg = True # Only works if spat agg export exists

#########################
# END USER INPUT ########
#########################


#########################
# INITIALIZATION ########
#########################

# Gerenal Python modules
# To do: I'm pretty sure these functions can be loaded automatically using _init_.py
# To do: convert to Python package?

import initialization
# In this module, the library is added to the system path. 
# This allows Python to import the custom modules in our library. 
# If you add new subfolders in the library, they need to be added in addLibary (in initialization.py) as well.
initialization.addLibrary(studentFolder)
dataFolder,tmpFigFolder,outputFolder,cleanedFolder,spatAggFolder,aggregatedOutputFilename,outputDescriptionFilename =\
initialization.checkFolders(folder,aggregateEvent)

import pdb; #pdb.set_trace()
from os.path import isfile, join#, isdir, exists
from os import listdir#, path, makedirs
from warnings import warn
# Custom modules (from LibrarRENS)
import spatialAggregation
import temporalAggregation
import importEvents
import dissectFilename
import importTimeseries_aspanda
import cleanupData
import pandas as pd
import exportCSV
import estimateRemainingTime
import plotTimeseries
#  Unused modules: 
# CSVexcerpt CSVimportAsColumns identifyDuplHeader LoadOrCreateCSVexcerpt individualAttributes plotTimeseries dataToDict 
# dataToDict2 safetyWarning countExistingEvents exportCSV importTimeseriesData csv importEvents CSVtoDF plotSnapshot

## These lines should be embedded elsewhere in the future.
# Preparing the dictionary of the raw data (NB: With the use of Pandas, this is a bit redundant)
rawHeaders = {'Ts': timestampString,\
'PlayerID': PlayerIDstring,\
'TeamID': TeamIDstring,\
'Location': (XPositionString,YPositionString) }

xstring = 'Time (s)'
aggregateLevel = (aggregateEvent,aggregateWindow,aggregateLag)

#########################
# ANALYSIS (file by file)
#########################
# Load all (not yet cleaned) files
DirtyDataFiles = [f for f in listdir(dataFolder) if isfile(join(dataFolder, f)) if '.csv' in f]
t = ([],0,len(DirtyDataFiles))#(time started,nth file,total number of files)

for dirtyFname in DirtyDataFiles:
	print(	'\nFILE: << %s >>' %dirtyFname[:-4])
	t = estimateRemainingTime.printProgress(t)

	#########################
	# PREPARATION ###########
	#########################
	# IMPORTANT: During preparation you can use 'dataType' (although it's better to try not to) which allows you
	# to prepare the data in a way that is specific for your dataset.
		
	# Prepare metadata of aggregated data to be exported:
	exportData, exportDataString, exportDataFullExplanation,cleanFname,spatAggFname,TeamAstring,TeamBstring = \
	dissectFilename.process(dirtyFname,dataType,TeamAstring,TeamBstring)

	# Clean cleanFname (it only cleans data if there is no existing cleaned file of the current (dirty)file )
	loadFolder,loadFname,fatalTimeStampIssue,skipSpatAgg_curFile = \
	cleanupData.process(dirtyFname,cleanFname,dataType,dataFolder,cleanedFolder,spatAggFname,spatAggFolder,TeamAstring,TeamBstring,rawHeaders,readAttributeCols,timestampString,readEventColumns,conversionToMeter,skipCleanup,skipSpatAgg,debuggingMode)

	if fatalTimeStampIssue:
		skippedData = True
		exportCSV.newOrAdd(aggregatedOutputFilename,exportDataString,exportData,skippedData)	
		continue
	# From now onward, rawData contains:
	#  'Ts' --> Timestamp
	#  'X' --> X-position
	#  'Y' --> Y-position
	#  'PlayerID' --> Player identification. NB: Ball-rows should be 'ball' and Match-rows should be 'groupRow' (to indicate CentroidTeamA)
	#  'TeamID' --> Team idenfitification

	###### Work in progress ##########
	# clean target events ('StringEvent', tStart, tEnd)
	# cleanupEvents.process()
	###### \Work in progress #########

	########################################################################################
	####### Import existing data ###########################################################
	########################################################################################
	
	rawPanda = importTimeseries_aspanda.rawData(loadFname,loadFolder)
	attrPanda,attrLabel = importTimeseries_aspanda.existingAttributes(loadFname,loadFolder,skipSpatAgg_curFile,readAttributeCols,attrLabel,outputFolder)
	eventsPanda,eventsLabel = importTimeseries_aspanda.existingAttributes(loadFname,loadFolder,False,readEventColumns,attrLabel,outputFolder)

	###### Work in progress ##########
	# Currently code is not very generic. It should work for NP though..
	targetEventsImported = importEvents.process(eventsPanda,TeamAstring,TeamBstring)
	###### \Work in progress #########

	########################################################################################
	####### Compute new attributes #########################################################
	########################################################################################

	attrPanda,attrLabel = spatialAggregation.process(rawPanda,attrPanda,attrLabel,TeamAstring,TeamBstring,skipSpatAgg_curFile,debuggingMode)

	###### Work in progress ##########
	# computeEvents.process()
	###### \Work in progress #########

	## Temporal aggregation
	exportData,exportDataString,exportFullExplanation = \
	temporalAggregation.process(targetEventsImported,aggregateLevel,rawPanda,attrPanda,exportData,exportDataString,exportDataFullExplanation,TeamAstring,TeamBstring,debuggingMode)

	########################################################################################
	####### EXPORT to CSV ##################################################################
	########################################################################################
	
	# Temporally aggregated data
	skippedData = False
	exportCSV.newOrAdd(aggregatedOutputFilename,exportDataString,exportData,skippedData)	
	exportCSV.varDescription(outputDescriptionFilename,exportDataString,exportFullExplanation)

	# Spatially aggregated data
	spatAgg = pd.concat([rawPanda, eventsPanda, attrPanda], axis=1) # debugging only
	spatAgg.to_csv(spatAggFolder + spatAggFname) # debugging only		

	## EDIT: Instead of exporting the attributes labels, 
	## it's easier to create the attribute lables, 
	## EVEN if spatAgg is being skipped.
	# # Need to save the attribute labels for when skipping spatAgg
	# if t[1] == 1: # only after the first file
	# 	attrLabel_asPanda = pd.DataFrame.from_dict([attrLabel],orient='columns')
	# 	attrLabel_asPanda.to_csv(outputFolder + 'attributeLabel.csv') 

	if not Visualization: # stop early if visualization is FALSE
		continue
	########################################################################################
	####### Visualization  #################################################################
	########################################################################################

	# printTheseAttributes = [('LengthA','LengthB'),('WidthA','WidthB'),('SurfaceA','SurfaceB'),('SpreadA','SpreadB')] # teams that need to be compared as tuple
	printTheseAttributes = ['vNorm',('LengthA','LengthB'),('SurfaceA','SurfaceB'),('SpreadA','SpreadB'),('WidthA','WidthB')]
	plotTimeseries.process(printTheseAttributes,aggregateLevel,targetEventsImported,rawPanda,attrPanda,attrLabel,tmpFigFolder,cleanFname[:-4],TeamAstring,TeamBstring,debuggingMode)