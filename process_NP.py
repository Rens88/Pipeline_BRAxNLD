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
aggregateEvent = 'Goal' # Event that will be used to aggregate over ('full' denotes aggregating over the whole file. 'None' denotes skipping the temporal aggregation)
aggregateWindow = 10 # in seconds #NB: still need to write warning in temporal aggregation in case you have Goals in combination with None.
aggregateLag = 0 # in seconds

# Key events (TO DO)
# - Load existing events
# - Include modules to compute events
## -- \work in progress -- 

# Parts of the pipeline can be skipped
skipCleanup = True # Only works if cleaned file exists
skipSpatAgg = True # Only works if spat agg export exists
# skipEventAgg = True # TO DO: implement a 'skipEventAgg', which needs to 'read' whether the file already exists in the eventExcerpt_window(X)_lag(X).csv

appendEventAggregate = False # Choose between append (= True) or overwrite (= False) (the first time around only of course) the existing (if any) eventAggregate CSV.

# This (simple) visualization plots every outcome variable for the given window for the temporal aggregation
Visualization = False # True = includes visualization, False = skips visualization

# Strings need to correspond to outcome variables (dict keys). 
# Individual level variables ('vNorm') should be included as a list element.
# Group level variables ('LengthA','LengthB') should be included as a tuple (and will be plotted in the same plot).
plotTheseAttributes = ['vNorm',('SurfaceA','SurfaceB')]#,'LengthB',('LengthA','LengthB'),('SurfaceA','SurfaceB'),('SpreadA','SpreadB'),('WidthA','WidthB')] # [('LengthA','LengthB'),('WidthA','WidthB'),('SurfaceA','SurfaceB'),('SpreadA','SpreadB')] # teams that need to be compared as tuple

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
aggregateLevel = (aggregateEvent,aggregateWindow,aggregateLag)
dataFolder,tmpFigFolder,outputFolder,cleanedFolder,spatAggFolder,eventAggFolder,aggregatedOutputFilename,outputDescriptionFilename,eventAggFname =\
initialization.checkFolders(folder,aggregateLevel)

import pdb; #pdb.set_trace()
from os.path import isfile, join, exists#, isdir, exists
from os import listdir, stat#, path, makedirs
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
import computeEvents
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
	# if t[1] == 1:
	# 	eventsPanda = eventsPanda.drop('Goal', axis = 1)
	targetEventsImported = importEvents.process(eventsPanda,TeamAstring,TeamBstring)
	###### \Work in progress #########

	########################################################################################
	####### Compute new attributes #########################################################
	########################################################################################

	attrPanda,attrLabel = spatialAggregation.process(rawPanda,attrPanda,attrLabel,TeamAstring,TeamBstring,skipSpatAgg_curFile,debuggingMode)

	###### Work in progress ##########
	targetEvents = \
	computeEvents.process(targetEventsImported,aggregateLevel,rawPanda,attrPanda,eventsPanda,TeamAstring,TeamBstring,debuggingMode)
	###### \Work in progress #########

	## Temporal aggregation
	exportData,exportDataString,exportFullExplanation,eventExcerptPanda = \
	temporalAggregation.process(targetEvents,aggregateLevel,rawPanda,attrPanda,exportData,exportDataString,exportDataFullExplanation,TeamAstring,TeamBstring,debuggingMode,spatAggFolder + spatAggFname)

	########################################################################################
	####### EXPORT to CSV ##################################################################
	########################################################################################
	
	# Temporally aggregated data
	skippedData = False
	exportCSV.newOrAdd(aggregatedOutputFilename,exportDataString,exportData,skippedData)	
	exportCSV.varDescription(outputDescriptionFilename,exportDataString,exportFullExplanation)

	# Spatially aggregated data
	spatAggPanda = pd.concat([rawPanda, eventsPanda.loc[:, eventsPanda.columns != 'Ts'], attrPanda.loc[:, attrPanda.columns != 'Ts']], axis=1) # Skip the duplicate 'Ts' columns
	spatAggPanda.to_csv(spatAggFolder + spatAggFname) # debugging only		

	# Spatially aggregated data per event
	# If it exists, if it is requested, and if the file is not empty --> skip the header and append
	# Otherwise, overwrite.
	# print('1')
	# print('Does it exist?')
	# print(exists(eventAggFolder + eventAggFname))
	# print('Is append requested?')
	# print(appendEventAggregate)
	# print('Is the file not empty?')
	# print(stat(eventAggFolder + eventAggFname).st_size)

	if exists(eventAggFolder + eventAggFname) and appendEventAggregate:# and not stat(eventAggFolder + eventAggFname).st_size == 0: 
		# print('2asdfasdf')
		with open(eventAggFolder + eventAggFname,'a') as f:			
			eventExcerptPanda.to_csv(f,header=False)
		# print('!!!!!!!!!!!!')
		# pdb.set_trace()
	elif not eventExcerptPanda.empty:
		
		eventExcerptPanda.to_csv(eventAggFolder + eventAggFname)
		appendEventAggregate = True
		
	else: # apparently eventExcerptPanda was empty..
		warn('\nWARNING: Targetevents were empty. \nNo Data exported.\n')

	# if t[1] == 3:
	# 	pdb.set_trace()
	if not Visualization: # stop early if visualization is FALSE
		continue
	########################################################################################
	####### Visualization  #################################################################
	########################################################################################
	
	# It's not that elaborate, but functional enough to get an idea whether the computed outcome variables are correct
	# plotTimeseries.process(plotTheseAttributes,aggregateLevel,targetEvents,rawPanda,attrPanda,attrLabel,tmpFigFolder,cleanFname[:-4],TeamAstring,TeamBstring,debuggingMode)
	plotTimeseries.process(plotTheseAttributes,aggregateLevel,targetEvents,eventExcerptPanda,attrLabel,tmpFigFolder,cleanFname[:-4],TeamAstring,TeamBstring,debuggingMode)
	# import plotTimeseries2
	# print('\nthe old plot')
	# plotTimeseries2.process(plotTheseAttributes,aggregateLevel,targetEvents,rawPanda,attrPanda,attrLabel,tmpFigFolder,cleanFname[:-4],TeamAstring,TeamBstring,debuggingMode)

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!!!! THIS IS WHERE YOU LEFT IT!!!
# And then: create overall plot.
# !!!!! THIS IS WHERE YOU LEFT IT!!!
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

pdb.set_trace()
pdb.set_trace()

eventExcerptPanda.to_csv('C:\\Users\\rensm\\Documents\\PostdocLeiden\\NP repository\\test.csv')