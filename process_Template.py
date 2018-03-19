# # # THESE FIRST FOUR LINES OF CODE ARE ONLY NECESSARY TO TEST WHETHER THE TEMPLATE STILL WORKS ON FDP DATA.
# import os
# if os.path.isfile('C:\\Users\\rensm\\Documents\\PostdocLeiden\\BRAxNLD repository\\Data\\Cleaned\\CROPPED_XX123456_XX0001_v_XX0002_vPP_SpecialExport_cleaned.csv'):
# 	os.remove('C:\\Users\\rensm\\Documents\\PostdocLeiden\\BRAxNLD repository\\Data\\Cleaned\\CROPPED_XX123456_XX0001_v_XX0002_vPP_SpecialExport_cleaned.csv')

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
# This folder should contain a folder with 'Data'. The tabular output and figures will be stored in this folder as well.
folder = 'C:\\Users\\rensm\\Documents\\PostdocLeiden\\BRAxNLD repository\\'

# Here, you provide the string name of the student folder that you want to include.
studentFolder = 'XXcontributions' 

# Temporary inputs (whilst updating to using pandas)
exportPerFile = True # whether you want to export a csv for every complete file (no temporal aggregation)
debuggingMode = False # whether yo want to continue with the remaining code to incorporate using pandas (temporal aggregation, export and visualization)

# dataType is used for dataset specific parts of the analysis (in the preparation phase only)
dataType =  "FDP" # "FPD" or or "NP" --> so far, only used to call the right cleanup script. Long term goal would be to have a generic cleanup script

# String representing the different teams
# NB: not necessary for FDP (and other datasets where teamstring can be read from the filename, should be done in discetFilename.py)
TeamAstring = 'Provide the string that represents one team' 
TeamBstring = 'Provide the string that represents the other team'

# Input of raw data, indicate at least timestamp, entity and Location info
timestampString = 'Timestamp' 						#'enter the string in the header of the column that represents TIMESTAMP' 	# 'Video time (s)'
PlayerIDstring = 'Naam' 							#'enter the string in the header of the column that represents PLAYERID' 	# 'jersey n.'
TeamIDstring = None 								#'enter the string in the header of the column that represents TEAMID' 			# Optional
XPositionString = 'X' 								#'enter the string in the header of the column that represents X-POSITION'			# 'x'
YPositionString = 'Y' 								#'enter the string in the header of the column that represents Y-POSITION'			# 'y'

# Case-sensitive string rawHeaders of attribute columns that already exist in the data (optional). NB: String also sensitive for extra spaces.
readAttributeCols = ['Snelheid','Acceleration']
attrLabel = {readAttributeCols[0]: 'Speed (m/s)',readAttributeCols[1]: 'Acceleration (m/s^2)'} 

# When event columns exist in the raw data, they can be read to export an event file
readEventColumns = []

# If the raw data is not given in meters, provide the conversion.
conversionToMeter = 1 #111111 # https://gis.stackexchange.com/questions/8650/measuring-accuracy-of-latitude-and-longitude/8674#8674

## -- work in progress --  ##
## For inomtio data, the only aggregateEvent that works is 'Full'
## Other levels of temporal aggregation to be added soon.
# Indicate some parameters for temporal aggregation: 'Full' aggregates over the whole file, any other event needs to be specified with the same string as in the header of the CSV file.
aggregateEvent = 'Full' # Event that will be used to aggregate over (verified for 'Goals' and for 'Possession')
aggregateWindow = 10 # in seconds #NB: still need to write warning in temporal aggregation in case you have Goals in combination with None.
aggregateLag = 0 # in seconds

# This (simple) visualization plots every outcome variable for the given window for the temporal aggregation
Visualization = False # True = includes visualization, False = skips visualization

# Key events (TO DO)
# - Load existing events
# - Include modules to compute events
## -- \work in progress -- 

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
dataFolder,tmpFigFolder,outputFolder,cleanedFolder,aggregatedOutputFilename,outputDescriptionFilename =\
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
t = ([],1,len(DirtyDataFiles))#(time started,nth file,total number of files)

for dirtyFname in DirtyDataFiles:
	print(	'\nFILE: << %s >>' %dirtyFname[:-4])
	t = estimateRemainingTime.printProgress(t)
	#########################
	# PREPARATION ###########
	#########################
	# IMPORTANT: During preparation you can use 'dataType' (although it's better to try not to) which allows you
	# to prepare the data in a way that is specific for your dataset.

	# Prepare metadata of aggregated data to be exported:
	exportData, exportDataString, exportDataFullExplanation,cleanFname,TeamAstring,TeamBstring = \
	dissectFilename.process(dirtyFname,dataType,TeamAstring,TeamBstring)

	# Clean cleanFname (it only cleans data if there is no existing cleaned file of the current (dirty)file )
	cleanedFolder,fatalTimeStampIssue = \
	cleanupData.process(dirtyFname,cleanFname,dataType,dataFolder,cleanedFolder,TeamAstring,TeamBstring,rawHeaders,readAttributeCols,timestampString,readEventColumns,conversionToMeter)

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

	rawPanda = importTimeseries_aspanda.rawData(cleanFname,cleanedFolder)
	attrPanda,attrLabel = importTimeseries_aspanda.existingAttributes(cleanFname,cleanedFolder,readAttributeCols,attrLabel)
	eventsPanda,eventsLabel = importTimeseries_aspanda.existingAttributes(cleanFname,cleanedFolder,readEventColumns,attrLabel)	

	###### Work in progress ##########
	# Currently code is not very generic. It should work for NP though..
	# The events are based on event columns that have the same structure as the timeseries data.
	targetEventsImported = importEvents.process(eventsPanda,TeamAstring,TeamBstring)
	###### \Work in progress #########

	########################################################################################
	####### Compute new attributes #########################################################
	########################################################################################

	## Spatial aggregation
	attrPanda,attrLabel = spatialAggregation.process(rawPanda,attrPanda,attrLabel,TeamAstring,TeamBstring)

	###### Work in progress ##########
	## Temporal aggregation
	exportData,exportDataString,exportFullExplanation = \
	temporalAggregation.process(targetEventsImported,aggregateLevel,rawPanda,attrPanda,exportData,exportDataString,exportDataFullExplanation,TeamAstring,TeamBstring)

	########################################################################################
	####### EXPORT to CSV #########################################################
	########################################################################################

	# This can be written more efficiently.
	# Idea: recognize when trial already exists in data and overwrite.
	skippedData = False
	exportCSV.newOrAdd(aggregatedOutputFilename,exportDataString,exportData,skippedData)	
	exportCSV.varDescription(outputDescriptionFilename,exportDataString,exportFullExplanation)

	## As a temporary work around, the raw data is here exported per file
	if exportPerFile:
		# debugging only
		altogether = pd.concat([rawPanda, attrPanda], axis=1) # debugging only
		altogether.to_csv(outputFolder + 'output_' + dirtyFname) # debugging only		
		print('EXPORTED <%s>' %dirtyFname[:-4])
		print('in <%s>' %outputFolder)
	continue
	###### \Work in progress #########
	# import time
	# t = time.time()	# do stuff
	# elapsed = time.time() - t
	# print('Time elapsed: %s' %elapsed)
	# pdb.set_trace()

	#############################################################
	#############################################################
	#############################################################
	#############################################################
	#############################################################
	#############################################################
	#############################################################
	# The code below still needs to be adjusted to using pandas #
	#############################################################
	#############################################################
	#############################################################
	#############################################################
	#############################################################
	#############################################################
	if not debuggingMode:
		continue
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
