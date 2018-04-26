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
dataType =  "FDP" # "FDP" or or "NP" --> so far, only used to call the right cleanup script. Long term goal would be to have a generic cleanup script

# This folder should contain a folder with 'Data'. The tabular output and figures will be stored in this folder as well.
folder = 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository\\'

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
## For inomtio data, aggregateEvent can either be 'Full' or 'Random'
## Other levels of temporal aggregation to be added soon.
# Indicate some parameters for temporal aggregation: 'Full' aggregates over the whole file, any other event needs to be specified with the same string as in the header of the CSV file.
aggregateEvent = 'Random' # Event that will be used to aggregate over (verified for 'Goals' and for 'Possession')
aggregateWindow = 10 # in seconds #NB: still need to write warning in temporal aggregation in case you have Goals in combination with None.
aggregateLag = 0 # in seconds

# Key events (TO DO)
# - Load existing events
# - Include modules to compute events
## -- \work in progress -- 

# Strings need to correspond to outcome variables (dict keys). 
# Individual level variables ('vNorm') should be included as a list element.
# Group level variables ('LengthA','LengthB') should be included as a tuple (and will be plotted in the same plot).
plotTheseAttributes = ['vNorm',('Surface_ref','Surface_oth')]#,('Spread_ref','Spread_oth'),('stdSpread_ref','stdSpread_oth'),'vNorm']#,'LengthB',('LengthA','LengthB'),('SurfaceA','SurfaceB'),('SpreadA','SpreadB'),('WidthA','WidthB')] # [('LengthA','LengthB'),('WidthA','WidthB'),('SurfaceA','SurfaceB'),('SpreadA','SpreadB')] # teams that need to be compared as tuple
# This trialVisualization plots the selected outcome variables variable for the given window for the temporal aggregation. Useful to verify if your variables are as excpected.
includeTrialVisualization = False
# This datasetVisualization compares all events of all files in the dataset. Useful for datasetlevel comparisons
includeDatasetVisualization = False

# Parts of the pipeline can be skipped
skipCleanup = False # Only works if cleaned file exists. NB: if False, all other skips become ineffective.
skipSpatAgg = True # Only works if spat agg export exists. NB: if False, skipEventAgg and skipToDataSetLevel become ineffective.
skipEventAgg = False # Only works if current file already exists in eventAgg. NB: if False, skipToDataSetLevel becomes ineffective.
skipToDataSetLevel = False # Only works if corresponding AUTOMATIC BACKUP exists. NB: Does not check if all raw data files are in automatic backup. NB2: does not include any changes in cleanup, spatagg, or eventagg


# Choose between append (= True) or overwrite (= False) (the first time around only of course) the existing (if any) eventAggregate CSV.
# NB: This could risk in adding duplicate data. There is no warning for that at the moment (could use code from cleanupData that checks if current file already exist in eventAggregate)
appendEventAggregate = False

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
dataFolder,tmpFigFolder,outputFolder,cleanedFolder,spatAggFolder,eventAggFolder,aggregatedOutputFilename,outputDescriptionFilename,eventAggFname,backupEventAggFname,DirtyDataFiles,t,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization =\
initialization.checkFolders(folder,aggregateLevel,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization)

import pdb; #pdb.set_trace()
from os.path import isfile, join, exists#, isdir, exists
from os import listdir, stat#, path, makedirs
from warnings import warn
# Custom modules (from LibrarRENS)
import datasetVisualization
import spatialAggregation
import temporalAggregation
import importEvents
import dissectFilename
import importTimeseries_aspanda
import cleanupData
import pandas as pd
import numpy as np
import exportCSV
import estimateRemainingTime
import trialVisualization
import computeEvents
import copy
from shutil import copyfile
import importFieldDimensions

## These lines should be embedded elsewhere in the future.
# Preparing the dictionary of the raw data (NB: With the use of Pandas, this is a bit redundant)
rawHeaders = {'Ts': timestampString,\
'PlayerID': PlayerIDstring,\
'TeamID': TeamIDstring,\
'Location': (XPositionString,YPositionString) }

#########################
# ANALYSIS (file by file)
#########################

for dirtyFname in DirtyDataFiles[:10]:
	# dirtyFname = 'data_JYSS_1E1_T1_Gp 3 v 4_oneSheet_inColumns.csv'
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
	fileIdentifiers = copy.copy(exportData)

	# Clean cleanFname (it only cleans data if there is no existing cleaned file of the current (dirty)file )
	loadFolder,loadFname,fatalTimeStampIssue,skipSpatAgg_curFile,skipEventAgg_curFile = \
	cleanupData.process(dirtyFname,cleanFname,dataType,dataFolder,cleanedFolder,spatAggFname,spatAggFolder,eventAggFolder,eventAggFname,TeamAstring,TeamBstring,rawHeaders,readAttributeCols,timestampString,readEventColumns,conversionToMeter,skipCleanup,skipSpatAgg,skipEventAgg,exportData, exportDataString,debuggingMode)

	if fatalTimeStampIssue:
		skippedData = True
		exportCSV.newOrAdd(aggregatedOutputFilename,exportDataString,exportData,skippedData)	
		continue
	if skipEventAgg_curFile and not includeTrialVisualization and t[1] != 1:
		# The first loop has to be run to make sure the attribute labels are exported.
		# The rest of the loop can be skipped, as there is no action to be taken in the rest of the for loop
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
	
	# This can be used to import specific field dimensions.
	# By default, it takes the field dimensions that should be typical with football data.
	# This is the first example of using metadata. Probably will end up formalizing this in a 'importMetaData' module.
	fieldDimensions = importFieldDimensions.process(dataFolder,dirtyFname,exportData,exportDataString)
	# TO DO: add code to rotate based on fieldDimensions !!!!!!!!!!!!!!!!
	# TO DO: add filtering here. !!!!!!!!!1 THIS IS WHERE YOU SHOULD CONTINE
	rawPanda,attrPanda,attrLabel,eventsPanda,eventsLabel = \
	importTimeseries_aspanda.process(loadFname,loadFolder,skipSpatAgg_curFile,readAttributeCols,readEventColumns,attrLabel,outputFolder,debuggingMode)

	###### Work in progress ##########
	# Currently code is not very generic. It should work for NP though..
	targetEventsImported = importEvents.process(eventsPanda,TeamAstring,TeamBstring)
	###### \Work in progress #########

	########################################################################################
	####### Compute new attributes #########################################################
	########################################################################################

	attrPanda,attrLabel = spatialAggregation.process(rawPanda,attrPanda,attrLabel,TeamAstring,TeamBstring,skipSpatAgg_curFile,debuggingMode)

	###### Work in progress ##########
	# NB: targetEVents is a dictionary with the key corresponding to the type of event.
	# For each key, there is a tuple that contains (timeOfEvent,TeamID,..) 
	# --> in some cases there is also a starting time of the event and other information 
	# (for example, possession contains the starting time and the nubmer of passes made within that possession)
	# NB2: Probably should change this to a panda / dictionary to avoid errors.
	targetEvents = \
	computeEvents.process(targetEventsImported,aggregateLevel,rawPanda,attrPanda,eventsPanda,TeamAstring,TeamBstring,debuggingMode)
	###### \Work in progress #########

	## Temporal aggregation
	# TO DO: add interpolation here !!!!!!!!!1 THIS IS WHERE YOU SHOULD CONTINE
	exportData,exportDataString,exportFullExplanation,trialEventsSpatAggExcerpt,attrLabel = \
	temporalAggregation.process(targetEvents,aggregateLevel,rawPanda,attrPanda,exportData,exportDataString,exportDataFullExplanation,TeamAstring,TeamBstring,debuggingMode,skipEventAgg_curFile,fileIdentifiers,attrLabel)

	########################################################################################
	####### EXPORT to CSV ##################################################################
	########################################################################################

	if not all([skipEventAgg, skipSpatAgg, skipCleanup]): # and not skip to dataset level (if statement occurs at initialization)
		appendEventAggregate = \
		exportCSV.process(trialEventsSpatAggExcerpt,exportData,exportDataString,exportFullExplanation,readEventColumns,readAttributeCols,aggregatedOutputFilename,outputDescriptionFilename,rawPanda,eventsPanda,attrPanda,spatAggFolder,spatAggFname,eventAggFolder,eventAggFname,appendEventAggregate,skipEventAgg_curFile,fileIdentifiers,t,attrLabel,outputFolder,debuggingMode)
	else:
		# Load previously created excerpt
		datasetEventsSpatAggExcerpt = pd.read_csv(eventAggFolder + eventAggFname, low_memory = False, index_col = 'DataSetIndex')
		FileID = "_".join(fileIdentifiers)
		# Select events related to current file only --> realistically, this is only used for trialVisualization
		trialEventsSpatAggExcerpt = datasetEventsSpatAggExcerpt.loc[datasetEventsSpatAggExcerpt['EventUID'].str.contains(FileID)]

	if not includeTrialVisualization: # stop early if trialVisualization is FALSE
		continue

	########################################################################################
	####### trialVisualization  ############################################################
	########################################################################################
			
	# This plotting procedure allows you to plot the events separately. 
	# These plots can be used to (visually) assess whether the outcome measures had the expected values.
	trialVisualization.process(plotTheseAttributes,aggregateLevel,trialEventsSpatAggExcerpt,attrLabel,tmpFigFolder,cleanFname[:-4],TeamAstring,TeamBstring,debuggingMode,dataType,fieldDimensions)
estimateRemainingTime.printDuration(t)

if not skipToDataSetLevel: # i.e., did the whole file-by-file analysis
	# Store an automatic back-up if the file-by-file analysis has been completed without skipping the data.
	copyfile(eventAggFolder + eventAggFname, backupEventAggFname)

# Load the datasetEventsSpatAggExcerpt
datasetEventsSpatAggExcerpt = pd.read_csv(backupEventAggFname, low_memory = False, index_col = 'DataSetIndex')
# if attrLabel != {}:
# 	attrLabel_asPanda = pd.DataFrame.from_dict([attrLabel],orient='columns')
# else:
# Load the saved attribute labels
attrLabel_asPanda = pd.read_csv(outputFolder+'attributeLabel.csv',low_memory=False, index_col = 'Unnamed: 0') # index_col added last. Should work. Otherwise use the next line
# attrLabel_asPanda.set_index('Unnamed: 0', drop=True, append=False, inplace=True, verify_integrity=False)

################################
# End of file by file analysis #
################################

########################################################################################
####### datasetVisualization  ##########################################################
########################################################################################

if not includeDatasetVisualization:
	print('No datasetVisualization requested.\n')
else:

	# # In attrLabel_asPanda, create a column that identifies each event by combining
	# tmp = pd.DataFrame([], index = eventExcerptPanda.index, columns = ['UID'])
	# # eventExcerptPanda["EventUID"] = eventExcerptPanda[0] + eventExcerptPanda['temporalAggregate']
	# for idx,val in enumerate(fileIdentifiers):
	# 	if idx == 0:
	# 		tmp['UID'] = eventExcerptPanda[val]
	# 	else:
	# 		tmp['UID'] = tmp['UID'] + eventExcerptPanda[val]

	# # Unless the filename was already similar before, this should create unique identifiers per event
	# eventExcerptPanda["EventUID"] = tmp['UID'] + eventExcerptPanda['temporalAggregate']
	# TO DO:
	# Write a check to verify that the fileidentifiers combine into a unique ID...
	# If not, this would be very problematic!!
	# But.. It would also mean that the filenames in the raw data are the same / very similar (a space difference for example, or capitalization)


	# !!!!!!!!! THIS IS WHERE YOU LEFT IT !!!!!!!!
	# !!!!!!!!! THIS IS WHERE YOU LEFT IT !!!!!!!!
	# !!!!!!!!! THIS IS WHERE YOU LEFT IT !!!!!!!!
	# !!!!!!!!! THIS IS WHERE YOU LEFT IT !!!!!!!!
	# print(len(eventExcerptPanda.keys()))
	# print(eventExcerptPanda.keys())
	# pdb.set_trace()
	# print('--')
	# eventExcerptPanda = eventExcerptPanda.drop_duplicates()
	# print(len(eventExcerptPanda.keys()))
	# pdb.set_trace()
	pltFname = 'OVERALL PLOT_' + dataType
	datasetVisualization.process(plotTheseAttributes,aggregateLevel,datasetEventsSpatAggExcerpt,attrLabel_asPanda,tmpFigFolder,pltFname,TeamAstring,TeamBstring,debuggingMode)


########################################################################################
####### statisticalComparison / DATA MINING ############################################
########################################################################################


########################################################################################
####### THE END ########################################################################
########################################################################################
print('       -')
print('      ---')
print('     -----') 
print('    -------') 
print('   ---------')
print('  -----------')
print(' -------------')
print('---------------')
print('---- THE END ----')
print('---------------')
print(' -------------')
print('  -----------')
print('   ---------')
print('    -------')
print('     -----') 
print('      ---')
exit('       -')