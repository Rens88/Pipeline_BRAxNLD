# If you want to edit something in the code and you're not sure where it is, 
# just ask. l.a.meerhoff@liacs.leidenuniv.nl

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

# This folder should contain a folder with 'Data'. The tabular output and figure will be stored in this folder as well.
# folder = 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\Sample repository\\'
# folder = 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\'
# folder = '/home/meerhoffla/Repositories/Sample repository/'
folder = '/local/rens/Repositories/BRAxNLD repository_newStyle/'

# String representing the different teams
# NB: not necessary for FDP (and other datasets where teamstring can be read from the filename, should be done in discetFilename.py)
TeamAstring = 'Provide the string that represents one team' 
TeamBstring = 'Provide the string that represents the other team'

# Input of raw data, indicate at least timestamp, entity and Location info
timestampString = 'Timestamp' 						#'enter the string in the header of the column that represents TIMESTAMP' 	# 'Video time (s)'
PlayerIDstring = 'Player Name' 							#'enter the string in the header of the column that represents PLAYERID' 	# 'jersey n.'
TeamIDstring = 'Team' 								#'enter the string in the header of the column that represents TEAMID' 			# Optional
XPositionString = 'X' 								#'enter the string in the header of the column that represents X-POSITION'			# 'x'
YPositionString = 'Y' 								#'enter the string in the header of the column that represents Y-POSITION'			# 'y'

# Case-sensitive string rawHeaders of attribute columns that already exist in the data (optional). NB: String also sensitive for extra spaces.
readAttributeCols = ['Speed','Acceleration'] # NB: should correspond directly with readAttributeLabels
readAttributeLabels = ['Speed (m/s)','Acceleration (m/s^2)','Distance to closest home (m)','Distance to cloesest visitor (m)'] # NB: should correspond directly with readAttributeCols

# When event columns exist in the raw data, they can be read to export an event file
readEventColumns = []

# If the raw data is not given in meters, provide the conversion.
conversionToMeter = 1 #111111 # https://gis.stackexchange.com/questions/8650/measuring-accuracy-of-latitude-and-longitude/8674#8674

# Here you can determine which event is aggregated before. 
# 'Full' and 'Random' always work. 
# 'Regular' works as long as you don't choose a window larger than your file.
# Other keywords depend on which events you import and/or compute.
aggregateEvent = 'Turnovers' # Event that will be used to aggregate over (verified for 'Goals' and for 'Possession')
allWindows_and_Lags = [(30,0),(25,0),(20,0),(15,0),(10,0),(5,0)] # input tuple of window and corresponding lag. # a negative lag indicates tEnd as after the event

aggregatePerPlayer = [] # a list of outcome variables that you want to aggregated per player. For example: ['vNorm','distFrame']

# Strings need to correspond to outcome variables (dict keys). 
# Individual level variables ('vNorm') should be included as a list element.
# Group level variables ('LengthA','LengthB') should be included as a tuple (and will be plotted in the same plot).
# plotTheseAttributes = ['vNorm',('Surface_ref','Surface_oth')]#,('Spread_ref','Spread_oth'),('stdSpread_ref','stdSpread_oth'),'vNorm']#,'LengthB',('LengthA','LengthB'),('SurfaceA','SurfaceB'),('SpreadA','SpreadB'),('WidthA','WidthB')] # [('LengthA','LengthB'),('WidthA','WidthB'),('SurfaceA','SurfaceB'),('SpreadA','SpreadB')] # teams that need to be compared as tuple
# This trialVisualization plots the selected outcome variables variable for the given window for the temporal aggregation. Useful to verify if your variables are as excpected.
includeTrialVisualization = False
plotTheseAttributes_atTrialLevel = ['vNorm'] #
# This datasetVisualization compares all events of all files in the dataset. Useful for datasetlevel comparisons
includeDatasetVisualization = False
plotTheseAttributes_atDatasetLevel = ['vNorm',('Surface_ref','Surface_oth'),('Spread_ref','Spread_oth')]

# Parts of the pipeline can be skipped
skipCleanup = True # Only works if cleaned file exists. NB: if False, all other skips become ineffective.
skipSpatAgg = True # Only works if spat agg export exists. NB: if False, skipEventAgg and skipToDataSetLevel become ineffective.
skipComputeEvents = True #

# If both True, then files are not verified to be analyzed previously
# If skipToDataSetLevel == False, then it is verified that every match exists in eventAggregate
skipEventAgg = False # For now, don't skip unless longest window was computed. # Only works if current file already exists in eventAgg. NB: if False, skipToDataSetLevel becomes ineffective.
skipToDataSetLevel = False # Only works if corresponding AUTOMATIC BACKUP exists. NB: Does not check if all raw data files are in automatic backup. NB2: does not include any changes in cleanup, spatagg, or eventagg NB2: may not work after implementing iteratoverwindows thingy

# Choose between append (= True) or overwrite (= False) (the first time around only of course) the existing (if any) eventAggregate CSV.
# NB: This could risk in adding duplicate data. There is no warning for that at the moment (could use code from cleanupData that checks if current file already exist in eventAggregate)
appendEventAggregate = False

# Restrict the analysis to files of which event data exists
onlyAnalyzeFilesWithEventData = True

#########################
# END USER INPUT ########
#########################

## Advanced user input (i.e., it might not work if you change it):
# Currently, it's possible to turn off interpolation (saves time), but it may cause other parts of the pipeline to malfunction
includeEventInterpolation = False # may cause problems at the plotting level, 
includeCleanupInterpolation = True # When not interpolating at all, plotting procedure becomes less reliable as it uses an un-aligned index (and it may even fail)
datasetFramerate = 10 # (Hz) This is the framerate with which the whole dataset will be aggregated.

parallelProcess = (12,15) # (nth process,total n processes) # default = (1,1)

#########################
# INITIALIZATION ########
#########################

# pre-initialization
from shutil import copyfile	
import pdb; #pdb.set_trace()
from os.path import isfile, join, exists, realpath, abspath, split,dirname, isdir#, isdir, exists
from os import listdir, stat, sep, rename#, path, makedirs
import inspect
from warnings import warn

cdir = realpath(abspath(split(inspect.getfile( inspect.currentframe() ))[0]))
pdir = dirname(cdir) # parent directory
library_folder = cdir + str(sep + "LibraryRENS")

if not isdir(library_folder):
	# if the process.py is in a subfolder, then copy the initialization module
	copyfile(pdir + sep + 'initialization.py', cdir + sep + 'initialization.py')

import initialization
# In this module, the library is added to the system path. 
# This allows Python to import the custom modules in our library. 
# If you add new subfolders in the library, they need to be added in addLibary (in initialization.py) as well.
dataFolder,tmpFigFolder,outputFolder,cleanedFolder,spatAggFolder,eventAggFolder,aggregatedOutputFilename,outputDescriptionFilename,eventAggFname,backupEventAggFname,DirtyDataFiles,aggregateLevel,t,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization,rawHeaders, attrLabel,skipComputeEvents,DirtyDataFiles_backup = \
initialization.process(studentFolder,folder,aggregateEvent,allWindows_and_Lags,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization,timestampString,PlayerIDstring,TeamIDstring,XPositionString,YPositionString,readAttributeCols,readAttributeLabels,onlyAnalyzeFilesWithEventData,parallelProcess,skipComputeEvents)

# Custom modules (from LibrarRENS)
import datasetVisualization
import spatialAggregation
import temporalAggregation_towardsGeneric
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
import importFieldDimensions
import gc
import iterateWindowsOverEventAgg

#########################
# ANALYSIS (file by file)
#########################

for dirtyFname in DirtyDataFiles:
	print(	'\nFILE: << %s >>' %dirtyFname[:-4])
	t = estimateRemainingTime.printProgress(t)
	gc.collect() # not entirey sure what this does, but it's my attempt to avoid a MemoryError
	if skipToDataSetLevel:
		# Skipp immediately without verification
		warn('\nWARNING: Skipped to datasetLevel without verifying whether matches from current batch were analyzed.')
		break

	#########################
	# PREPARATION ###########
	#########################
	# IMPORTANT: During preparation you can use 'dataType' (although it's better to try not to) which allows you
	# to prepare the data in a way that is specific for your dataset.
		
	# Prepare metadata of aggregated data to be exported:
	exportData, exportDataString, exportDataFullExplanation,cleanFname,spatAggFname,TeamAstring,TeamBstring = \
	dissectFilename.process(dirtyFname,dataType,TeamAstring,TeamBstring,debuggingMode)
	fileIdentifiers = copy.copy(exportData)

	# Clean cleanFname (it only cleans data if there is no existing cleaned file of the current (dirty)file )
	loadFolder,loadFname,fatalTimeStampIssue,skipSpatAgg_curFile,skipEventAgg_curFile,TeamAstring,TeamBstring,skipComputeEvents_curFile = \
	cleanupData.process(dirtyFname,cleanFname,dataType,dataFolder,cleanedFolder,spatAggFname,spatAggFolder,eventAggFolder,eventAggFname,TeamAstring,TeamBstring,rawHeaders,readAttributeCols,timestampString,readEventColumns,conversionToMeter,skipCleanup,skipSpatAgg,skipEventAgg,exportData, exportDataString,includeCleanupInterpolation,datasetFramerate,debuggingMode,skipComputeEvents,aggregateLevel)

	# perhaps write a separate function that decides to skip the file or not.
	# In there, you can find an efficient way to process current skips (and automatically force skip to fals if necessary of higher level processes)

	if fatalTimeStampIssue:

		if not skipEventAgg:
			skippedData = True
			exportCSV.newOrAdd(aggregatedOutputFilename,exportDataString,exportData,skippedData)	
		else:
			doNotExport = []
			# This is the exception. To avoid incorrectly exported data..
			# Best solution would be to look in cleanedfiles to overrule decision not to skipDataset level
			# Anyway, whenever skipping to eventAgg, it's not necessary to export fataIssue because it was already exported in the past.
		continue
	
	if skipEventAgg_curFile and not includeTrialVisualization:# and t[1] != t[2]:
		# if eventAgg exists for currentfile and no trialVizualiation was asked
		appendEventAggregate = True
		continue

	elif skipEventAgg == True:
		# Apparently specifically this file was missing from eventAgg, therefore re-do it, but always as an append
		appendEventAggregate = True
		continue

	# From now onward, rawData contains:
	#  'Ts' --> Timestamp
	#  'X' --> X-position
	#  'Y' --> Y-position
	#  'PlayerID' --> Player identification. NB: Ball-rows should be 'ball' and Match-rows should be 'groupRow' (to indicate CentroidTeamA)
	#  'TeamID' --> Team idenfitification

	########################################################################################
	####### Import existing data ###########################################################
	########################################################################################
	
	# This can be used to import specific field dimensions.
	# By default, it takes the field dimensions that should be typical with football data.
	# This is the first example of using metadata. Probably will end up formalizing this in a 'importMetaData' module.
	fieldDimensions = importFieldDimensions.process(dataFolder,dirtyFname,exportData,exportDataString,debuggingMode)
	# TO DO: add code to rotate based on fieldDimensions !!!!!!!!!!!!!!!!
	# TO DO: add filtering here. !!!!!!!!! THIS IS WHERE YOU SHOULD CONTINE
	rawPanda,attrPanda,attrLabel,eventsPanda,eventsLabel = \
	importTimeseries_aspanda.process(loadFname,loadFolder,skipSpatAgg_curFile,readAttributeCols,readEventColumns,attrLabel,outputFolder,debuggingMode)

	# Here you can write the code to import targetEvents. 
	# Events can be imported from columns in the rawPanda.
	# Or, events can be imported from a separate file (metadata)
	# Eventually, this separate file should be in a generic format. (using a cleanup module)
	# This is the second example of using metadata. Probably will end up formalizing this in a 'importMetaData' module.
	targetEventsImported = importEvents.process(rawPanda,eventsPanda,TeamAstring,TeamBstring,cleanFname,dataFolder,debuggingMode,skipComputeEvents_curFile)
	
	########################################################################################
	####### Compute new attributes #########################################################
	########################################################################################

	attrPanda,attrLabel = spatialAggregation.process(rawPanda,attrPanda,attrLabel,TeamAstring,TeamBstring,skipSpatAgg_curFile,eventsPanda,spatAggFolder,spatAggFname,debuggingMode)

	# NB: targetEVents is a dictionary with the key corresponding to the type of event.
	# For each key, there is a tuple that contains (timeOfEvent,TeamID,..) 
	# --> in some cases there is also a starting time of the event and other information 
	# (for example, possession contains the starting time and the nubmer of passes made within that possession)
	# NB2: For attack - events, use the 4th place in the tuple for the label (e.g., 1 = no shot, 2 = shot off target, 3 = shot on target, 4 = goals)
	targetEvents = \
	computeEvents.process(targetEventsImported,aggregateLevel,rawPanda,attrPanda,eventsPanda,TeamAstring,TeamBstring,dataFolder,cleanFname,debuggingMode,skipComputeEvents_curFile)

	## Temporal aggregation
	exportData,exportDataString,exportFullExplanation,trialEventsSpatAggExcerpt,attrLabel = \
	temporalAggregation_towardsGeneric.process(targetEvents,aggregateLevel,rawPanda,attrPanda,exportData,exportDataString,exportDataFullExplanation,TeamAstring,TeamBstring,debuggingMode,skipEventAgg_curFile,fileIdentifiers,attrLabel,aggregatePerPlayer,includeEventInterpolation,datasetFramerate)
	# pdb.set_trace()
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
	trialVisualization.process(plotTheseAttributes_atTrialLevel,aggregateLevel,trialEventsSpatAggExcerpt,attrLabel,tmpFigFolder,cleanFname[:-4],TeamAstring,TeamBstring,debuggingMode,dataType,fieldDimensions)

estimateRemainingTime.printDuration(t)

################################
# End of file by file analysis #
################################

########################
# DataSetLevel - stuff #
########################
if not skipToDataSetLevel: # i.e., did the whole file-by-file analysis
	# Store an automatic back-up if the file-by-file analysis has been completed without skipping the data.
	copyfile(eventAggFolder + eventAggFname, backupEventAggFname)
	## If you want to be more memory efficient, then use rename instead of copy.
	## Make sure you check the consequences for if-statements that check whether the temporal aggregate exists.
	# os.rename(eventAggFolder + eventAggFname, backupEventAggFname)

if not isfile(backupEventAggFname):
	warn('\nFATAL WARNING: Could not find backupEventAggFname:\n%s' %backupEventAggFname)
	exit()

# Load the datasetEventsSpatAggExcerpt
datasetEventsSpatAggExcerpt = pd.read_csv(backupEventAggFname, low_memory = True, index_col = 'DataSetIndex')
attrLabel_asPanda = pd.read_csv(outputFolder+'attributeLabel.csv',low_memory=True, index_col = 'Unnamed: 0') # index_col added last. Should work. Otherwise use the next line

########### can be embedded in a function?
# Necessary to double check whether there are only files in the backed up file that were also in DirtyDataFiles
# Need to make sure that there are only files in here that were in fact requested
# See also, initialization
uniqueMatches = datasetEventsSpatAggExcerpt['MatchID'].unique()
warn('\nWARNING: Currently, Im using something non-generic. It only works if your file has a MatchID column.\nTo make it more generic, I should store the filename of the match as a column and refer to that instead.\n!!!!!!!!!!!!\n!!!!!!!!!!!!!!!!!\n')

for i in uniqueMatches:
	# Is True when a unique match in eventAggrgate does not exist the dirtyDataFiles
	# This match should be dropped from the backup
	doesItExistIn_DDF = [True for j in DirtyDataFiles_backup if str(i) in j]
	if not any(doesItExistIn_DDF):
		# drop this uniqueMatch from the eventAggregate automatic backup file
		warn('\nWARNING: Dropped MatchID <%s> from eventAggregate backup as it did not exist in DirtyDataFiles.\nNote that it was only dropped from the working memory, the stored CSV still has the data.\n' %i)
		### datasetEventsSpatAggExcerpt = datasetEventsSpatAggExcerpt.loc[datasetEventsSpatAggExcerpt['MatchID'] != i]
		# Memory error with line above, perhaps this works:
		datasetEventsSpatAggExcerpt = datasetEventsSpatAggExcerpt.drop(datasetEventsSpatAggExcerpt[datasetEventsSpatAggExcerpt['MatchID'] != i].index)
########### can be embedded in a function?

# Iterate over the remaining windows
###iterateWindowsOverEventAgg.process(aggregateLevel,eventAggFolder,aggregateEvent,allWindows_and_Lags,eventAggFname,aggregatePerPlayer,outputFolder,debuggingMode,dataFolder,parallelProcess)
iterateWindowsOverEventAgg.process(datasetEventsSpatAggExcerpt,attrLabel_asPanda,aggregateLevel,aggregateEvent,allWindows_and_Lags,aggregatePerPlayer,outputFolder,debuggingMode,dataFolder,parallelProcess,eventAggFname)

warn('\nWARNING: Due to implementation iterateWindowsOverEventAgg, plotting procedure might not work correctly.\n')

############################
# End dataSetLevel - stuff #
############################

########################################################################################
####### datasetVisualization  ##########################################################
########################################################################################

if not includeDatasetVisualization:
	print('No datasetVisualization requested.\n')
else:

	pltFname = 'OVERALL PLOT_' + dataType
	datasetVisualization.process(plotTheseAttributes_atDatasetLevel,aggregateLevel,datasetEventsSpatAggExcerpt,attrLabel_asPanda,tmpFigFolder,pltFname,debuggingMode)

	# When comparing events with a particular ID versus other events with that particular ID
	# It's a work in progress
	if dataType == 'NP':
		pltFname = 'OVERALL PLOT_' + dataType
		datasetVisualization.process(plotTheseAttributes_atDatasetLevel,aggregateLevel,datasetEventsSpatAggExcerpt,attrLabel_asPanda,tmpFigFolder,pltFname,debuggingMode,LPvsNP = True)

########################################################################################
####### statisticalComparison / DATA MINING ############################################
########################################################################################

# Work in progress
# Connect to Cortana for data mining

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