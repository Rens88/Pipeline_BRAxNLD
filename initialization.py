# 13/03/2018 Rens Meerhoff
# Function that initializes the pipeline.

from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs, sep, walk
import pandas as pd
import sys, inspect
import pdb; #pdb.set_trace()
from warnings import warn
import time
import numpy as np

if __name__ == '__main__':
	addLibrary()
	checkFolders(folder,aggregateEvent)

def process(studentFolder,folder,aggregateEvent,aggregateWindow,aggregateLag,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization,timestampString,PlayerIDstring,TeamIDstring,XPositionString,YPositionString,readAttributeCols,readAttributeLabels,onlyAnalyzeFilesWithEventData,parallelProcess):

	addLibrary(studentFolder)
	
	dataFolder,tmpFigFolder,outputFolder,cleanedFolder,spatAggFolder,eventAggFolder,aggregatedOutputFilename,outputDescriptionFilename,eventAggFname,backupEventAggFname,DirtyDataFiles,aggregateLevel = \
	checkFolders(folder,aggregateEvent,aggregateWindow,aggregateLag,onlyAnalyzeFilesWithEventData,parallelProcess)

	t,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization =\
	skipPartsOfPipeline(backupEventAggFname,DirtyDataFiles,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization)
	
	rawHeaders, attrLabel =\
	processUserInput(timestampString,PlayerIDstring,TeamIDstring,XPositionString,YPositionString,readAttributeCols,readAttributeLabels)

	return dataFolder,tmpFigFolder,outputFolder,cleanedFolder,spatAggFolder,eventAggFolder,aggregatedOutputFilename,outputDescriptionFilename,eventAggFname,backupEventAggFname,DirtyDataFiles,aggregateLevel,t,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization,rawHeaders, attrLabel

def addLibrary(studentFolder):

	####### Adding the library ######
	# The folder and relevant subfolders where you store the python library with all the custom modules.
	current_folder_process = path.realpath(path.abspath(path.split(inspect.getfile( inspect.currentframe() ))[0]))
	current_folder = current_folder_process

	library_folder = current_folder + str(sep + "LibraryRENS")
	if not path.isdir(library_folder):
		# either the current, or the foler above
		current_folder = path.dirname(current_folder)
		library_folder = current_folder + str(sep + "LibraryRENS")

	if not path.isdir(library_folder):
		warn('\nFATAL WARNING: Could not find the library. It should be named <libraryRENS> and either put in:\n%s\nor in:\n%s' %(current_folder,current_folder_process))

	if library_folder not in sys.path:
		sys.path.insert(0, library_folder) 	

	library_subfolders = [library_folder + str(sep + "FDP")] # FDP = Football Data Project
	library_subfolders.append(library_folder + str(sep + "" + studentFolder)) # Contributions of a Bachelor student
	# library_subfolders.append(library_folder + str(sep + "LTcontributions")) # Contributions of a Bachelor student

	for subf in library_subfolders:
		if subf not in sys.path:
			sys.path.insert(0, subf) # idea: could loop over multiple subfolders and enter as a list

	## Uncomment this line to open the function in the editor (matlab's ctrl + d)
	# subprocess.call(cmd_folder + "\\callThisFunction.py", shell=True)
	#########################################


def checkFolders(folder,aggregateEvent,aggregateWindow,aggregateLag,onlyAnalyzeFilesWithEventData,parallelProcess):
	aggregateLevel = (aggregateEvent,aggregateWindow,aggregateLag)

	if folder[-1:] != sep:
		warn('\n<folder> did not end with <%s>. \nOriginal input <%s>\nReplaced with <%s>' %(sep,folder,folder+sep))
		folder = folder + sep

	dataFolder = folder + 'Data' + sep
	dataFolder_BRxNL = dataFolder + 'BRAxNLD Data' + sep
	tmpFigFolder = folder + 'Figs' + sep + 'Temp' + sep + aggregateLevel[0] + sep
	outputFolder = folder + 'Output' + sep# Folder where tabular output will be stored (aggregated spatially and temporally)
	cleanedFolder = dataFolder + 'Cleaned' + sep    
	spatAggFolder = dataFolder + 'SpatAgg' + sep
	eventAggFolder = dataFolder + 'EventAgg' + sep

	# Verify if folders exists
	BRAxNLD = False
	if not exists(dataFolder):
		warn('\nFATAL WARNING: dataFolder not found.\nMake sure that you put your data in the folder <%s>\n' %dataFolder)
		exit()
	if exists(dataFolder_BRxNL):
		BRAxNLD = True

	if not exists(outputFolder):
		makedirs(outputFolder)
	if not exists(tmpFigFolder):
		makedirs(tmpFigFolder)
	if not exists(tmpFigFolder + 'trialVisualization' + sep):
		makedirs(tmpFigFolder + 'trialVisualization' + sep)
	if not exists(cleanedFolder):
		makedirs(cleanedFolder)
	if not exists(spatAggFolder):
		makedirs(spatAggFolder)
	if not exists(eventAggFolder):
		makedirs(eventAggFolder)
	
	timeString = time.strftime("%Hh%Mm_%d_%B_%Y")
	outputFilename = outputFolder + 'output_' + aggregateLevel[0] + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')_' + timeString + '_' + str(parallelProcess[0]) + 'of' + str(parallelProcess[1]) +  '.csv'
	outputDescriptionFilename = outputFolder + 'output_Description_' + aggregateLevel[0] + '.txt'

	eventAggFname = 'eventExcerpt_' + aggregateLevel[0] + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')_' + str(parallelProcess[0]) + 'of' + str(parallelProcess[1]) + '.csv'
	backupEventAggFname = eventAggFolder + 'eventExcerpt_' + aggregateLevel[0] + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')_' + str(parallelProcess[0]) + 'of' + str(parallelProcess[1]) + '- AUTOMATIC BACKUP.csv'


	# if BRAxNLD:
	# 	# basically, this is a work-around for when the data is in subfolders
	# 	# could make it more generic.
	# 	#
	# 	# dataFolder_BRxNL
	# 	DirtyDataFiles = []
	# 	DirtyEventFiles = []
	# 	for dirpath, dirnames, filenames in walk(dataFolder_BRxNL):
	# 		for f in filenames:
	# 			if '_SpecialExport.csv' in f:
	# 				lastSubDir = dirpath.replace(dataFolder,'')
	# 				# if not exists(cleanedFolder + lastSubDir):
	# 				# 	makedirs(cleanedFolder + lastSubDir)
	# 				DirtyDataFiles.append(path.join(lastSubDir, f))

	# 			elif '_Event.xlsx' in f:
	# 				DirtyEventFiles.append(path.join(dirpath, f))
	# 	        	# and of course I can specify the others..

	# 	# print(DirtyDataFiles)
	# 	# pdb.set_trace()
	# 	# print(listdir(dataFolder_BRxNL))
	# 	# DirtyDataFolders = [f for f in listdir(dataFolder_BRxNL) if isdir(join(dataFolder, f))]
	# 	# print(DirtyDataFolders)
	# 	# FilesInDirtyDataFolders = [listdir(f) for f in DirtyDataFolders]
	# 	# print(FilesInDirtyDataFolders)
	# 	# # DirtyDataFiles = [f for f in FilesInDirtyDataFolders if isfile(join(dataFolder, f)) if '.csv' in f]
	# 	# if isfile(join(dataFolder, f)) if '.csv' in f]
	# 	# And eventfiles?

	# 	# pdb.set_trace()
	if onlyAnalyzeFilesWithEventData:
		# Check if target events already exist
		if not exists(dataFolder + sep + 'existingTargets'):
			warn('\nWARNING: Can only restrict analysis to files with eventData if a folder <existingTargets> exists in the dataFolder.\n')
			DirtyDataFiles = [f for f in listdir(dataFolder) if isfile(join(dataFolder, f)) if '.csv' in f]

			if len(DirtyDataFiles) == 0:
				warn('\nWARNING: No datafiles found that had a matching eventData file.\nNo files will be analyzed.\n')

		else:
			eventFolder = dataFolder + sep + 'existingTargets' + sep
			DirtyEventFiles = [f for f in listdir(eventFolder) if isfile(join(eventFolder, f)) if '.xml' in f]	#LT: changed to .xml		
			DirtyDataFiles = []
			for f in DirtyEventFiles:
				rawData_f = f[:-11] + '.csv' #LT: changed to -11
				if isfile(join(dataFolder, rawData_f)):
					DirtyDataFiles.append(rawData_f)
				else:
					warn('\nWARNING: Couldnt find the raw data of event file: <%s>.\nTherefore, it was excluded.' %f)
				
	else:
		DirtyDataFiles = [f for f in listdir(dataFolder) if isfile(join(dataFolder, f)) if '.csv' in f]

	# divide dirtyDataFiles amongst parallel processes:
	nFiles = len(DirtyDataFiles)
	nPerProcess = np.ceil(nFiles / parallelProcess[1])
	if not parallelProcess[1] == 1:
		start = int((parallelProcess[0] * nPerProcess) - nPerProcess)
		end = int(((parallelProcess[0]+1) * nPerProcess) - nPerProcess)
		print('First file = %s' %start)
		print('Last file = %s\n' %end)
		if parallelProcess[0] == parallelProcess[1]:
			DirtyDataFiles = DirtyDataFiles[start : ]
		else:
			DirtyDataFiles = DirtyDataFiles[start : end]

	return dataFolder,tmpFigFolder,outputFolder,cleanedFolder,spatAggFolder,eventAggFolder, outputFilename,outputDescriptionFilename,eventAggFname,backupEventAggFname,DirtyDataFiles,aggregateLevel

def skipPartsOfPipeline(backupEventAggFname,DirtyDataFiles,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization):

	t = ([],0,len(DirtyDataFiles))#(time started,nth file,total number of files)

	# Skipping certain parts can only be done if other parts are skipped as well:
	if skipCleanup == False: # NB: if False, all other skips become ineffective.
		if skipSpatAgg:
			skipSpatAgg = False
			warn('\nWARNING: Requested skipSpatAgg, but not skipCleanup.\nBy default, when not skipping cleanup, spatial aggregation can\'t be skipped.\n')
		if skipEventAgg:
			skipEventAgg = False		
			warn('\nWARNING: Requested skipEventAgg, but not skipCleanup.\nBy default, when not skipping cleanup, event aggregation can\'t be skipped.\n')
		if skipToDataSetLevel:
			skipToDataSetLevel = False
			warn('\nWARNING: Requested skipToDataSetLevel, but not skipCleanup.\nBy default, when not skipping cleanup, can\'t jump to datasetlevel.\n')
	else:
		if skipSpatAgg == False: # NB: if False, skipEventAgg and skipToDataSetLevel become ineffective.
			if skipEventAgg:
				skipEventAgg = False		
				warn('\nWARNING: Requested skipEventAgg, but not skipSpatAgg.\nBy default, when not skipping spatial aggregation, event aggregation can\'t be skipped.\n')
			if skipToDataSetLevel:
				skipToDataSetLevel = False
				warn('\nWARNING: Requested skipToDataSetLevel, but not skipSpatAgg.\nBy default, when not skipping spatial aggregation, can\'t jump to datasetlevel.\n')
		else:
			if skipEventAgg == False: # NB: if False, skipToDataSetLevel becomes ineffective.
				if skipToDataSetLevel:
					skipToDataSetLevel = False
					warn('\nWARNING: Requested skipToDataSetLevel, but not skipEventAgg.\nBy default, when not skipping event aggregation, can\'t jump to datasetlevel.\n')
			else:
				if skipToDataSetLevel:
					if includeTrialVisualization:
						includeTrialVisualization = False
						warn('\nWARNING: Requested includeTrialVisualization, but also skipToDataSetLevel.\nBy default, when skipping to DataSet-level, trial-level plots are skipped.\n')


	if skipToDataSetLevel: # This allows you to quickly skip the analysis section, if you've already created a backup of a fully analyzed dataset
		if isfile(backupEventAggFname):
			warn('\n********\nWARNING: Skipped analyzing the database and jumped straight to DataSet-level comparisons.\nAny new files, spatial aggregates, temporal aggregates, windows, lags etc. ARE NOT INCLUDED.\nTo re-analyze the database, change <skipToDataSetLevel> to False. (and re-analyzing MANUALLY copy a \'BACKUP\'.)\n')
			t = (t[0],t[2],t[2])
			DirtyDataFiles = []
		else:
			skipToDataSetLevel = False
			warn('\nWARNING: Tried to <skipToDataSetLevel>, but could not find corresponding data backup:\n%s\n\n*********' %backupEventAggFname)

	return t,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization


def processUserInput(timestampString,PlayerIDstring,TeamIDstring,XPositionString,YPositionString,readAttributeCols,readAttributeLabels):
	## These lines should be embedded elsewhere (e.g. in initialization) in the future.
	## UPDATE: THE FUTURE IS NOW

	# Preparing the dictionary of the raw data (NB: With the use of Pandas, this is a bit redundant)
	rawHeaders = {'Ts': timestampString,\
	'PlayerID': PlayerIDstring,\
	'TeamID': TeamIDstring,\
	'Location': (XPositionString,YPositionString) }
	# if len(readAttributeCols) != len(readAttributeLabels):
	# 	warn('\nFORMAT USER INPUT ERROR: The number of columns to be read as attributes does not correspond to the number of labels given.\nMake sure that for each column that is read from the data there is a label.\nAs a working solution, the attributes will be given <no_label> as a label.\n********* PLEASE UPDATE USER INPUT ********\n**********************************')
	attrLabel = {}
	for ix,v in enumerate(readAttributeCols):
		
		if len(readAttributeLabels) >= ix:
			attrLabel.update({readAttributeCols[ix]: readAttributeLabels[ix]})
		else:
			warn('\nFORMAT USER INPUT ERROR: The number of columns to be read as attributes does not correspond to the number of labels given.\nMake sure that for each column that is read from the data there is a label.\nAs a working solution, the attributes will be given <no_label> as a label.\n********* PLEASE UPDATE USER INPUT ********\n*******************************************')
			attrLabel.update({readAttributeCols[ix]: 'NO_LABEL'})

	return rawHeaders, attrLabel