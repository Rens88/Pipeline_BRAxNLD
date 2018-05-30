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
import re


if __name__ == '__main__':
	addLibrary()
	checkFolders(folder,aggregateEvent)

def process(studentFolder,folder,aggregateEvent,allWindows_and_Lags,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization,timestampString,PlayerIDstring,TeamIDstring,XPositionString,YPositionString,readAttributeCols,readAttributeLabels,onlyAnalyzeFilesWithEventData,parallelProcess,skipComputeEvents):

	addLibrary(studentFolder)
	
	# if it works, put this in initialization
	maxStarts_and_Ends = [(i[1]*-1-i[0],i[1]*-1) for i in allWindows_and_Lags]
	minSt,maxEn = maxStarts_and_Ends[0]
	for s,e in maxStarts_and_Ends:
		if s < minSt:
			minSt = s
		if e > maxEn:
			maxEn = e
	aggregateLag = maxEn * -1
	aggregateWindow = maxEn - minSt

	dataFolder,tmpFigFolder,outputFolder,cleanedFolder,spatAggFolder,eventAggFolder,aggregatedOutputFilename,outputDescriptionFilename,eventAggFname,backupEventAggFname,DirtyDataFiles,aggregateLevel = \
	checkFolders(folder,aggregateEvent,aggregateWindow,aggregateLag,onlyAnalyzeFilesWithEventData,parallelProcess)

	# WARNING: any edits made to DirtyDataFiles in skipPartsOfPipeline will NOT apply to the back-up
	if '35_ERE_XIV.csv' in DirtyDataFiles:
		DirtyDataFiles.remove('35_ERE_XIV.csv')
		warn('\nWARNING: Hard-coded to skip Match 35_ERE_XIV. This match has something weird with duplicate players that messes up the temporal aggregation.')

	DirtyDataFiles_backup = DirtyDataFiles.copy()

	t,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization,DirtyDataFiles,skipComputeEvents =\
	skipPartsOfPipeline(backupEventAggFname,DirtyDataFiles,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization,eventAggFolder,eventAggFname,skipComputeEvents,cleanedFolder)
	
	rawHeaders, attrLabel =\
	processUserInput(timestampString,PlayerIDstring,TeamIDstring,XPositionString,YPositionString,readAttributeCols,readAttributeLabels)

	# Check if first DirtyDataFile did not have an empty clean
	# if skipCleanup:
	# 	firstDataFile = DirtyDataFiles[0]
	# 	firstCleanFile = cleanedFolder + firstDataFile[:-4] + '_cleaned.csv'
	# 	if isfile(firstCleanFile):
	# 		df = pd.read_csv(firstCleanFile)
	# 		print(firstCleanFile)
	# 		print()
	# 		print(df.keys())
	# 		print(df['Unnamed: 0'])
	# pdb.set_trace()

	return dataFolder,tmpFigFolder,outputFolder,cleanedFolder,spatAggFolder,eventAggFolder,aggregatedOutputFilename,outputDescriptionFilename,eventAggFname,backupEventAggFname,DirtyDataFiles,aggregateLevel,t,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization,rawHeaders, attrLabel,skipComputeEvents,DirtyDataFiles_backup

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

	# Check whether old eventAgg files were removed to avoid memory issues

	duplSubString = 'eventExcerpt_' + aggregateLevel[0] + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')'
	theseFilesNeedToBeRemoved = [f for f in listdir(eventAggFolder) if not duplSubString in f]
	warn('\nWARNING: It could be nicer by even looking for files with larger windows than the one asked for. See below for a start. Remember to export the new window and lag sizes as the filename afeter initialization...\nShort term solution is to include a window and lag from the longer window and lag..')
	# ## to be more precise:
	# # if lag from existing files is larger than newly requested lag
	# # if existing -(lag + window) > newly requested -(lag+window)
	# for f in listdir(eventAggFolder):
	# 	# use regular expression to extract window and lag

	# 	# test for statements


	if not theseFilesNeedToBeRemoved == []:
		warn('\nFATAL WARNING: eventAggFolder <%s> contained files with other windows than currently analyzing.\nThis leads to a high risk of a memory error.\nBefore you can continue, either delete the old files entirely, or - if you do want to keep them - back them up somewhere else.\n' %eventAggFolder)
		print('The following files need to be removed and/or backup up:\n')
		[print(t) for t in theseFilesNeedToBeRemoved]
		print()
		exit()

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
	eventFolder = dataFolder + sep + 'existingTargets' + sep
	preComputed_targetEventsFolder = eventFolder + 'preComputed' + sep
	
	if onlyAnalyzeFilesWithEventData:			

		# Check if target events already exist
		if not exists(eventFolder):
			warn('\nWARNING: Can only restrict analysis to files with eventData if a folder <existingTargets> exists in the dataFolder.\n')
			DirtyDataFiles = [f for f in listdir(dataFolder) if isfile(join(dataFolder, f)) if '.csv' in f]

			if len(DirtyDataFiles) == 0:
				warn('\nWARNING: No datafiles found that had a matching eventData file.\nNo files will be analyzed.\n')

			makedirs(eventFolder)
			makedirs(preComputed_targetEventsFolder)

		else:
			if not exists(preComputed_targetEventsFolder):
				makedirs(preComputed_targetEventsFolder)

			DirtyEventFiles = [f for f in listdir(eventFolder) if isfile(join(eventFolder, f)) if '.csv' in f]				
			if DirtyDataFiles == []:
				# if no csv files, then look for .xml files:
				DirtyEventFiles = [f for f in listdir(eventFolder) if isfile(join(eventFolder, f)) if '.xml' in f]				
			
			DirtyDataFiles = []
			for f in DirtyEventFiles:
				dataFname = f[:-10]
				if dataFname[-1] == '_':
					dataFname = dataFname[:-1]
				rawData_f = dataFname + '.csv'
				if isfile(join(dataFolder, rawData_f)):
					DirtyDataFiles.append(rawData_f)
				else:
					warn('\nWARNING: Couldnt find the raw data of event file: <%s>.\nTherefore, it was excluded.' %f)
				
	else:
		DirtyDataFiles = [f for f in listdir(dataFolder) if isfile(join(dataFolder, f)) if '.csv' in f]

	# divide dirtyDataFiles amongst parallel processes:
	print('parallelProcess = %s of %s' %parallelProcess)
	nFiles = len(DirtyDataFiles)
	print('nFiles = %s' %nFiles)
	# nPerProcess = np.ceil(nFiles / parallelProcess[1])
	# print('nPerProcess = %s' %nPerProcess)
	# if not parallelProcess[1] == 1:
	# 	start = int((parallelProcess[0] * nPerProcess) - nPerProcess)
	# 	end = int(((parallelProcess[0]+1) * nPerProcess) - nPerProcess)
	# 	print('First file = %s' %start)
	# 	print('Last file = %s\n' %end)
	# 	if parallelProcess[0] == parallelProcess[1]:
	# 		DirtyDataFiles = DirtyDataFiles[start : ]
	# 		print('Last file overwritten to finish to the end.')
	# 	else:
	# 		DirtyDataFiles = DirtyDataFiles[start : end]

	nthBatch = 0
	batchContent = {}
	for n in np.arange(parallelProcess[1]):
		batchContent.update({str(n+1):[]})

	# Distribute files over the batches until there are no files left
	for n in np.arange(nFiles):
		nthBatch = nthBatch + 1
		# print(nthBatch)
		batchContent[str(nthBatch)].append(n)
		# batchContent[nthBatch-1].append(n)
		
		if nthBatch == parallelProcess[1]:
			nthBatch = 0
	### Optional code to check the number of files in each batch
	##nFilesInBatches = 0
	##for n in np.arange(parallelProcess[1]):
	##	# print(len(batchContent[str(n+1)]))
	##	nFilesInBatches = nFilesInBatches + len(batchContent[str(n+1)])
	
	# Convert the indices to the file string
	DirtyDataFiles = [DirtyDataFiles[i] for i in batchContent[str(parallelProcess[0])]]
	
	return dataFolder,tmpFigFolder,outputFolder,cleanedFolder,spatAggFolder,eventAggFolder, outputFilename,outputDescriptionFilename,eventAggFname,backupEventAggFname,DirtyDataFiles,aggregateLevel

def skipPartsOfPipeline(backupEventAggFname,DirtyDataFiles,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization,eventAggFolder,eventAggFname,skipComputeEvents,cleanedFolder):

	t = ([],0,len(DirtyDataFiles))#(time started,nth file,total number of files)

	if not isfile(eventAggFolder + eventAggFname):
		skipEventAgg = False
		warn('\nWARNING: Can only skipEventAgg if output already exists, otherwise the iteration over the windows doesnt work.')

	if not skipComputeEvents and skipEventAgg:
		skipToDataSetLevel = False
		skipEventAgg = False
		warn('\nWARNING: Requested skipEventAgg, but not skipComputeEvents.\nBy default, when not skipping computeEvents, can\'t skip EventAgg (nor jump to datasetlevel).\n')

	if skipToDataSetLevel:
		# check if automatic backup exists
		# if it does

		if isfile(backupEventAggFname):
			# Correct for missing matches due to fatalIssue
			nFatal = 0
			# [print(DDF[:-4] + '_cleaned.csv') for DDF in DirtyDataFiles]
			cleanFilesCurrentBatch = [cleanedFolder + DDF[:-4] + '_cleaned.csv' for DDF in DirtyDataFiles if DDF[:-4] + '_cleaned.csv' in listdir(cleanedFolder)]

			for cf in cleanFilesCurrentBatch:
				df = pd.read_csv(cf,nrows = 1,low_memory=False)
				if 'fatalIssue' in df.keys():
					nFatal = nFatal + 1

			df = pd.read_csv(backupEventAggFname, index_col = 'DataSetIndex',low_memory=False)

			uniqueMatches = df['MatchID'].unique()
			warn('\nWARNING: Currently, Im using something non-generic. It only works if your file has a MatchID column.\nTo make it more generic, I should store the filename of the match as a column and refer to that instead.\n!!!!!!!!!!!!\n!!!!!!!!!!!!!!!!!\n')

			storeItAgain = False
			for i in uniqueMatches:
				# Is True when a unique match in eventAggrgate does not exist the dirtyDataFiles
				# This match should be dropped from the backup
				doesItExistIn_DDF = [True for j in DirtyDataFiles if str(i) in j]
				if not any(doesItExistIn_DDF):
					# drop this uniqueMatch from the eventAggregate automatic backup file
					warn('\nWARNING: Dropped MatchID <%s> from eventAggregate backup as it did not exist in DirtyDataFiles.\nNote that it was only dropped from the automatic backup, in the original file the match can still be accessed.\n' %i)
					### df = df.loc[df['MatchID'] != i]
					# Memory error with line above, perhaps this works:
					df = df.drop(df[df['MatchID'] != i].index)
					
					storeItAgain = True

			if storeItAgain:
				# Overwrite existing if changs were made
				df.to_csv(backupEventAggFname)

			# recompute uniqueMatches
			# check if the length is the same as the length of DirtyDataFiles? 
			# --> small weakness, sometimes a match is skipped because of missing data. This won't show up in eventAggregate
			# --> alternatively, figure out which match each DDF is..
			uniqueMatchesAfterDropping = df['MatchID'].unique()

			if len(uniqueMatchesAfterDropping) < len(DirtyDataFiles) + nFatal:
				# Only need to test for shorter than, as it's only important whether files are missing.
				# After the file-by-file analysis, it is checked one more time whether there aren't any files in there that shouldn't be in there.
				# The reason for this, is to not fully delete these extra matches from any of the stored data, in case it was accidental.
				# They can be deleted from the automatic back-up (see above), but not from the original file.
				warn('\nWARNING: Suspected that files are missing from eventAggregate (could be because there is a fatal error in the data).\nTo be sure, you cant skip directly to dataset level (which then verifies whether every file exists')
				skipToDataSetLevel = False

		else:
			warn('\nWARNING: Could not skip to datasetlevel because the backup of the eventAggregate didnt exist:\n%s' %backupEventAggFname)
			skipToDataSetLevel = False

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
			warn('\n********\nWARNING: Skipped analyzing the database and jumped straight to DataSet-level comparisons.\nAny raw data, spatial aggregates ARE NOT INCLUDED.\nTo re-analyze the database, change <skipToDataSetLevel> to False.\n')

			t = (t[0],t[2],t[2])
			DirtyDataFiles = [] # WARNING: any edits made to DirtyDataFiles in skipPartsOfPipeline will NOT apply to the back-up
		else:
			skipToDataSetLevel = False
			warn('\nWARNING: Tried to <skipToDataSetLevel>, but could not find corresponding data backup:\n%s\n\n*********' %backupEventAggFname)

	return t,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization,DirtyDataFiles,skipComputeEvents


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