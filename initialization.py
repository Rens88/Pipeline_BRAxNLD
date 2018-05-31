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

def process(studentFolder,folder,aggregateEvent,allWindows_and_Lags,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization,timestampString,PlayerIDstring,TeamIDstring,XPositionString,YPositionString,readAttributeCols,readAttributeLabels,onlyAnalyzeFilesWithEventData,parallelProcess,skipComputeEvents,**kwargs):
	skipEventAgg_MatchVerification = False
	if 'skipEventAgg_MatchVerification' in kwargs:
		skipEventAgg_MatchVerification = kwargs['skipEventAgg_MatchVerification']

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
		warn('\nWARNING: Hard-coded to skip Match 35_ERE_XIV. This match has something weird with duplicate players that messes up the temporal aggregation.\nOther matches may have the same issue.\nNEEDS TO BE SOLVED\n!!!!!!!!!!\n!!!!!!!!!.')

	if '76_ERE-P_XIV.csv' in DirtyDataFiles:
		DirtyDataFiles.remove('76_ERE-P_XIV.csv')
		warn('\nWARNING: Hard-coded to skip Match 8_ERE_XIV. This match has something weird with the events which results in incorrectly not appending (but overwriting) the eventAggregate in the next file of the file-by-file analysis.\n!!!!!!!!!!!!!!!!!!\nThis is a bigger issue that may result into missing matches in other batches as well.\nNEEDS TO BE SOLVED\n!!!!!!!!!!\n!!!!!!!!!.')

	DirtyDataFiles_backup = DirtyDataFiles.copy()

	t,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization,DirtyDataFiles,skipComputeEvents =\
	skipPartsOfPipeline(backupEventAggFname,DirtyDataFiles,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization,eventAggFolder,eventAggFname,skipComputeEvents,cleanedFolder,skipEventAgg_MatchVerification,dataFolder)
	
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
			if DirtyEventFiles == []:
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
	# print('parallelProcess = %s of %s' %parallelProcess)
	nFiles = len(DirtyDataFiles)
	# print('nFiles = %s' %nFiles)
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

def skipPartsOfPipeline(backupEventAggFname,DirtyDataFiles,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization,eventAggFolder,eventAggFname,skipComputeEvents,cleanedFolder,skipEventAgg_MatchVerification,dataFolder):

	t = ([],0,len(DirtyDataFiles))#(time started,nth file,total number of files)

	if not isfile(eventAggFolder + eventAggFname):
		skipEventAgg = False
		warn('\nWARNING: Can only skipEventAgg if output already exists, otherwise the iteration over the windows doesnt work.\nThe missing file should be in:\n%s\nAnd should be called:\n%s\n' %(eventAggFolder,eventAggFname))

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
				df = pd.read_csv(cf,nrows = 1,low_memory=True)
				if 'fatalIssue' in df.keys():
					nFatal = nFatal + 1

			# This file can become very big, so it needs to be read in chuncks:
			uniqueMatches = []
			dfNeedsToBeStoredAgain = False
			warn('\nWARNING: Currently, Im using something non-generic. It only works if your file has a MatchID column.\nTo make it more generic, I should store the filename of the match as a column and refer to that instead.\n!!!!!!!!!!!!\n!!!!!!!!!!!!!!!!!\n')
			chunkedDf = pd.read_csv(backupEventAggFname, index_col = 'DataSetIndex',low_memory=True,chunksize = 1000000,usecols = ['DataSetIndex','MatchID'])
			
			nMatchesThatDontBelongInCurrentBatch = 0
			for chunk in chunkedDf:
				umCurChunk = chunk['MatchID'].unique()
				curUM = [umCC for umCC in umCurChunk if not umCC in uniqueMatches]
				[uniqueMatches.append(i) for i in curUM]
				for i in curUM:
					doesItExistIn_DDF = [True for j in DirtyDataFiles if str(i) in j]
					if not any(doesItExistIn_DDF):
						# if skipEventAgg_MatchVerification:
						# 	warn('\nWARNING: Found matches that did not belong in the current batch according.\nWhen running the pipeline from a different operating system as where the eventAggFile was created, this may be a result storing files in listdir differently.\nIn that case, this is a known issue (and will be solved in the future).\nTo be sure, you could set <skipEventAgg_MatchVerification> to False, which checks the contents of the eventAgg file, but this is time-consuming...\n')
						# else:
						# 	warn('\nWARNING: Found matches that did not belong in current batch. Will drop these below.\n' %i)
						nMatchesThatDontBelongInCurrentBatch = nMatchesThatDontBelongInCurrentBatch + 1
						dfNeedsToBeStoredAgain = True
						if not skipEventAgg_MatchVerification:
							# Break early to save time, you're gonna re-run this loop below anyway.
							break

			# issue some warnings 
			if dfNeedsToBeStoredAgain and skipEventAgg_MatchVerification:
				warn('\nWARNING: Found <%s> matches that did not belong in the current batch.\nWhen running the pipeline from a different operating system as where the eventAggFile was created, this may be a result storing files in listdir differently.\nIn that case, this is a known issue (and will be solved in the future).\nTo be sure, you could set <skipEventAgg_MatchVerification> to False, which checks the contents of the eventAgg file, but this is time-consuming...\n' %nMatchesThatDontBelongInCurrentBatch)

			elif dfNeedsToBeStoredAgain and not skipEventAgg_MatchVerification:
				warn('\nWARNING: Found <%s> matches that did not belong in current batch. Will drop these below.\n' %nMatchesThatDontBelongInCurrentBatch)

				# Now read the whole thing, all columns.
				chunkedDf = pd.read_csv(backupEventAggFname, index_col = 'DataSetIndex',low_memory=True,chunksize = 1000000)

				# Do it again, but now actually drop it in store it			
				allChunksCombined = pd.DataFrame([])
				uniqueMatches = []

				for chunk in chunkedDf:
					umCurChunk = chunk['MatchID'].unique()

					# This needs to be skipped per chunk (so based on curUM and not uniqueMatches)
					for i in umCurChunk:
						# doesItExistIn_DDF = [True for j in DirtyDataFiles if str(i) in j]
						# More specific:
						doesItExistIn_DDF = [True for j in DirtyDataFiles if str(i) == str(j.split('_')[0])]
						if not any(doesItExistIn_DDF):
							warn('\nWARNING: Dropped MatchID <%s> from eventAggregate backup as it did not exist in DirtyDataFiles.\nNote that it was only dropped from the automatic backup, in the original file the match can still be accessed.\n' %i)
							chunk = chunk.drop(chunk[chunk['MatchID'] != i].index)
						else: # if it does exist, add it to uniqueMatches if it wasnt found in a previous chunk
							if not i in uniqueMatches:
								uniqueMatches.append(i)
								# else, it had already been added previously

					# Store chunk
					allChunksCombined = pd.concat([allChunksCombined, chunk])#, ignore_index = True)

				# export (and overwrite)
				allChunksCombined.to_csv(backupEventAggFname)

			targetFolder = dataFolder + sep + 'existingTargets' + sep
			preComputedTargetFolder = targetFolder + 'preComputed' + sep
			warn_and_exit = False
			if exists(preComputedTargetFolder):
				# find a way to iterate over all keys.
				files = listdir(preComputedTargetFolder)

				for MatchID in uniqueMatches:

					curFiles = [f for f in files if '_preComputed_Event_' in f and str(MatchID) == str(f.split('_')[0])]

					if curFiles == []:
						warn('\nWARNING: Pre-fatal: could not find any pre-computed event files that corresponded to MatchID <%s>' %MatchID)
						warn_and_exit = True

			else:
				# It will crash (probably) if there are no targetEvents: 
				# - in iterateWindows, it will go through a special part of temporalAggregation.py
				# - in this section, it will read the targetEvents from file (which will go wrong if it doesnt exist)
				# Also:
				# - in countEvent2, it assumes that targetEvents with each key exist.
				# 
				# Therefore, now I'm choosing option 3
				warn('\nWARNING: Pre-fatal: Could not find pre-computed events folder:\n<%s>' %preComputedTargetFolder)
				warn_and_exit = True

			if warn_and_exit:
				warn('\nFATAL WARNING: The files that will be aggregated temporarily MUST have pre-computed targetEvents stored in the designated foler (data / existingTargets / preComputed).\nWithout these, things will go wrong when iterating over windows.\nTherefore, exit. Read code here for some ideas.\n')
				print('For every unique match included, there MUST be a targetEvents pre-computed file.')
				print('They must exist, because the event features are currently not exported in the eventAggregate exported csv.')
				print('Either:')
				print('1) Find a way to export the eventAggregate features to the eventAgg file')
				print('2) Edit temporalAggregation_towardsGeneric.py to be able to deal with the absence of a targetEvents file (and simply return None or Nan or 0 for event aggregates)')
				print('But for now:') 
				print('3) Exit the pipeline')
				exit()

			if len(uniqueMatches) < len(DirtyDataFiles) + nFatal + nMatchesThatDontBelongInCurrentBatch:
				# Only need to test for shorter than, as it's only important whether files are missing.
				# After the file-by-file analysis, it is checked one more time whether there aren't any files in there that shouldn't be in there.
				# The reason for this, is to not fully delete these extra matches from any of the stored data, in case it was accidental.
				# They can be deleted from the automatic back-up (see above), but not from the original file.
				if not skipEventAgg_MatchVerification:
					warn('\nWARNING: Suspected that files are missing from eventAggregate (could be because there is a fatal error in the data).\nTo be sure, you cant skip directly to dataset level (which then verifies whether every file exists')
					skipToDataSetLevel = False
				else:
					warn('\nWARNING: Skipped the match verification, but it seems that the eventAgg Back-up file is not complete (or has more matches than requested according to DirtyDataFiles as run on current OS).\nIf you want to make sure all matches have been included, change <skipEventAgg_MatchVerification> to False.\n')
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
					warn('\nWARNING: Requested skipToDataSetLevel, but not skipEventAgg (it was not requested, or could not be done - in that case, see warning messages above).\nBy default, when not skipping event aggregation, can\'t jump to datasetlevel.\n')
			else:
				if skipToDataSetLevel:
					if includeTrialVisualization:
						includeTrialVisualization = False
						warn('\nWARNING: Requested includeTrialVisualization, but also skipToDataSetLevel.\nBy default, when skipping to DataSet-level, trial-level plots are skipped.\n')


	if skipToDataSetLevel: # This allows you to quickly skip the analysis section, if you've already created a backup of a fully analyzed dataset
		if isfile(backupEventAggFname):
			warn('\n********\nWARNING: Skipped analyzing the database and jumped straight to DataSet-level comparisons.\nAny NEW raw data, spatial aggregates ARE NOT INCLUDED.\nTo re-analyze the database, change <skipToDataSetLevel> to False.\n')

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