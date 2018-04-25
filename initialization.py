# 13/03/2018 Rens Meerhoff
# Function that initializes the pipeline.

from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs, sep
import sys, inspect
import pdb; #pdb.set_trace()
from warnings import warn
import time

if __name__ == '__main__':
	addLibrary()
	checkFolders(folder,aggregateEvent)

def addLibrary(studentFolder):

	####### Adding the library ######
	# The folder and relevant subfolders where you store the python library with all the custom modules.
	current_folder = path.realpath(path.abspath(path.split(inspect.getfile( inspect.currentframe() ))[0]))
	library_folder = current_folder + str(sep + "LibraryRENS")
	library_subfolders = [library_folder + str(sep + "FDP")] # FDP = Football Data Project
	library_subfolders.append(library_folder + str(sep + "" + studentFolder)) # Contributions of a Bachelor student
	# library_subfolders.append(library_folder + str(sep + "LTcontributions")) # Contributions of a Bachelor student

	if library_folder not in sys.path:
		sys.path.insert(0, library_folder) 
	for subf in library_subfolders:
		if subf not in sys.path:
			sys.path.insert(0, subf) # idea: could loop over multiple subfolders and enter as a list

	## Uncomment this line to open the function in the editor (matlab's ctrl + d)
	# subprocess.call(cmd_folder + "\\callThisFunction.py", shell=True)
	#########################################


def checkFolders(folder,aggregateLevel,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization):
	if folder[-1:] != sep:
		warn('\n<folder> did not end with <%s>. \nOriginal input <%s>\nReplaced with <%s>' %(sep,folder,folder+sep))
		folder = folder + sep

	dataFolder = folder + 'Data' + sep
	tmpFigFolder = folder + 'Figs' + sep + 'Temp' + sep + aggregateLevel[0] + sep
	outputFolder = folder + 'Output' + sep# Folder where tabular output will be stored (aggregated spatially and temporally)
	cleanedFolder = dataFolder + 'Cleaned' + sep    
	spatAggFolder = dataFolder + 'SpatAgg' + sep
	eventAggFolder = dataFolder + 'EventAgg' + sep

	# Verify if folders exists
	if not exists(dataFolder):
		warn('\nWARNING: dataFolder not found.\nMake sure that you put your data in the folder <%s>\n' %dataFolder)
		exit()
	if not exists(outputFolder):
		makedirs(outputFolder)
	if not exists(tmpFigFolder):
		makedirs(tmpFigFolder)
	if not exists(cleanedFolder):
		makedirs(cleanedFolder)
	if not exists(spatAggFolder):
		makedirs(spatAggFolder)
	if not exists(eventAggFolder):
		makedirs(eventAggFolder)
	
	timeString = time.strftime("%Hh%Mm_%d_%B_%Y")
	outputFilename = outputFolder + 'output_' + aggregateLevel[0] + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')_' + timeString +  '.csv'
	outputDescriptionFilename = outputFolder + 'output_Description_' + aggregateLevel[0] + '.txt'
	eventAggFname = 'eventExcerpt_' + aggregateLevel[0] + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ').csv'
	backupEventAggFname = eventAggFolder + 'eventExcerpt_' + aggregateLevel[0] + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ') - AUTOMATIC BACKUP.csv'



	DirtyDataFiles = [f for f in listdir(dataFolder) if isfile(join(dataFolder, f)) if '.csv' in f]
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


	return dataFolder,tmpFigFolder,outputFolder,cleanedFolder,spatAggFolder,eventAggFolder, outputFilename,outputDescriptionFilename,eventAggFname,backupEventAggFname,DirtyDataFiles,t,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization