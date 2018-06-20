# 26-05-2018 Rens Meerhoff
import time
import pandas as pd
import gc
import pdb; #pdb.set_trace()
from os.path import isfile, join, exists#, isdir, exists
from os import listdir, stat#, path, makedirs
from warnings import warn
import temporalAggregation_towardsGeneric



if __name__ == '__main__':

	process(aggregateLevel,eventAggFolder,aggregateEvent,allWindows_and_Lags,eventAggFname,aggregatePerPlayer,outputFolder,debuggingMode)

###def process(aggregateLevel,eventAggFolder,aggregateEvent,allWindows_and_Lags,eventAggFname,aggregatePerPlayer,outputFolder,debuggingMode,dataFolder,parallelProcess):
def process(df,attrLabel_asPanda,aggregateLevel,aggregateEvent,allWindows_and_Lags,aggregatePerPlayer,outputFolder,debuggingMode,dataFolder,parallelProcess,eventAggFname):

	#########################
	# Temporal aggregation (file by file)
	#########################
	tIteratWindowsOverEventAgg = time.time()

	winString = '_window(%s)_' %aggregateLevel[1]
	lagString = '_lag(%s)' %aggregateLevel[2]
	# ppString = '%sof%s' %parallelProcess

	# # Find the ones with corresponding events, windows and lags, AND parallelProcess
	# eventDataFiles = [f for f in listdir(eventAggFolder) if isfile(join(eventAggFolder, f)) if 'AUTOMATIC BACKUP.csv' in f if aggregateEvent in f if winString in f if lagString in f if ppString in f]
	# attrLabel_asPanda = pd.read_csv(outputFolder+'attributeLabel.csv',low_memory=True, index_col = 'Unnamed: 0') # index_col added last. Should work. Otherwise use the next line

	# for eventFname in eventDataFiles:

	# 	gc.collect() # not entirey sure what this does, but it's my attempt to avoid a MemoryError

	# 	df = pd.read_csv(eventAggFolder+eventFname,low_memory=False) # NB: low_memory MUST be True, otherwise it results in problems later on.

	origEnd = -aggregateLevel[2] # relative to eventfor eventFname in eventDataFiles:

	origStart = origEnd - aggregateLevel[1]

	for win,lag in allWindows_and_Lags:

		# create the new filename
		tmp = eventAggFname.replace(winString,'_window(%s)_' %win)
		exportFname = tmp.replace(lagString,'_lag(%s)_DerivedFromLargestWindow_' %lag)

		# Check if requested window itTempAggs covered by available window
		newEnd = -lag # relative to event
		newStart = newEnd - win

		if newEnd > origEnd or newStart < origStart:
			warn('\nWARNING: Could not compute new temporal aggregate, as the originaly aggregate did not cover the requested aggregated.\nThis can be avoided by design: when running the pipeline, compute the event aggregates based on the largest windows imaginable given the user input.\n')
			continue

		exportMatrix = \
		temporalAggregation_towardsGeneric.processEventAggOnly(df,newStart,newEnd,aggregateEvent,attrLabel_asPanda,aggregatePerPlayer,debuggingMode,dataFolder)

		if 'eventOverlap' in exportMatrix.keys() and 'dtPrevEvent' in exportMatrix.keys():
			# replace eventOverlap Boolean based on interated window
			exportMatrix.loc[exportMatrix['dtPrevEvent'] > win,'eventOverlap'] = False

		# new temporal aggregation

		## NB: event based output (i.e., eventOverlap) needs to be re-computed. It's currently being skipped

		# export file
		exportMatrix.to_csv(outputFolder + exportFname)

		# Export also as eventAggregate?
		# store <backupEventAggFname>s which is used for datasetlevel plots.
	if debuggingMode:
		elapsed = str(round(time.time() - tIteratWindowsOverEventAgg, 2))
		print('***** Time elapsed during iterateWindowsOverEventAgg: %ss' %elapsed)
