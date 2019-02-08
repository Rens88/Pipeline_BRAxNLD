# 06-12-2017 Rens Meerhoff
# Script to export a CSV. Check whether the file already exists and then adds it.

import numpy as np
import pandas as pd
import pdb; #pdb.set_trace()
import csv
from warnings import warn
# import numpy as np
from os.path import isfile, join, exists#, isdir, exists
from os import listdir#, startfile
import os, sys, subprocess
import pandas as pd
import time
import logging
from datetime import datetime
from os.path import isfile, join, exists, realpath, abspath, split,dirname, isdir, basename

# import CSVexcerpt


if __name__ == '__main__':

	# fname can include the folder as well
	newOrAdd(fname,header,data,skippedData)
	varDescription(fname,exportDataString,exportFullExplanation)

	# Filename should probably be 'temp.csv'
	# varToPrint is the variable you want to print
	# winopen: if True: open in windows
	debugPrint(filename,varToPrint,winopen)
#########################################################################

def open_file(filename):
    if sys.platform == "win32":
        os.startfile(filename)
    else:
        opener ="open" if sys.platform == "darwin" else "xdg-open"
        subprocess.call([opener, filename])

def process(trialEventsSpatAggExcerpt,exportData,exportDataString,exportFullExplanation,readEventColumns,readAttributeCols,aggregatedOutputFilename,outputDescriptionFilename,rawPanda,eventsPanda,attrPanda,spatAggFolder,spatAggFname,eventAggFolder,eventAggFname,appendEventAggregate,skipEventAgg_curFile,fileIdentifiers,t,attrLabel,outputFolder,debuggingMode):
	tExportCSV = time.time()

	# Determine standardized column order, with all identifier columns at the start, the remaining columns witll be alphabetical
	firstColumns = ['TrialBasedIndex','eventTimeIndex','eventTime','Ts','X','Y','TeamID','PlayerID','EventUID']
	secondColumns = []
	thirdColumns = []
	laterColumns = []
	for ikey in trialEventsSpatAggExcerpt.keys():
		if ikey in firstColumns:
			# skip it, as it was already predefined
			continue
		if ikey in exportDataString:
			# File and event identifiers
			firstColumns.append(ikey)
		elif ikey in readEventColumns:
			# Pre existing event columns
			secondColumns.append(ikey)
		elif ikey in readAttributeCols:
			# Pre existing attribute columns
			thirdColumns.append(ikey)
		else:
			laterColumns.append(ikey)
	columnOrder = firstColumns + secondColumns + thirdColumns + laterColumns

	# if len(exportData[0]) <= len(firstColumns):
	# ###if len(exportData.keys()) <= len(firstColumns):
	# 	skippedData = True
	# 	newOrAdd(aggregatedOutputFilename,exportDataString,exportData,skippedData)
	# 	warn('\nWARNING: No Data exported.\nProbably because there were no targetevents detected.\nCheck if this was warned for in temporalAggregation.\n')

	# 	if debuggingMode:
	# 		elapsed = str(round(time.time() - tExportCSV, 2))
	# 		print('***** Time elapsed during exportCSV: %ss' %elapsed)
	# 	return appendEventAggregate

	# # Temporally aggregated data
	# skippedData = False
	# newOrAdd(aggregatedOutputFilename,exportDataString,exportData,skippedData)
	
	# check if file exists
	# if not, create new

	## Export attribute label for skip skipToDataSetLevel
	if t[1] == 1: # only after the first file (attribute label won't change in the next iterations of the file by file analysis)
		attrLabel_asPanda = pd.DataFrame.from_dict([attrLabel],orient='columns')
		attrLabel_asPanda.to_csv(outputFolder + 'attributeLabel.csv')
		exportData.to_csv(aggregatedOutputFilename)

	elif isfile(aggregatedOutputFilename):
		existing_df = pd.read_csv(aggregatedOutputFilename)
		combined_df = pd.concat([existing_df, exportData],axis = 0, ignore_index = True)

		try:
			combined_df.drop(['Unnamed: 0'],axis = 1,inplace=True)
		except:
			logging.warning(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' - ' + basename(__file__) + '\nCouldnt find unnamed column.\nConsider specifying index column string to make it consistent.\nThis will result in a (mostly) empty column that should be named something with <Unnamed>.')
			warn('\nWARN: Couldnt find unnamed column.\nConsider specifying index column string to make it consistent.\nThis will result in a (mostly) empty column that should be named something with <Unnamed>.\n')
		
		existingFirstColumns = [i for i in firstColumns if i in combined_df.keys()]
		notExistingFirstColumns = [i for i in firstColumns if not i in combined_df.keys()]
		strangeColumns = [i for i in notExistingFirstColumns if i not in ['TrialBasedIndex', 'eventTimeIndex', 'eventTime', 'Ts', 'X', 'Y', 'TeamID', 'PlayerID']]

		if strangeColumns != []:
			logging.warning(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' - ' + basename(__file__) + '\nDuring export, these columns existed in the spatial aggregate, but were not exported:\n<%s>' %strangeColumns)
			warn('\nWARNING: During export, these columns existed in the spatial aggregate, but were not exported:\n<%s>' %strangeColumns)

		LastColumns = [i for i in combined_df.keys() if i not in existingFirstColumns]

		columnOrderAggregatedOutput = existingFirstColumns + LastColumns

		combined_df = combined_df[columnOrderAggregatedOutput]
		combined_df.to_csv(aggregatedOutputFilename)
	else:
		logging.warning(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' - ' + basename(__file__) + '\nIt was not the first time that exportCSV was run, but still, the aggregatedOutputFilename <%s> could not be found. This means that the data from the current file was not exported.' %aggregatedOutputFilename)
		warn('\nFATAL WARNING: It was not the first time that exportCSV was run, but still, the aggregatedOutputFilename <%s> could not be found. This means that the data from the current file was not exported.' %aggregatedOutputFilename)

	varDescription(outputDescriptionFilename,exportDataString,exportFullExplanation)

	# # Spatially aggregated data ----> moved to end of spatial aggregation
	# spatAggPanda = pd.concat([rawPanda, eventsPanda.loc[:, eventsPanda.columns != 'Ts'], attrPanda.loc[:, attrPanda.columns != 'Ts']], axis=1) # Skip the duplicate 'Ts' columns
	# spatAggPanda.to_csv(spatAggFolder + spatAggFname)

	# Spatially aggregated data per event
	# (with the specified window), added into one long file combining the whole database.

	appendEventAggregate = eventAggregate(eventAggFolder,eventAggFname,appendEventAggregate,trialEventsSpatAggExcerpt,skipEventAgg_curFile,fileIdentifiers,columnOrder)

	if debuggingMode:
		elapsed = str(round(time.time() - tExportCSV, 2))
		print('***** Time elapsed during exportCSV: %ss' %elapsed)
	return appendEventAggregate

######################################################################################################################################################
######################################################################################################################################################

def eventAggregate(eventAggFolder,eventAggFname,appendEventAggregate,trialEventsSpatAggExcerpt,skipEventAgg_curFile,fileIdentifiers,columnOrder):
	if skipEventAgg_curFile:
		loging.warning(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' - ' + basename(__file__) + '\nDid not export any new eventAggregate data.\nIf you want to add new (or revised) spatial aggregates, change <skipEventAgg> into <False>.\n')
		warn('\nDid not export any new eventAggregate data.\nIf you want to add new (or revised) spatial aggregates, change <skipEventAgg> into <False>.\n')
		return appendEventAggregate

	# Give the trial based index a name
	trialEventsSpatAggExcerpt.index.name = 'TrialBasedIndex'
	# Create a new index
	trialEventsSpatAggExcerpt.reset_index(inplace=True)
	# Give the new index a name
	trialEventsSpatAggExcerpt.index.name = 'DataSetIndex'

	if exists(eventAggFolder + eventAggFname) and appendEventAggregate:# and not stat(eventAggFolder + eventAggFname).st_size == 0:
		# Append to existing file

		# # Store the trial based index
		# trialEventsSpatAggExcerpt.index.name = 'TrialBasedIndex'
		# # Create a new index
		# trialEventsSpatAggExcerpt.reset_index()
		# trialEventsSpatAggExcerpt.index.name = 'DataSetIndex'

		# Load the existing file
		datasetEventsSpatAggExcerpt = pd.read_csv(eventAggFolder + eventAggFname, low_memory = False, index_col = 'DataSetIndex')

		# Check if current event already exists in datasetEventsSpatAggExcerpt
		FileID = "_".join(fileIdentifiers)
		if any(datasetEventsSpatAggExcerpt['EventUID'].str.contains(FileID)):
			loging.warning(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' - ' + basename(__file__) + '\ncurrent trialEventsSpatAggExcerpt already existed in datasetEventsSpatAggExcerpt.\nFileID = <%s>\nDropped it from the previously stored datasetEventsSpatAggExcerpt.\nIF THIS WAS UNINTENTIONAL, change appendEventAggregate to False (which creates an entirely new datasetEventsSpatAggExcerpt.' %FileID)
			warn('\nWARNING: current trialEventsSpatAggExcerpt already existed in datasetEventsSpatAggExcerpt.\nFileID = <%s>\nDropped it from the previously stored datasetEventsSpatAggExcerpt.\nIF THIS WAS UNINTENTIONAL, change appendEventAggregate to False (which creates an entirely new datasetEventsSpatAggExcerpt.' %FileID)
			## Drop previous version of the current trial
			datasetEventsSpatAggExcerpt.drop(datasetEventsSpatAggExcerpt['EventUID'].str.contains(FileID).index, inplace = True)
			## Only select trials that do not correspond with the current trial (does the same as dropping it..)
			# datasetEventsSpatAggExcerpt = datasetEventsSpatAggExcerpt.loc[datasetEventsSpatAggExcerpt['EventUID'].str.contains(FileID) == False]

		# Concat with new eventExcerpt
		try:
			combinedData = pd.concat([datasetEventsSpatAggExcerpt, trialEventsSpatAggExcerpt], axis = 0,ignore_index = True)
			## This should also work:
			#result = df1.append(dicts, ignore_index=True)

		except:
			print('****************************')
			print('****************************')
			print('\n************ WARNING *****************')
			print('Had to drop a duplicate column. For some reason, one of the columns appeared twice.')
			print('Original keys existing data:')
			print(datasetEventsSpatAggExcerpt.keys())
			print('Length:')
			print(len(datasetEventsSpatAggExcerpt.keys()))
			print('Original keys new data:')
			print(trialEventsSpatAggExcerpt.keys())
			print('Length:')
			print(len(trialEventsSpatAggExcerpt.keys()))
			print('maybe it\'s empty:')
			print(trialEventsSpatAggExcerpt.empty)

			trialEventsSpatAggExcerpt = trialEventsSpatAggExcerpt.drop_duplicates()
			# combinedData = pd.concat([datasetEventsSpatAggExcerpt, trialEventsSpatAggExcerpt], axis = 0)
			combinedData = pd.concat([datasetEventsSpatAggExcerpt, trialEventsSpatAggExcerpt], axis = 0,ignore_index = True)

			print('New keys:')
			print(trialEventsSpatAggExcerpt.keys())
			print('Length:')
			print(len(trialEventsSpatAggExcerpt.keys()))
			print('\n************ WARNING *****************')
			print('****************************')
			print('****************************')
			print('****************************')

	elif not trialEventsSpatAggExcerpt.empty:
		# # Give the trial based index a name
		# trialEventsSpatAggExcerpt.index.name = 'TrialBasedIndex'
		# # Create a new index
		# trialEventsSpatAggExcerpt.reset_index(inplace=True)
		# # Give the new index a name
		# trialEventsSpatAggExcerpt.index.name = 'DataSetIndex'
		# Create a new file
		# trialEventsSpatAggExcerpt.to_csv(eventAggFolder + eventAggFname, index_label = 'DataSetIndex')
		combinedData = trialEventsSpatAggExcerpt
		# From now onward, append to existing eventAgg
		appendEventAggregate = True

	else: # apparently trialEventsSpatAggExcerpt was empty..
		loging.warning(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' - ' + basename(__file__) + '\nTargetevents were empty. \nNo Data exported.')
		warn('\nWARNING: Targetevents were empty. \nNo Data exported.\n')
		return appendEventAggregate


	# double check if all columns exist
	for ikey in combinedData.keys():
		if not ikey in columnOrder:
			# ikey wasnt in columnOrder, add it to the end
			loging.warning(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' - ' + basename(__file__) + '\n<%s> existed in the data, but was not given in columnOrder.\nTherefore, it was added to the end of columnOrder.' %ikey)
			warn('\nWARNING: <%s> existed in the data, but was not given in columnOrder.\nTherefore, it was added to the end of columnOrder.' %ikey)
			# columnOrder = columnOrder + ikey
			columnOrder.append(ikey)
	for ikey in columnOrder:
		if not ikey in combinedData:
			# ikey wasnt in columnOrder, remove it
			loging.warning(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' - ' + basename(__file__) + '\n<%s> existed in the columnOrder, but not in the data.\nTherefore, it was omitted from the columnOrder.' %ikey)
			warn('\nWARNING: <%s> existed in the columnOrder, but not in the data.\nTherefore, it was omitted from the columnOrder.' %ikey)
			columnOrder.remove(ikey)

	# Save as new csv (overwrite)
	# combinedData = combinedData.astype(object)
	# print(type(combinedData))
	# print(type(columnOrder))
	# print(eventAggFolder + eventAggFname)
	# test = pd.DataFrame([])
	# test.to_csv(eventAggFolder + eventAggFname)

	# print('test 1 successful')
	# test = pd.DataFrame([],columns = ['DataSetIndex'])
	# test.to_csv(eventAggFolder + eventAggFname, index_label = 'DataSetIndex')

	# print('test 2 successful')
	
	# test = pd.DataFrame([],columns = columnOrder)
	# test.to_csv(eventAggFolder + eventAggFname, columns = columnOrder)
	
	# print('test 3 successful')
	# combinedData = combinedData[columnOrder]


	tmp = pd.DataFrame([],index = combinedData.index)
	for c in columnOrder:
		
		tmp = tmp.join(combinedData[c])
	tmp.to_csv(eventAggFolder + eventAggFname, index_label = 'DataSetIndex')

	# print('BEFORE ordering columns')
	# print(combinedData.keys())
	# print('AFTER ordering columns')
	# print(tmp.keys())
	# print('**************************')
	# print(combinedData.shape)
	# combinedData = tmp
	# print(combinedData.shape)
	###combinedData.to_csv(eventAggFolder + eventAggFname, index_label = 'DataSetIndex')

	return appendEventAggregate

def debugPrint(filename,varToPrint,winopen):

	with open(filename,'w',newline='') as myfile:
		wr = csv.writer(myfile)
		for i in varToPrint:
			# if len(i) == 1:
			# 	print('I did this')
			# 	print([i][0:5])
			# 	wr.writerow([i])
			# else:
			# 	print('I did the other this')
			# 	print(len(i))
			# 	print(i[0:5])
			wr.writerow(i)


	if winopen == True:
		open_file(filename)

def varDescription(fname,exportDataString,exportFullExplanation):
	with open(fname,'w') as myfile:
		for idx,val in enumerate(exportDataString):

			# IDEA: This can be written better using the number of characters of the header and the desired characters before the description starts

			curLine = '%s:\t\t\t %s\n' %(val,exportFullExplanation[idx])
			if len(val) >= 15:
				curLine = '%s:\t\t %s\n' %(val,exportFullExplanation[idx])
			if len(val) >= 23:
				curLine = '%s:\t %s\n' %(val,exportFullExplanation[idx])
			myfile.write(curLine)

#########################################################################

def newOrAdd(fname,header,data,skippedData):
	if not isfile(fname):
		# write new
		if skippedData:
			loging.warning(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' - ' + basename(__file__) + '\nWARNING first file had skipped data. Output file will be inconsistent, change order')
			warn('\nWARNING first file had skipped data. Output file will be inconsistent, change order')
		with open(fname, 'w',newline='') as myfile:
			wr = csv.writer(myfile)
			wr.writerow(header)
			for row in data:
				missingValue = [missingValue for missingValue,value in enumerate(row) if value == None]
				for i in missingValue:
					row[i] = 'NaN'
				wr.writerow(row)

	else:
		# append
		with open(fname, 'r') as myfile:
			reader = csv.reader(myfile)
			existingHeaders = list(next(reader))


		if len(existingHeaders) != len(header):
			if skippedData:
				newData = pd.DataFrame([np.nan]*len(existingHeaders),columns = ['newData'])
				for idx,val in enumerate(existingHeaders):

					tmp = [data[i] for i,s in enumerate(header) if val == s] # if the new header corresponds to the old header
					if len(tmp) != 0:
						newData.loc[idx,'newData'] = tmp[0]
					else:
						newData.loc[idx,'newData'] = None
					# if idx in header:
					# 	index = [i for i, s in enumerate(header) if val == s]
					# 	newData[idx] = data[index]
				# print(data)
				data = newData['newData'].values.tolist()
				# print(data)
			else:
				loging.warning(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' - ' + basename(__file__) + '\noriginal file did not have the same number of columns as the newly exported data.\nConsider creating a new file or at least edit the existingHeaders.')
				warn('\nWARNING: original file did not have the same number of columns as the newly exported data.\nConsider creating a new file or at least edit the existingHeaders.')

		with open(fname, 'a',newline='') as myfile:
			wr = csv.writer(myfile)
			if type(data[0]) != list: # only one row
				row = data
				missingValue = [missingValue for missingValue,value in enumerate(row) if value == None]
				for i in missingValue:
					row[i] = 'NaN'
				wr.writerow(row)
			else:
				for row in data:
					# [print('hi'+ i) for i in row]
					missingValue = [missingValue for missingValue,value in enumerate(row) if value == None]
					for i in missingValue:
						row[i] = 'NaN'
					wr.writerow(row)
