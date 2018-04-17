# 06-12-2017 Rens Meerhoff
# Script to export a CSV. Check whether the file already exists and then adds it.

import pdb; #pdb.set_trace()
import csv
from warnings import warn
# import numpy as np
from os.path import isfile, join, exists#, isdir, exists
from os import listdir, startfile
import pandas as pd
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

def eventAggregate(eventAggFolder,eventAggFname,appendEventAggregate,eventExcerptPanda,skipEventAgg_curFile):
	if skipEventAgg_curFile:
		warn('\nDid not export any new eventAggregate data.\nIf you want to add new (or revised) spatial aggregates, change <skipEventAgg> into <False>.\n')
		return appendEventAggregate
		
	if exists(eventAggFolder + eventAggFname) and appendEventAggregate:# and not stat(eventAggFolder + eventAggFname).st_size == 0: 
		# Store the trial based index
		eventExcerptPanda.index.name = 'TrialBasedIndex'
		# Create a new index
		eventExcerptPanda.reset_index()
		eventExcerptPanda.index.name = 'DataSetIndex'


		# Load the existing file
		existingData = pd.read_csv(eventAggFolder + eventAggFname, low_memory = False, index_col = 'DataSetIndex')
		# Concat with new eventExcerpt
		try:
			combinedData = pd.concat([existingData, eventExcerptPanda], axis = 0,ignore_index = True)
			## This should also work:
			#result = df1.append(dicts, ignore_index=True)

		except:
			print('****************************')
			print('****************************')
			print('\n************ WARNING *****************')
			print('Had to drop a duplicate column. For some reason, one of the columns appeared twice.')
			print('Original keys existing data:')
			print(existingData.keys())
			print('Length:')
			print(len(existingData.keys()))
			print('Original keys new data:')
			print(eventExcerptPanda.keys())
			print('Length:')
			print(len(eventExcerptPanda.keys()))
			print('maybe it\'s empty:')
			print(eventExcerptPanda.empty)

			eventExcerptPanda = eventExcerptPanda.drop_duplicates()
			# combinedData = pd.concat([existingData, eventExcerptPanda], axis = 0)
			combinedData = pd.concat([existingData, eventExcerptPanda], axis = 0,ignore_index = True)


			print('New keys:')
			print(eventExcerptPanda.keys())
			print('Length:')
			print(len(eventExcerptPanda.keys()))
			print('\n************ WARNING *****************')
			print('****************************')
			print('****************************')
			print('****************************')


		# Save as new csv (overwrite)
		combinedData.to_csv(eventAggFolder + eventAggFname, index_label = 'DataSetIndex')
		# --> store current label as TrialBasedIndex

		# tmp = np.arange(0,len(eventExcerptPanda['Ts']))
		# eventExcerptPanda.reindex(tmp)

		# eventExcerptPanda.reset_index()

		# combinedData.to_csv(eventAggFolder + eventAggFname, index_label = 'TrialBasedIndex')
		# --> create new index that starts at 0 and has unique values for each row in the aggregated file

		# with open(eventAggFolder + eventAggFname,'a') as f:			
		# 	eventExcerptPanda.to_csv(f,header=False)
		# # match columns..
		# # print('!!!!!!!!!!!!')
		# # pdb.set_trace()
	elif not eventExcerptPanda.empty:
		# Store the trial based index
		eventExcerptPanda.index.name = 'TrialBasedIndex'
		print(eventExcerptPanda.keys())
		# Create a new index
		eventExcerptPanda.reset_index(drop=False)
		eventExcerptPanda.index.name = 'DataSetIndex'
		print(eventExcerptPanda.keys())
		pdb.set_trace()
		# Create a new file
		eventExcerptPanda.to_csv(eventAggFolder + eventAggFname, index_label = 'DataSetIndex')
		appendEventAggregate = True
		
	else: # apparently eventExcerptPanda was empty..
		warn('\nWARNING: Targetevents were empty. \nNo Data exported.\n')
	
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
		startfile(filename)

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
				newData = [999]*len(existingHeaders)
				for idx,val in enumerate(existingHeaders):

					tmp = [data[i] for i,s in enumerate(header) if val == s] # if the new header corresponds to the old header
					if len(tmp) != 0:
						newData[idx] = tmp[0]
					else:
						newData[idx] = None
					# if idx in header:
					# 	index = [i for i, s in enumerate(header) if val == s]
					# 	newData[idx] = data[index]
				# print(data)
				data = newData
				# print(data)
			else:
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
