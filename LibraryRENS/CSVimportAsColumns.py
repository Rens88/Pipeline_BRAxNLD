# 17-11-2017, Rens Meerhoff
# This function reads data from a CSV into lists of specific columns.
# Input required is the CSV-file and the header strings of the columns that need to be exported.
# Output data is in the same order as the input of the column strings.
# Moreover:
# It checks whether the requested column exists, if not, it exports an empty column (with 'None')
# It checks whether a requested duplicate column indeed has a duplicate in the data.
# It checks whether a duplicate column in the data was indeed requested.

import csv
import pdb; #pdb.set_trace()
from warnings import warn
import numpy as np


if __name__ == '__main__':
	# fname = filename; include .csv extension and folder if not in the same folder
	# readCols = a list of strings of the headers that should be imported
	# if readCols is a string (and not a list) with the string "ImportAll", all columns are imported
	# It exports the data in columns
	# And it exports the column headers that were read (if ImportAll)
	readPosData(fname,readCols)


def readPosData(fname,readCols):
	
	# Read the CSV file
	Col = []
	with open(fname, 'r') as f:
    		reader = csv.reader(f)
    		headers = list(next(reader))
    		result = [[c for c in row] for row in reader]
    		for i, header_name in enumerate(headers):
        		# print(header_name, [row[i] for row in result]) # convenient code that prints the data per column
        		Col.append([row[i] for row in result])
	returnHeaders = 'False'
	if readCols == 'ImportAll':
		readCols = headers
		returnHeaders = 'True'
    # Find columns of which the headers correspond to the columns to be read (readCols)
	dataOut = []	
	storedInd = []	
	headers_copy = headers.copy()
	duplInData = 'False'
	duplInData2 = []
	duplInData2_idx = []
	duplRequested2 = []
	duplInData2_idx_incols = []
	for idx, val in enumerate(readCols):		
		duplRequested = 'False'
		colFound = 'False'
		for idx2, val2 in enumerate(headers):
			if val == val2:				
				headers_copy[idx2] = None				
				if not next((x for x in headers_copy if x == val),None) == None:
					# Another occurrence of val2 exists in columnheaders
					# This is used later: if there is a duplicate in the data, but not in the input, then a warning is issued					
					if next((x for x in duplInData2 if x == val),None) == None:
						# Duplicate not yet seen before
						# So: Add 'val' to duplicates in data
						duplInData2.append(val)
						duplInData2_idx.append(idx2)	
						duplInData2_idx_incols.append(len(dataOut))				
				if not next((x for x in storedInd if x == idx2),None) == None:
					# Idx2 was already identified before, which means this is the second time 'val' is sought for.
					# Problem: An input duplicate.
					# Solution: don't export current idx2, don't break the loop. But continue and find a unique idx2
					duplRequested = 'True'
					if next((x for x in duplRequested2 if x == val),None) == None:
						duplRequested2.append(val)
					warn('\nDuplicate in input <<%s>>, check which one is which\n' %val)					
					# so continue and take the next
				else:
					dataOut.append(Col[idx2])
					storedInd.append(idx2)
					colFound = 'True'
					break
		if colFound == 'False':
			if duplRequested == 'False':
				if val != None:
					warn('\nColumn << %s >> not found\n' %val)
				# else: ## NB: I believe this line is obsolete.
				# 	warn('\nColumn << %s >> not found from << %s >>\n' %(val,headers[idx2]))
			else:
				warn('\nDuplicate column << %s >> requested, but duplicate not found\n' %val)
			# Export empty list to avoid data confusion
			dataOut.append([None] * len(Col[0]))
	# And finally, check if all duplicates in the data were indeed requested
	for ind,i in enumerate(duplInData2):
		if next((x for x in duplRequested2 if x == i),None) == None:
			# There is no dupl requested, but there was a duplicate in the data
			# This means that the wrong data may be selected

			# Export data as duplicate:
			tmp = [idx for idx,x in enumerate(headers) if headers[duplInData2_idx[ind]] == x]
			for idx in tmp[1:]:
				dataOut.append(Col[idx])
			# warn('\nColumn << %s >> had an unrequested duplicate in the data: wrong data may be exported.\nDuplicate is exported in column %i.' %(i,len(dataOut)))
			warn('\nColumn << %s >> had an unrequested duplicate in the data: wrong data may be exported.\n' %i)

			# TO DO: Automate duplicate identification. 
			# Export len(dataOut)-1 here as the index for the column with duplicates
			dataOut.append([duplInData2_idx_incols[ind],len(dataOut)-1])
	for ind,i in enumerate(duplInData2):
		if next((x for x in duplRequested2 if x == i),None) != None:
			# There was a duplicate requested (and recognized)
			# Export columns of duplicate			
			dataOut.append([idx for idx,val in enumerate(readCols) if val == i]) # code only functional for ONE requested dupplicate


	if returnHeaders == 'True':
		return dataOut,headers
	else:
		return dataOut