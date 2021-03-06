# 06/03/2018 Rens Meerhoff
# New way to import timeseries, as a pandas data frame
# One of the important steps is that from here on onward the rawdata
# columns now use their systematic indications, and no longer 

import csv
import pdb; #pdb.set_trace()
import numpy as np
from os.path import isfile, join, isdir
from os import listdir, path
from warnings import warn
import time

# From my own library:
import plotSnapshot
# import CSVexcerpt
# import CSVimportAsColumns
# import identifyDuplHeader
# import LoadOrCreateCSVexcerpt
# import individualAttributes
# import plotTimeseries
# import dataToDict
# import dataToDict2
import safetyWarning
# import exportCSV
import pandas as pd

if __name__ == '__main__':
	
	process(loadFname,loadFolder,skipSpatAgg_curFile,readAttributeCols,readEventColumns,attrLabel,outputFolder,debuggingMode)
	rawData(filename,folder)
	existingAttributes(filename,folder,rawHeaders)

def process(loadFname,loadFolder,skipSpatAgg_curFile,readAttributeCols,readEventColumns,attrLabel,outputFolder,debuggingMode):

	tImport = time.time()	# do stuff

	rawPanda = rawData(loadFname,loadFolder)
	attrPanda,attrLabel = existingAttributes(loadFname,loadFolder,skipSpatAgg_curFile,readAttributeCols,attrLabel,outputFolder)
	eventsPanda,eventsLabel = existingAttributes(loadFname,loadFolder,False,readEventColumns,attrLabel,outputFolder)

	if debuggingMode:
		elapsed = str(round(time.time() - tImport, 2))
		print('***** Time elapsed during imporTimeseries: %ss' %elapsed)

	return rawPanda,attrPanda,attrLabel,eventsPanda,eventsLabel

def existingAttributes(filename,folder,skipSpatAgg,headers,attrLabel,outputFolder):
		
	# Add time to the attribute columns (easy for indexing)
	if 'Ts' not in headers:
		colHeaders = ['Ts'] + headers # This makes sure that timeStamp is also imported in attribute cols, necessary for pivoting etc.
		attrLabel.update({'Ts': 'Time (s)'})

	# colHeaders = headers
	# Only read the headers as a check-up:
	with open(folder+filename, 'r') as f:
		reader = csv.reader(f)
		tmpHeaders = list(next(reader))

	if skipSpatAgg:			
		# Import all headers
		colHeaders = tmpHeaders[1:] # Skip the index column which is empty
		## EDIT: Instead of exporting the attributes labels, 
		## it's easier to create the attribute lables, 
		## EVEN if spatAgg is being skipped.
		#
		# if isfile(join(outputFolder, 'attributeLabel.csv')):
		# 	print('loaded attributeLabel')
		# 	pdb.set_trace()

		# 	tmp = pd.read_csv(outputFolder + 'attributeLabel.csv')
		# 	attrLabel = pd.DataFrame.to_dict(tmp)
		# else:
		# 	warn('\nWARNING: Could not find <attributeLabel.csv> in <%s>. Consider running a file with skipSpatAgg = False to export the attribute lables to a csv.' %outputFolder)

	for i in colHeaders:
		if not i in tmpHeaders:
			exit('EXIT: Column header <%s> not in column headers of the file:\n%s\n\nSOLUTION: Change the user input in \'process\' \n' %(i,tmpHeaders))

	# Import existing attributes
	# NB: Indexing by timestamp (adding "index_col='Timestamp'") requires
	# being certain that the timestamps are exactly the same for each person.
	# This could be done, but should be included in the cleanup.
	Loaded_Attr_Data = pd.read_csv(folder + filename, 
		usecols=(colHeaders),low_memory=True)

	return Loaded_Attr_Data,attrLabel

def rawData(filename,folder):
	
	ts = 'Ts'
	x,y = ('X','Y')
	ID = 'PlayerID'
	team = 'TeamID'

	# If these are not the column headers of the file, then rename the headers in the cleanupdata module.
	colHeaders = [ts,x,y,ID,team]

	# Import existing attributes
	# NB: Indexing by timestamp (adding "index_col='Timestamp'") requires
	# being certain that the timestamps are exactly the same for each person.
	# This could be done, but should be included in the cleanup.
	Loaded_Pos_Data = pd.read_csv(folder + filename, 
		usecols=(colHeaders),low_memory=True,
		dtype = {'Ts': float,'X': float, 'Y': float, 'PlayerID': str,'TeamID': str})
	
	return Loaded_Pos_Data