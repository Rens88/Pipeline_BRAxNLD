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
	rawData(filename,folder)
	existingAttributes(filename,folder,rawHeaders)

def existingAttributes(filename,folder,headers):

	colHeaders = headers
	# Only read the headers as a check-up:
	with open(folder+filename, 'r') as f:
		reader = csv.reader(f)
		tmpHeaders = list(next(reader))

	for i in colHeaders:
		if not i in tmpHeaders:
			exit('EXIT: Column header <%s> not in column headers of the file:\n%s\n\nSOLUTION: Change the user input in \'process\' \n' %(i,tmpHeaders))

	# Import existing attributes
	# NB: Indexing by timestamp (adding "index_col='Timestamp'") requires
	# being certain that the timestamps are exactly the same for each person.
	# This could be done, but should be included in the cleanup.
	Loaded_Attr_Data = pd.read_csv(folder + filename, 
		usecols=(colHeaders),low_memory=True)

	return Loaded_Attr_Data

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
		usecols=(colHeaders),low_memory=False)
	
	return Loaded_Pos_Data