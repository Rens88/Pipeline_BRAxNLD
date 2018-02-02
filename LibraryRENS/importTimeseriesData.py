# 08-12-2017 Rens Meerhoff

import csv
import pdb; #pdb.set_trace()
import numpy as np
from os.path import isfile, join, isdir
from os import listdir, path
from warnings import warn

# From my own library:
import plotSnapshot
import CSVexcerpt
import CSVimportAsColumns
import identifyDuplHeader
import LoadOrCreateCSVexcerpt
import individualAttributes
import plotTimeseries
import dataToDict
import dataToDict2
import safetyWarning
import exportCSV

if __name__ == '__main__':
	rawData(filename,folder,headers,conversionToMeter)
	existingAttributes(filename,folder,headers)

def existingAttributes(filename,folder,headers):
	# Import existing attributes
	attributeData = CSVimportAsColumns.readPosData(folder + filename,headers)
	# Allocate 'attribute'-variables from CSV to local variables
	attributeDict = dataToDict2.attrData(headers,attributeData)

	return attributeDict

def rawData(filename,folder,headers,conversionToMeter):
	# First, create an empty dictionary
	readRawDataCols = {'Time':{'Ts':None,'TsMS':None,'TsS':None}, \
	'Entity':{'PlayerID':None,'TeamID':None,'PlayerRole':None}, \
	'Location':None}	
	# Then, for all terms in the dictionary, add them.
	timeIn = False
	locationIn = False
	entityIn = False

	for i in headers.keys():
		if 'Ts' in i:
			readRawDataCols['Time'][i] = headers[i]
			timeIn = True
		elif 'ID' in i:
			readRawDataCols['Entity'][i] = headers[i]
			entityIn = True
		elif 'PlayerRole' == i:
			readRawDataCols['Entity'][i] = headers[i]
		elif i == 'Location':
			locationIn = True
			readRawDataCols['Location'] = headers[i]
		else:
			warn('\nCouldnt identify data entry. Stick to Time, Entity and Location notation')

	if timeIn == False:		
		warn('\nNo time intput found: \nConsider putting in a header string for time (HH:MM, s, or ms)')
	if entityIn == False:
		warn('\nNo entity intput found: \nConsider putting in a header string for entity (TeamID, PlayerID, or PlayerRole)')		
	if locationIn == False:
		warn('\nNo location intput found: \nConsider putting in a header string for entity (Location)')		
	
	cols2read = [readRawDataCols['Time']['Ts'],\
		readRawDataCols['Time']['TsMS'],\
		readRawDataCols['Time']['TsS'],\
		readRawDataCols['Entity']['PlayerID'],\
		readRawDataCols['Entity']['TeamID'],\
		readRawDataCols['Entity']['PlayerRole'],\
		readRawDataCols['Location'][0],\
		readRawDataCols['Location'][1] ]

	# Load data
	rawData = CSVimportAsColumns.readPosData(folder + filename,cols2read)
	# Allocate 'rawData'-variables from CSV to local variables
	rawDict = dataToDict2.rawData(cols2read,rawData,readRawDataCols,conversionToMeter)

	# A security measure to pick up any inconsistencies in timestamp
	# Could potentially be expanded with some automatic corrections
	timestampIssues = False
	PlayerID = rawDict['Entity']['PlayerID']
	TsS = rawDict['Time']['TsS']
	Ts = rawDict['Time']['Ts']
	uniqueTsS,tmp = np.unique(TsS,return_counts=True)
	uniquePlayers = np.unique(PlayerID)

	if any(tmp != np.median(tmp)) or len(uniqueTsS) != len(PlayerID) / len(uniquePlayers):
		# Problem with timestamp. Not every timestamp occurs equally often and/or there isn't the expected number of unique timestamps
		# singleoccurrence = [uniqueTsS[idx] for idx,val in enumerate(tmp) if val == 1]
		# for i in singleoccurrence:
		# 	TsS[np.where(TsS == i)] = i
		warn('\n!!!!!\nProblem with timestamp: Not every timestamp occurs equally often.\n!!!!!')
		indices = np.where(tmp != np.median(tmp))
		for i in np.nditer(indices):
			i2 = np.where(uniqueTsS[i]==TsS)
			# print('Timestamp <%s> occurred <%s> times.' %(uniqueTsS[i],tmp[i]))
			print('i2 = %s' %np.nditer(i2)[0])
			print('Timestamp <%s> occurred <%s> times.' %(Ts[np.nditer(i2)[0]],tmp[i]))
		timestampIssues = True
	return rawDict,timestampIssues