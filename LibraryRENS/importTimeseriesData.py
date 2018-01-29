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

	return rawDict