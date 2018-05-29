# 30-11-2017 Rens Meerhoff
# Function to create dictionary of existing data
#
# rawData => data required for any FDP analysis (X,Y,PlayerID,TeamID,Timestamp,PlayerRole)
# NB: Ball data should at some point be added here.
# attrData => put existing attribute data in dictionary

import pdb; #pdb.set_trace()
from warnings import warn
import numpy as np
from os.path import isfile, join, isdir
from os import listdir
import CSVexcerpt
import matplotlib.pyplot as plt
from matplotlib import colors as mcolors


if __name__ == '__main__':		
	# rawDataCols = column headers of raw data
	# rawData = actual rawdata (from CSVimportAsColumns.readPosData())
	# timeUnitData = to immediately also export time in seconds.
	# readRawDataCols = rawdata columns of interest
	rawData(rawDataCols,rawData,readRawDataCols,timeUnitData)

	# attributeDataCols = column headers of raw data corresponding to pre-existing attributes
	# attributeData = pre-existing attribute-data
	# readAttributeCols = attributes of interest
	attrData(attributeDataCols,attributeData,readAttributeCols)
	
	# To do: make numpy arrays of all (numeric) vars

########################################################################	

def attrData(attributeDataCols,attributeData,readAttributeCols):
	attrDict = {}
	for idx,val in enumerate(attributeDataCols):
		if val == readAttributeCols[0]:
			Speed = np.empty([len(attributeData[idx]),1],dtype=np.float64)
			for ind,i in enumerate(attributeData[idx]):
				if  not i == '':
					Speed[ind] = np.float64(i)
				else: # missing data					
					Speed[ind] = np.nan
			attrDict['Speed'] = Speed
		elif val == readAttributeCols[1]:
			attrDict['Area'] = attributeData[idx]
		elif val == readAttributeCols[2]:
			attrDict['Perimeter'] = attributeData[idx]
		elif val == readAttributeCols[3]:
			attrDict['CentX'] = attributeData[idx]
		elif val == readAttributeCols[4]:
			attrDict['CentY'] = attributeData[idx]
		elif val == readAttributeCols[5]:
			attrDict['avgDistGroup'] = attributeData[idx]
		elif val == readAttributeCols[6]:
			LPWratio = attributeData[idx]
			# Check if comma decimal separator
			pointDecimal = 'False'
			for j in range(len(LPWratio)):
				for idc,i in enumerate(LPWratio[-j]): # -j, because team variables are usually at the end
					if i == ',':
						# comma-separate decimal
						tmp = list(LPWratio[-j])
						tmp[idc] = '.'
						LPWratio[-j] = ''.join(tmp)					
						break
					elif i == '.':
						pointDecimal = 'True'
						break
				if pointDecimal == 'True':
					# Apparently it was a point decimal aftr all
					break
			attrDict['LPWratio'] = LPWratio
		elif val == readAttributeCols[7]:
			attrDict['groupLength'] = attributeData[idx]
		elif val == readAttributeCols[8]:
			attrDict['groupWidth'] = attributeData[idx]

	return attrDict

################################################################	
################################################################	

def rawData(rawDataCols,rawData,readRawDataCols,timeUnitData):
	rawDict = {}

	# To do (generalizability)
	# - incorporate if 'hh:mm:ss'
	# get rid of strings (and let them be imported)

	for idx,val in enumerate(rawDataCols):
		if val == readRawDataCols[2]:
			# X = rawData[idx]
			X = np.empty([len(rawData[idx]),1],dtype=np.float64)
			for ind,i in enumerate(rawData[idx]):
				if  not i == '':
					X[ind] = np.float64(i)
				else: # missing data (empty cell to be precise)					
					X[ind] = np.nan
			rawDict['X'] = X
		elif val == readRawDataCols[3]:
			Y = np.empty([len(rawData[idx]),1],dtype=np.float64)
			for ind,i in enumerate(rawData[idx]):
				if  not i == '':
					Y[ind] = np.float64(i)
				else: # missing data					
					Y[ind] = np.nan
			rawDict['Y'] = Y
		elif val == readRawDataCols[6]:
			rawDict['PlayerRole'] = rawData[idx]
		elif val == 'TsMS':
			rawTime = np.array([float(i) for i in rawData[idx]])
			# Consider replacing 'TsMS' for 'rawTime'
			rawDict['TsMS'] = rawTime
			rawDict['TsS'] = rawTime * timeUnitData # Time in Seconds
		elif val == 'PlayerID':
			rawDict['PlayerID'] = rawData[idx]
		elif val == 'TeamID':
			rawDict['TeamID'] = rawData[idx]
		else:
			# Column not included (should happen for Timestamp in InMotio time)
			# print('\n<< %s >> is unidentified' %val)
			doNothing = []

	return rawDict