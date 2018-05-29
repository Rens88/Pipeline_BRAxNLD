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
import math
import re

if __name__ == '__main__':		
	# rawDataCols = column headers of raw data
	# rawData = actual rawdata (from CSVimportAsColumns.readPosData())
	# timeUnitData = to immediately also export time in seconds.
	# readRawDataCols = rawdata columns of interest
	rawData(rawDataCols,rawData,readRawDataCols,timeUnitData,conversionToMeter)

	# attributeDataCols = column headers of raw data corresponding to pre-existing attributes
	# attributeData = pre-existing attribute-data
	# readAttributeCols = attributes of interest
	attrData(attributeDataCols,attributeData,readAttributeCols)
	
	# To do: make numpy arrays of all (numeric) vars

########################################################################	

def attrData(attributeDataCols,attributeData):
	attrDict = {}
	attrLabel = {}
	for idx,val in enumerate(attributeDataCols):
		if val == 'Speed':
			Speed = np.empty([len(attributeData[idx]),1],dtype=np.float64)
			for ind,i in enumerate(attributeData[idx]):
				if  not i == '':
					Speed[ind] = np.float64(i)
				else: # missing data					
					Speed[ind] = np.nan
			attrDict['Speed'] = Speed
			attrLabel['Speed'] = 'Speed (m/s)'
		elif val == 'LPWratio':
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
			attrLabel['Speed'] = 'LPW Ratio'			
		else:			
			attrDict[val] = attributeData[idx]
			attrLabel[val] = val

	return attrDict,attrLabel

################################################################	
################################################################	

def rawData(cols2read,rawData,readRawDataCols,conversionToMeter):
	

	# To do (generalizability)
	# - incorporate if 'hh:mm:ss'
	# get rid of strings (and let them be imported)
	# tmpLocation = readRawDataCols['Location']
	# print(readRawDataCols['Location'][1])
	rawDict = {'Time':{'Ts':[],'TsS':[],'TsMS':[]}, \
	'Entity':{'PlayerID':[],'TeamID':[],'PlayerRole':[]}, \
	'Location':{'X':[],'Y':[]}}

	for idx,val in enumerate(cols2read):
		if not val == None:
			if readRawDataCols['Time']['Ts'] == val:
				rawDict['Time']['Ts'] = rawData[idx]
				
			elif readRawDataCols['Time']['TsMS'] == val:
				# print('Im here')
				TsMS = np.empty([len(rawData[idx]),1],dtype=np.float64)
				for ind,i in enumerate(rawData[idx]):
					if  not i == '':
						TsMS[ind] = np.float64(i)
					else: # missing data (empty cell to be precise)					
						TsMS[ind] = np.nan
				rawDict['Time']['TsMS'] = TsMS
			elif readRawDataCols['Time']['TsS'] == val:
				# print('Im here as well')
				TsS = np.empty([len(rawData[idx]),1],dtype=np.float64)
				for ind,i in enumerate(rawData[idx]):
					if  not i == '':
						TsS[ind] = np.float64(i)
					else: # missing data (empty cell to be precise)					
						TsS[ind] = np.nan
				rawDict['Time']['TsS'] = TsS

			elif readRawDataCols['Entity']['PlayerID'] == val:
				rawDict['Entity']['PlayerID'] = rawData[idx]
			elif readRawDataCols['Entity']['TeamID'] == val:
				rawDict['Entity']['TeamID'] = rawData[idx]
			elif readRawDataCols['Entity']['PlayerRole'] == val:
				rawDict['Entity']['PlayerRole'] = rawData[idx]	

			elif readRawDataCols['Location'][0] == val:		
				X = np.empty([len(rawData[idx]),1],dtype=np.float64)
				for ind,i in enumerate(rawData[idx]):
					if  not i == '':
						X[ind] = np.float64(i)*conversionToMeter
					else: # missing data (empty cell to be precise)					
						X[ind] = np.nan
				rawDict['Location']['X'] = X
			elif readRawDataCols['Location'][1]== val:
				Y = np.empty([len(rawData[idx]),1],dtype=np.float64)
				for ind,i in enumerate(rawData[idx]):
					if  not i == '':
						Y[ind] = np.float64(i)*conversionToMeter
					else: # missing data					
						Y[ind] = np.nan
				rawDict['Location']['Y'] = Y
			else:
				# Column not included (should happen for Timestamp in InMotio time)
				warn('\n\nCould not find the correct string to allocate raw data.\n\n')
	
	if rawDict['Time']['TsMS'] == []:
		# need to compute timestamp in milliseconds		
		if not rawDict['Time']['TsMS'] == []:
			rawDict['Time']['TsS'] = rawDict['Time']['TsMS'] * 1000
		elif not rawDict['Time']['Ts'] == []:
			tmpInMilliSec = np.empty([len(rawDict['Time']['Ts']),1],dtype=np.float64)
			for i in range(len(rawDict['Time']['Ts'])):
				
				# To avoid rounding errors and reading problems, 
				# I used a regular expression to convert a timestamp to numbers.
				# Timestamp can either be h+:mm:ss.d+ or m+:ss.d+
				# First assume there are 2 ':'
				tmpString = rawDict['Time']['Ts'][i]
				regex = r'(\d+):(\d{2}):(\d{2})\.(\d+)'
				match = re.search(regex,tmpString)
				if match:
					grp = match.groups()
					DecimalCorrection = 10 ** len(grp[2])

					timestamp = int(grp[0])*3600*DecimalCorrection +\
								int(grp[1])*60*DecimalCorrection +\
								int(grp[2])*1*DecimalCorrection +\
								int(grp[3])
					
				else:
					# Apparently, sometimes there's only one
					regex = r'(\d+):(\d{2})\.(\d+)'
					match = re.search(regex,tmpString)
					if match:
						grp = match.groups()
						DecimalCorrection = 10 ** len(grp[2])

						timestamp = int(grp[0])*60*DecimalCorrection +\
									int(grp[1])*1*DecimalCorrection +\
									int(grp[2])
					else:
						warn('\nCould not decipher timestamp.')

				tmpInMilliSec[i] = timestamp / DecimalCorrection * 1000

				# if rawDict['Time']['Ts'][i].count(':') == 1:
				# 	mins,secs = rawDict['Time']['Ts'][i].split(':')
				# 	tmp[i] = (float(mins)*60 + float(secs)) * 1000
				# elif rawDict['Time']['Ts'][i].count(':') == 2:
				# 	hrs,mins,secs = rawDict['Time']['Ts'][i].split(':')
				# 	tmp[i] = ( (float(hrs)*60+float(mins)) *60 + float(secs) ) * 1000
				# else:
				# 	warn('\nCould not decipher timestamp.')
								
			rawDict['Time']['TsMS'] = tmpInMilliSec			
		else:
			warn('\n\nCould not find a timestamp.')
		
	if rawDict['Time']['Ts'] == []:
		# need to compute timestamp in mm:ss
		# From TsMS to Ts
		if not rawDict['Time']['TsMS'] == []:
			tmp = [None]*len(rawDict['Time']['TsMS'])
			for i,val in enumerate(rawDict['Time']['TsMS']):
				mins = math.floor(val / 60000)
				secs = (val - mins*60000) / 1000
				tmp[i] = ['%s:%s' %(str(mins),str(secs[0]))]
			rawDict['Time']['Ts'] = tmp
		else:
			warn('\n\nCould not find a timestamp. (again)\nTsMS should already have been computed.')

	if rawDict['Time']['TsS'] == []:
		# need to compute timestamp in seconds
		if not rawDict['Time']['TsMS'] == []:
			rawDict['Time']['TsS'] = rawDict['Time']['TsMS'] / 1000
		else:
			warn('\n\nCould not find a timestamp. (again)\nTsMS should already have been computed.')

	# From TsMS to Ts
	tmp = [None]*len(rawDict['Time']['TsMS'])
	for i,val in enumerate(rawDict['Time']['TsMS']):
		mins = math.floor(val / 60000)
		secs = (val - mins*60000) / 1000
		tmp[i] = ['%s:%s' %(str(mins),str(secs[0]))]

	return rawDict