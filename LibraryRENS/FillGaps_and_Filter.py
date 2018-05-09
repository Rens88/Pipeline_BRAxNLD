# 25-04-2018
# Generic module that can be used for filling gaps and filtering.

import matplotlib.pyplot as plt

import pdb; #pdb.set_trace()
from warnings import warn
import numpy as np
import numpy as np
from scipy.interpolate import InterpolatedUnivariateSpline,interp1d
import pandas as pd
import math
from scipy.signal import butter,lfilter,filtfilt


if __name__ == '__main__':
	process(df)
########################################################################

def process(df,**kwargs):
	if 'datasetFramerate' in kwargs:
		df_filled = fillGaps(df, checkForJumps = True,datasetFramerate =  kwargs['datasetFramerate'])
	else:
		df_filled = fillGaps(df, checkForJumps = True)

	if max(df_filled['Ts']) == float("inf"):
		# an issue with filtering.. Havent resolved it yet.
		warn('\n(potentially FATAL) WARNING: Max Ts after filling gaps = %s' %max(df_filled['Ts']))

	df_filled_filtered = filterData(df_filled)

	if max(df_filled_filtered['Ts']) == float("inf"):
		# an issue with filtering.. Havent resolved it yet.
		warn('\n(potentially FATAL) WARNING: Max Ts after filtering = %s' %max(df_filled_filtered['Ts']))
	return df_filled_filtered
########################################################################

def filterData(df):
	# never use datasetframerate, because interpolation would have happened before anyway

	# Compute dt for the whole dataset to establish threshold and framerate.
	dataDrivenFrameRate,dataDrivenThreshold = deriveFramerate(df)
	# if 'datasetFramerate' in kwargs:
	# 	datasetFramerate = kwargs['datasetFramerate']
	# 	frameRateForInterpolation = datasetFramerate
	# 	if not dataDrivenFrameRate == datasetFramerate:
	# 		warn('\nWARNING: The specified datasetFramerate <%s> does not correspond with the dataDrivenFrameRate <%s>.\nIf the difference is substantial, this may affect the reliability of the results.\n' %(datasetFramerate,dataDrivenFrameRate))
	# else:
	# 	frameRateForInterpolation = dataDrivenFrameRate

	everyPlayerIDs = pd.unique(df['PlayerID'])

	# Prepare butterworth filter
	# Filter requirements.
	# order = 6
	# fs = frameRateForInterpolation      # sample rate, Hz
	# cutoff = 0.001# desired cutoff frequency of the filter, Hz

	for nthPlayer, everyPlayerID in enumerate(everyPlayerIDs):

		curRows = df.loc[df['PlayerID'] == everyPlayerID].index
		curTs = df.loc[curRows,'Ts']
		for key in ['X','Y']:


			# Some fancy moving average filter
			windowSize = 1.5 # in seconds
			windowSizeForFilter = np.round((windowSize*dataDrivenFrameRate),0)*2 +1

			Y = df.loc[curRows,key].as_matrix()
			yhat = savitzky_golay(Y, windowSizeForFilter, 3) # window size 51, polynomial order 3
			df.loc[curRows,key] = yhat

			starts,ends = findJumps(curTs,dataDrivenThreshold,dataDrivenFrameRate,returnStartsEnds = True)
			if len(starts) > 1:
				warn('\WARNING: Jump in the data detected. Pipeline currently not equiped to deal with this.\nFinish the filterData module in FillGaps_and_filter.py.\n**********\n!!!!!!!!!!!')

			# for idx,st in enumerate(starts):
			# 	en = ends.iloc[idx]

			# 	print(st)
			# 	print(ends.iloc[idx])

			# 	min(curTs - st)
			# 	print([curTs == st].index)
			# 	# Y = df.loc[[curTs == st].index : [curTs == en].index,key].as_matrix()
			# 	Y = df.loc[curTs >= st & curTs <= en,key].as_matrix()#[curTs == st].index : [curTs == en].index,key].as_matrix()

			# 	# Some fancy moving average filter
			# 	windowSize = 1.5 # in seconds
			# 	windowSizeForFilter = np.round((windowSize*2*dataDrivenFrameRate)+1,1)
			# 	yhat = savitzky_golay(Y, windowSizeForFilter, 3) # window size 51, polynomial order 3

			# # for jumps (if they exist)
			# pdb.set_trace()
			# print('---')
			# print(starts)
			# print(ends)
			# print('---')
			# if nthPlayer <2:
			# 	continue
			# pdb.set_trace()
			# Y = df.loc[curRows,key].as_matrix()

			# # # Use butterworth ##--> doesnt work so nicely. Not sure about parameters.
			# # tmp = butter_lowpass_filter(Y, cutoff, fs, order)

			# # Some fancy moving average filter
			# windowSize = 1.5 # in seconds
			# windowSizeForFilter = np.round((windowSize*2*dataDrivenFrameRate)+1,1)
			# yhat = savitzky_golay(Y, windowSizeForFilter, 3) # window size 51, polynomial order 3

			# # IDEA: Look for higher gradient then expected?

			# plt.figure()
			# plt.plot(curTs, Y,'o',label = 'Raw')
			# plt.plot(curTs, yhat,'.',label = 'Filtered')
			# plt.legend()
			# plt.show()
			# df.loc[curRows,key] = yhat


			# # Look for place where velocity is too high
			# # Delete
			# # Fillgaps (using interpolate with given time values)

	df_filtered = df
	return df_filtered

def butter_lowpass(cutoff, fs, order=5):
    nyq = 0.5 * fs
    normal_cutoff = cutoff / nyq
    print(normal_cutoff)
    b, a = butter(order, normal_cutoff, btype='low', analog=False)
    return b, a

def butter_lowpass_filter(data, cutoff, fs, order=5):
    b, a = butter_lowpass(cutoff, fs, order=order)
    y = lfilter(b, a, data)
    return y

def fillGaps(df,**kwargs):

	checkForJumps = False
	excludeJumps = False
	eventInterpolation = False
	if 'checkForJumps' in kwargs:
		checkForJumps = kwargs['checkForJumps']
	if 'eventInterpolation' in kwargs:
		# Always checks for jumps, en then omits anything up until the last jump
		eventInterpolation = kwargs['eventInterpolation']
		if eventInterpolation:
			# 1) Exclude any jumps before the last continuous timeseries
			# 2) Use fixed X_int (so all events have the exact same time steps)
			if 'fixed_X_int' in kwargs and 'aggregateLevel' in kwargs:
				fixed_X_int = kwargs['fixed_X_int']
				aggregateLevel = kwargs['aggregateLevel']
			else:
				warn('\nFATAL WARNING: When using fillGaps to interpolate before an event (in order to have the exact same timestamps), the module HAS TO BE CALLED with <fixed_X_int> as a keyword argument.\n<fixed_X_int> and <aggregateLevel> are used as a consistent timestamp to be able to compare with each event.\nWithout these, the pipeline will crash at the plotting at dataset level.')

	# Check if 'jumps' exist, because it should crop the window selected
	# A jump is based on the median of the framerates per frame. To be presize, a deviation 1.5 times the median
	# NB: Can also be based on an absolute window..

	# everyPlayerIDs = pd.unique(df['PlayerID'])
	everyPlayerIDs = df['PlayerID'].unique()
	everyPlayerIDs = pd.DataFrame(everyPlayerIDs,columns = ['everyPlayerIDs'])
	# print(type(everyPlayerIDs))
	# print(everyPlayerIDs)
	# # everyPlayerIDs = everyPlayerIDs[~np.isnan(everyPlayerIDs)]
	if any(everyPlayerIDs[everyPlayerIDs['everyPlayerIDs'].notnull()]):
		warn('\nWARNING: Found a row without a PlayerID that could neither be labelled as a ball-row, nor a groupRow.\nConsider checking the raw data file and filling in the empty PlayerID rows.\n')

	everyPlayerIDs = everyPlayerIDs[everyPlayerIDs['everyPlayerIDs'].notnull()]
	# # print(everyPlayerIDs.isnull())
	# print(everyPlayerIDs)
	# pdb.set_trace()
	uniqueTimes = pd.unique(df['Ts'])
	smallestTime = min(uniqueTimes)
	biggestTime = max(uniqueTimes)

	if smallestTime == biggestTime:
		warn('\nFATAL WARNING: There seems to be a problem with the length of the timeseries that is currently being interpolated.\nThe timeseries as the same tStart <%s> as tEnd <%s>.\nIn other words, it has a length of only 1 timestamp.\nThe working solution is to simply return the original eventExcerpt.\n' %(smallestTime,biggestTime))
		return df

	dataDrivenFrameRate,dataDrivenThreshold = deriveFramerate(df)
	if 'datasetFramerate' in kwargs:
		datasetFramerate = kwargs['datasetFramerate']
		frameRateForInterpolation = datasetFramerate
		if not dataDrivenFrameRate == datasetFramerate:
			warn('\nWARNING: The specified datasetFramerate <%s> does not correspond with the dataDrivenFrameRate <%s>.\nIf the difference is substantial, this may affect the reliability of the results.\n' %(datasetFramerate,dataDrivenFrameRate))
	else:
		frameRateForInterpolation = dataDrivenFrameRate
	interpolatedVals = pd.DataFrame([],columns = df.keys())

	for nthPlayer, everyPlayerID in enumerate(everyPlayerIDs['everyPlayerIDs']):
		# The interpolation has to happen per player
		curRows = df.loc[df['PlayerID'] == everyPlayerID].index
		curTs = df.loc[curRows]['Ts']

		# print(everyPlayerID)
		# print('**************')
		# print(df['Ts'])
		# print('**************')
		# print(min(df['Ts']))
		# print(min(curTs))
		# print(curRows)

		# Separately check for smallestTime and biggestTime
		# Not sure how this affects the rest of the code..
		if min(curTs) - min(df['Ts']) > dataDrivenThreshold:
			# 'gap' for this player at the start
			gapSize = min(curTs) - min(df['Ts'])
			warn('\nWARNING:Found a jump in time (of <%ss>) BEFORE the first frame of the current player <%s>.\nCurrently, this gap is ignored when filling gaps.\n Adjust code here if you want to do something with it.\n' %(gapSize,everyPlayerID))

		if max(df['Ts']) - max(curTs) > dataDrivenThreshold:
			# 'gap' for this player at the end
			gapSize = max(df['Ts']) - max(curTs)
			warn('\nWARNING:Found a jump in time (of <%ss>) AFTER the first frame of the current player <%s>.\nCurrently, this gap is ignored when filling gaps.\n Adjust code here if you want to do something with it.\n' %(gapSize,everyPlayerID))

		if eventInterpolation:
			X_int = findJumps_Fixed_X_int(curTs,dataDrivenThreshold,frameRateForInterpolation,fixed_X_int,aggregateLevel)

		elif checkForJumps:

			# try:
			# if not eventInterpolation:
			X_int = findJumps(curTs,dataDrivenThreshold,frameRateForInterpolation)
			# elif:

			# except:
			# 	# print(tStart)
			# 	# print(tEnd)
			# 	print(frameRateForInterpolation)
			# 	print(curTs)
			# 	print(nthPlayer)
			# 	print(curRows)
			# 	print(smallestTime)
			# 	print(biggestTime)
			# 	pdb.set_trace()
		else:
			X_int = determineX_int(curTs,frameRateForInterpolation)

		if len(X_int) != len(np.unique(X_int)):
			warn('\nWARNING: For some reason, the to be interpolated time had duplicate values.\nRemoved them, not sure about consequences.\nMight have something to do with jumps close together.\nCould avoid by making the threshold for jumps larger.\n')
			X_int = np.unique(X_int)

		# print(X_int)
		# np.savetxt("C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\NP repository\\X_int.csv", X_int, delimiter=",")

		# pdb.set_trace()
		# Start by adding time to the interpolated dataFrame
		int_curPlayer = pd.DataFrame(X_int, columns = ['Ts'])

		# Do the interpolation for every key separately (necessary, because some keys need to be interpolated and others not)
		for key in df.columns:

			# Take only the indices that are notnull
			curIdx = curRows[np.where(pd.notnull(df.loc[curRows,key]))]
			if len(curIdx) == 0:
				# Do nothing for this player. There's no data in this key, probably because it's a groupRow
				continue
			if key == 'Ts': # no need to interpolate time, it's added before based on X_int
				continue
			isString = [str_ix for str_ix,q in enumerate(df.loc[curRows,key]) if type(q) == str] 					# Search for rows where the cell has content

			if df.loc[curRows,key].dtype == float:
				# Figure out what type of key it is.
				# If numeric, then interpolate:
				Y = df.loc[curRows,key]
				try:
					f = InterpolatedUnivariateSpline(curTs,Y, k = 3) # order 1) = linear, 2) = quadratic, 3) = cubic
				except:
					print('curTs:')
					print(curTs)
					print('Y:')
					print(Y)
					print('!!!!!!!')
					print('Sometimes, this causes a problem. I think it has something to do with a window that overlaps a time period that doesn\'t exist. (before tStart or after tEnd)).\nIf it occurs, let me know and give me as many details as you can (about the excerpt as well).\n!!!!!!!!\n')
					pdb.set_trace()
				Y_int = f(X_int)
				int_curKey = pd.DataFrame(Y_int, columns = [key])

				# plt.figure()
				# plt.plot(curTs, Y,'o',label = 'Raw')
				# plt.plot(X_int, Y_int,'.',label = 'Interpolated')
				# plt.legend()
				# plt.show()
				# pdb.set_trace()

			else:
				# If they current key is not a float, it's not possible to interpolate.
				# In some cases, it concerns:
				# - event identifiers or player level identifiers
				# - OR string events
				# Event identifiers and player level identifiers can simply be copied:
				if all(df.loc[curRows,key] == df.loc[curRows[0],key]):
					# If they're all the same (i.e., player, team or match identifiers)
					int_curKey = pd.DataFrame([df.loc[curRows[0],key]]*len(X_int), columns = [key], index = int_curPlayer.index)
					if not key in ['TeamID', 'PlayerID', 'temporalAggregate','RefTeam','EventUID','School','Class','Group', 'Test', 'Exp','MatchContinent','MatchCountry','MatchID','HomeTeamContinent','HomeTeamCountry','HomeTeamAgeGroup','HomeTeamID','AwayTeamContinent','AwayTeamCountry','AwayTeamAgeGroup','AwayTeamID' ]:
						warn('\nWARNING: Key <%s> was identified as an event identifier.\nTherefore, no data was interpolated, instead, the same value was copied for all cells.\n' %key)
				else:
					# Create an empty dataframe
					int_curKey = pd.DataFrame([], columns = [key], index = [int_curPlayer.index])

					# Event Strings
					if not key in ['Goal','Run','Possession/Turnover','Pass']:
						warn('\nWARNING: Key <%s> was identified as a string event.\nTherefore, no data was interpolated, instead, the same value was copied to the cell with the interpolated time index closest to the original time index.\nMAKE SURE YOU HAVE FORCED THE DATATYPE CORRECTLY.' %key)
						# if key == 'vNorm':
						# 	print(df.loc[curRows,key].dtype )
						# 	print('still problematic')
						# 	pdb.set_trace()
						# 	Y = df.loc[curRows,key]
						# 	f = InterpolatedUnivariateSpline(curTs,Y, k = 3) # order 1) = linear, 2) = quadratic, 3) = cubic
						# 	Y_int = f(X_int)
						# 	int_curKey = pd.DataFrame(Y_int, columns = [key])

					# Check whether there are less cells with content than cells in X_int AND the original X
					if len(isString) >= len(X_int) or len(isString) >= len(curRows):
						warn('\nWARNING: Identified key <%s> as a column with string events.\n(String events have a string in the row where the event happens, the remaining rows are empty.)\nBut this seems unlikely, as there were more events than time indices.\nThis may result in an error, is there are no cells to interpolate the string events to (they\'re currently transferred to the interpolated time value that lies closest to the original time value, i.e., 1 on 1)\n' %key)
						# (if there are more, it can't have been a string event)

					for stringEvent in isString:
						timeOfEvent = df.loc[curRows,'Ts'].iloc[stringEvent]
						# Find time in X_int that is closest to timeOfEvent
						# nearestTimeOfEvent = [abs(X_int - timeOfEvent) == min(abs(X_int - timeOfEvent))].index
						nearestTimeOfEvent = np.where(abs(X_int - timeOfEvent) == min(abs(X_int - timeOfEvent)))
						# Currently, it's possible that a time index is chosen that has no corresponding positional data. (it may be the one frame before, or the one frame after)

						# Put the stringEvent there, and leave the rest as an empty string i.e., ''
						ix_event = nearestTimeOfEvent[0]
						int_curKey.loc[ix_event,key] = df.loc[curRows,key].iloc[stringEvent]

			# Put interpolated / replaced data from current KEY in frame for current PLAYER
			int_curPlayer = pd.concat([int_curPlayer,int_curKey],axis= 1) # Each key gets added one by one

		# Put interpolated / replaced data from current PLAYER in frame for current FILE
		interpolatedVals = pd.concat([interpolatedVals,int_curPlayer],axis= 0, ignore_index = True)
		# Same same but different:
		# interpolatedVals = interpolatedVals.append(int_curPlayer, ignore_index = True)

		# Order the columns in the original way
		interpolatedVals = interpolatedVals[df.keys()]

	warn('\nWARNING: The threshold for the size of a gap to be determined as a gap was based on the median of the data-driven sampling frequency <%sHz>.\nWith the current sampling frequency, any gap in time longer than <%ss> was considered a jump (and not interpolated between).\nYou may want to manually set the size of a gap that needs to be filled..\n' %(dataDrivenFrameRate, dataDrivenThreshold))
	df_filled = interpolatedVals
	return df_filled

def filter(df):

	return df_filtered

def determineX_int(curTs,frameRateForInterpolation):
	tStart = min(curTs)
	tEnd = max(curTs)
	# print(tStart)
	# print(tEnd)
	# print(frameRateForInterpolation)
	# print(curTs)
	tStartRound = np.round(tStart,math.ceil(math.log(frameRateForInterpolation,10)))
	tEndRound = np.round(tEnd,math.ceil(math.log(frameRateForInterpolation,10)))
	tmp = np.arange(tStartRound,tEndRound + 1/frameRateForInterpolation, 1/frameRateForInterpolation) # referring to eventTime
	X_int = np.round(tmp,math.ceil(math.log(frameRateForInterpolation,10))) # automatically rounds it to the required number of significant numbers (decimal points) for the given sampling frequency (to avoid '0' being stored as 1.474746 E16 AND 1.374750987 E16)
	return X_int

def findJumps(curTs,dataDrivenThreshold,frameRateForInterpolation,**kwargs):
	if 'returnStartsEnds' in kwargs:
		returnStartsEnds = kwargs['returnStartsEnds']
	else:
		returnStartsEnds = False

	# Find Jumps
	t0 = curTs[:-1].reset_index(level=None, drop=False, inplace=False)
	t1 = curTs[1:].reset_index(level=None, drop=False, inplace=False)
	dt = t1-t0
	jumpSizes = dt['Ts'][dt['Ts']>(dataDrivenThreshold)] # Contains the size of the jump and the index with respect to t0 and t1

	firstData = pd.DataFrame(curTs[min(curTs) == curTs])
	lastData = pd.DataFrame(curTs[max(curTs) == curTs])

	dataStarts = firstData
	dataEnds = lastData


	if jumpSizes.empty:
		# No jumps found, simply interpolate with standardaized X_int
		tmp = determineX_int(curTs,frameRateForInterpolation)
		X_int = np.round(tmp,math.ceil(math.log(frameRateForInterpolation,10))) # automatically rounds it to the required number of significant numbers (decimal points) for the given sampling frequency (to avoid '0' being stored as 1.474746 E16 AND 1.374750987 E16)

	else:
		# Jumps found, make sure X_int skips the jumps as well
		# Adjust for jumps in time.
		jumpStarts = t0['Ts'][dt['Ts']>(np.median(dt['Ts'])*1.5)] # SHould it be the frame before jump starts?
		dataEnds = pd.concat([jumpStarts, lastData['Ts']],axis = 0) #-1 # jumpStarts - 1

		jumpEnds = t1['Ts'][dt['Ts']>(np.median(dt['Ts'])*1.5)]  # should it be the frame after jump ends?
		dataStarts = pd.concat([firstData['Ts'], jumpEnds],axis = 0) #-1 # jumpStarts - 1

		tmp = [np.arange(dataStarts.iloc[isx],en + 1/frameRateForInterpolation,1/frameRateForInterpolation) for isx,en in enumerate(dataEnds)]
		tmp = np.concatenate(tmp)
		X_int = np.round(tmp,math.ceil(math.log(frameRateForInterpolation,10))) # automatically rounds it to the required number of significant numbers (decimal points) for the given sampling frequency (to avoid '0' being stored as 1.474746 E16 AND 1.374750987 E16)
		# X_int = np.around(tmp,1) # automatically rounds it to the required number of significant numbers (decimal points) for the given sampling frequency (to avoid '0' being stored as 1.474746 E16 AND 1.374750987 E16)
		# print(math.ceil(math.log(frameRateForInterpolation,10)))

		# np.savetxt("C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\NP repository\\test.csv", X_int, delimiter=",")
		# pdb.set_trace()
	if returnStartsEnds:
		return dataStarts,dataEnds
	return X_int

def findJumps_Fixed_X_int(curTs,dataDrivenThreshold,frameRateForInterpolation,fixed_X_int,aggregateLevel):

	# Don't thing I need this functionality, but saved it just in case:
	start_ix = 0
	end_ix = len(fixed_X_int) + start_ix -1

	X_int = fixed_X_int
	# Find Jumps
	t0 = curTs[:-1].reset_index(level=None, drop=False, inplace=False)
	t1 = curTs[1:].reset_index(level=None, drop=False, inplace=False)
	dt = t1-t0
	jumpSizes = dt['Ts'][dt['Ts']>(dataDrivenThreshold)] # Contains the size of the jump and the index with respect to t0 and t1

	firstData = pd.DataFrame(curTs[min(curTs) == curTs])
	lastData = pd.DataFrame(curTs[max(curTs) == curTs])

	dataStarts = firstData
	dataEnds = lastData


	#################################################################
	# This is where it's different from findJumps()
	#################################################################
	#################################################################
	if jumpSizes.empty:
		# No jumps
		## Using interp1d here to avoid (for example) negative values where they can't exist.
		## Spline interpolation can be used on the raw data. Not on the aggregated data.

		# Adjust first X_int frame in case of missing a frame at the start
		# Crop the missing first frames (if any)
		X_int_cropped_idx = np.where(X_int >= min(curTs))
		start_ixNew = X_int_cropped_idx[0][0] + start_ix
		X_int_cropped = X_int[X_int_cropped_idx]

		start_ix = start_ixNew

		# Crop the missing last frames (if any)
		X_int_cropped_idx = np.where(X_int_cropped <= max(curTs))
		end_ix = len(X_int_cropped_idx[0]) + start_ix -1
		X_int_cropped = X_int_cropped[X_int_cropped_idx]

		""""
		s = interp1d(curTs, curY)

		try:
			Y_int = s(X_int_cropped)
		except:
			print(min(curTs[1:-1]),min(X_int_cropped))
			print(max(curTs[1:-1]),max(X_int_cropped))
			print('---')
			print(curTs)
			print('----')
			print(X_int_cropped)
			print('---')
			print('If a problem occurs here, it has something to do with the window length and the available frames in the raw data. Probably, there is no overlap or something...')
			pdb.set_trace()
		"""
	else:
		# Adjust for jumps in time.
		jumpStarts = t0['Ts'][dt['Ts']>(np.median(dt['Ts'])*1.5)]
		jumpEnds = t1['Ts'][dt['Ts']>(np.median(dt['Ts'])*1.5)]
		endOfLastJump = jumpEnds.iloc[-1]

		curTs_cropped = curTs[curTs >= endOfLastJump]
		# curY_cropped = curY[curTs >= endOfLastJump]

		# It excludes the X_int preceding endOfLastJump
		X_int_cropped = X_int[X_int >= endOfLastJump]

		windowWithoutJumps = max(curTs_cropped) - endOfLastJump
		warn('\nWARNING: Could not use the whole window leading up to this event due to temporal jumps in the data.\nThe intended window was <%ss>, whereas the window from the last jump until the event was <%ss>.\n\n**********\n**********\n**********\nSTILL TO DO: Write an exception for <full> and for other events with a start AND an end.\n**********\n**********\n**********\n' %(aggregateLevel[1],windowWithoutJumps))

		"""
		## Using interp1d here to avoid (for example) negative values where they can't exist.
		## Spline interpolation can be used on the raw data. Not on the aggregated data.
		s = interp1d(curTs_cropped, curY_cropped)
		Y_int = s(X_int_cropped)

		tmp = next(i for i, x in enumerate(X_int) if x > endOfLastJump)
		start_ix = start_ix + tmp -1
		"""
	#################################################################
	#################################################################
	#################################################################
	return X_int_cropped

def deriveFramerate(df):

	# Compute dt for the whole dataset to establish threshold and framerate.
	allTs = df['Ts']
	t0 = allTs[:-1].reset_index(level=None, drop=False, inplace=False)
	t1 = allTs[1:].reset_index(level=None, drop=False, inplace=False)
	dt = t1-t0
	dataDrivenFrameRate = np.round(1/np.median(dt['Ts']),3) # in Hz. You might want to override this with an actual framerate
	dataDrivenThreshold = np.round(1.5/dataDrivenFrameRate,decimals = 3)
	return dataDrivenFrameRate,dataDrivenThreshold

def fillGapsConsistentTimestamps(curEventExcerptPanda,X_int,aggregateLevel,refTeam):
	## admittedly not the prettiest way....
	# It requires a for loop per key, then a for loop per unique playerID....

	everyPlayerIDs = pd.unique(curEventExcerptPanda['PlayerID'])

	newIndex = np.arange(0,len(everyPlayerIDs) * len(X_int))

	if len(newIndex) < 2:
		warn('\nFATAL WARNING: There seems to be a problem with the length of the timeseries that is currently being interpolated.\nThe timeseries as the same tStart <%s> as tEnd <%s>.\nIn other words, it has a length of only 1 timestamp.\nThe working solution is to simply return the original eventExcerpt.\n' %(smallestTime,biggestTime))
		return curEventExcerptPanda
	# Create an array that is empty and has the same size as the interpoloated values will have.
	interpolatedVals = pd.DataFrame(np.nan, index=newIndex, columns=['eventTimeIndex','eventTime','Ts'])

	# Interpolate each key separately
	for key in curEventExcerptPanda.keys():
	# Only interpolate numerical keys.
	# The other keys should be treated differently
	# Do the interpolation separately for every player
		interpolatedVals_newKey = pd.concat([interpolatedVals, pd.DataFrame(np.nan, index=newIndex, columns=[key])], axis = 0)

		# print('key = <%s>, dtype = <%s>' %(key,type(curEventExcerptPanda[key].iloc[0])))
		isString = [str_ix for str_ix,q in enumerate(curEventExcerptPanda[key]) if type(q) == str] 					# Search for rows where the cell has content
		# isString = [q.index for q in curEventExcerptPanda[key] if type(q) == str]
		if key == 'eventTimeIndex'or key == 'Ts' or key == 'eventTime' :
			continue
			##########
			# why does it give this error 'value below interpolation range??'

		mustBeTeamLevelKey = False
		for nthPlayer, everyPlayerID in enumerate(everyPlayerIDs):

			# Prepare indices to Store it in the panda
			start_ix = (1 + nthPlayer) * len(X_int) - len(X_int)
			end_ix = (1 + nthPlayer) * len(X_int) -1
			interpolatedVals.loc[start_ix : end_ix,'Ts'] = X_int # The same every time, only needs to be done once. Could use it as a check though, as they should always be the same
			interpolatedVals.loc[start_ix : end_ix,'eventTime'] = X_int - X_int[-1] # The same every time, only needs to be done once. Could use it as a check though, as they should always be the same
			interpolatedVals.loc[start_ix : end_ix,'eventTimeIndex'] = np.arange(-len(X_int)+1,0+1) # The same every time, only needs to be done once. Could use it as a check though, as they should always be the same
			# Select the current rows (should capture one timeseries)
			# curRows = curEventExcerptPanda.loc[curEventExcerptPanda['PlayerID'] == everyPlayerID]['Ts'].index
			curRows = curEventExcerptPanda.loc[curEventExcerptPanda['PlayerID'] == everyPlayerID].index

			# Take only the indices that are notnull
			curIdx = curRows[np.where(pd.notnull(curEventExcerptPanda.loc[curRows][key]))]

			if len(curIdx) == 0:
				if key == 'RefTeam':
					# An empty refTeam occurs when there was no refTeam for this event. As is already indicated in temporalAggregation with a warning, the pipeline then continues with Team A being the refteam.
					interpolatedVals.loc[start_ix : end_ix,key] = refTeam

				# Do nothing for this player
				# print('no data')
				mustBeTeamLevelKey = True
				continue

			curX = curEventExcerptPanda.loc[curIdx]['Ts']
			curY = curEventExcerptPanda.loc[curIdx][key]

			# if any(isString):
			if curEventExcerptPanda.loc[curRows,key].dtype == float:
				# Check if 'jumps' exist, because it should crop the window selected
				# A jump is based on the median of the framerates per frame. To be presize, a deviation 1.5 times the median
				t0 = curX[:-1].reset_index(level=None, drop=False, inplace=False)
				t1 = curX[1:].reset_index(level=None, drop=False, inplace=False)

				dt = t1-t0
				jumps = dt['Ts'][dt['Ts']>(np.median(dt['Ts'])*1.5)]

				if jumps.empty:
					# No jumps
					## Using interp1d here to avoid (for example) negative values where they can't exist.
					## Spline interpolation can be used on the raw data. Not on the aggregated data.

					# Adjust first X_int frame in case of missing a frame at the start
					# Crop the missing first frames (if any)
					X_int_cropped_idx = np.where(X_int >= min(curX))
					start_ixNew = X_int_cropped_idx[0][0] + start_ix
					X_int_cropped = X_int[X_int_cropped_idx]

					# print('Minimum orignal X:')
					# print(min(curX))
					# print('Old X_int:')
					# print(X_int)
					# print('Cropped X_int:')
					# print(X_int_cropped)
					# print('First cropped idx:')
					# print(X_int_cropped_idx[0][0])
					# print('Old start_ix:')
					# print(start_ix)
					# print('New start_ix:')
					# print(start_ixNew)

					start_ix = start_ixNew

					# Crop the missing last frames (if any)
					X_int_cropped_idx = np.where(X_int_cropped <= max(curX))
					end_ix = len(X_int_cropped_idx[0]) + start_ix -1
					X_int_cropped = X_int_cropped[X_int_cropped_idx]

					s = interp1d(curX, curY)

					try:
						Y_int = s(X_int_cropped)
					except:
						print(min(curX[1:-1]),min(X_int_cropped))
						print(max(curX[1:-1]),max(X_int_cropped))
						print('---')
						print(curX)
						print('----')
						print(X_int_cropped)
						print('---')
						print('If a problem occurs here, it has something to do with the window length and the available frames in the raw data. Probably, there is no overlap or something...')
						pdb.set_trace()
				else:
					# Adjust for jumps in time.
					jumpStarts = t0['Ts'][dt['Ts']>(np.median(dt['Ts'])*1.5)]
					jumpEnds = t1['Ts'][dt['Ts']>(np.median(dt['Ts'])*1.5)]
					endOfLastJump = jumpEnds.iloc[-1]

					curX_cropped = curX[curX >= endOfLastJump]
					curY_cropped = curY[curX >= endOfLastJump]

					# It excludes the X_int preceding endOfLastJump
					X_int_cropped = X_int[X_int >= endOfLastJump]
					### This also works; The last value in tmp is the X_int value prior to where the data restarts (BUT NOT WITH interp1d)
					# # Find all times in X_int before endOfLastJump
					# tmp = [i for i, x in enumerate(X_int) if x < endOfLastJump]
					# X_int_cropped = X_int[tmp[-1]:]
					# start_ix = start_ix + tmp[-1] +1

					windowWithoutJumps = max(curX_cropped) - endOfLastJump
					warn('\nWARNING: Could not use the whole window leading up to this event due to temporal jumps in the data.\nThe intended window was <%ss>, whereas the window from the last jump until the event was <%ss>.\n\n**********\n**********\n**********\nSTILL TO DO: Write an exception for <full> and for other events with a start AND an end.\n**********\n**********\n**********\n' %(aggregateLevel[1],windowWithoutJumps))

					## Using interp1d here to avoid (for example) negative values where they can't exist.
					## Spline interpolation can be used on the raw data. Not on the aggregated data.
					s = interp1d(curX_cropped, curY_cropped)
					Y_int = s(X_int_cropped)

					tmp = next(i for i, x in enumerate(X_int) if x > endOfLastJump)
					start_ix = start_ix + tmp -1

				# Store in panda
				try:
					interpolatedVals.loc[start_ix : end_ix,key] = Y_int
				except:
					print('Start:')

					print(start_ix)
					print('End:')
					print(end_ix)
					print('Length:')
					print(len(Y_int))
					print(key)
					print('-----')
					print('If a problem persists here, it\'s something with the windowsize.')
					# interpolatedVals.loc[start_ix : end_ix-1,key] = Y_int
					pdb.set_trace()

			else:
				# If they current key is not a float, it's not possible to interpolate.
				# In some cases, it concerns:
				# - event identifiers or player level identifiers
				# - OR string events
				# Event identifiers and player level identifiers can simply be copied:
				if all(curEventExcerptPanda.loc[curRows,key] == curEventExcerptPanda.loc[curRows[0],key]):
					# If they're all the same (i.e., player, team or match identifiers)
					# int_curKey = pd.DataFrame([curEventExcerptPanda.loc[curRows[0],key]]*len(X_int), columns = [key], index = [int_curPlayer.index])
					interpolatedVals.loc[start_ix : end_ix,key] = curEventExcerptPanda.loc[curRows[0],key]

					if not key in ['TeamID', 'PlayerID', 'temporalAggregate','RefTeam','EventUID','School','Class','Group', 'Test', 'Exp','MatchContinent','MatchCountry','MatchID','HomeTeamContinent','HomeTeamCountry','HomeTeamAgeGroup','HomeTeamID','AwayTeamContinent','AwayTeamCountry','AwayTeamAgeGroup','AwayTeamID','OthTeam' ]:
						warn('\nWARNING: Key <%s> was identified as an event identifier.\nTherefore, no data was interpolated, instead, the same value was copied for all cells.\n' %key)
				else:
					# Create an empty dataframe
					# int_curKey = pd.DataFrame([], columns = [key], index = [int_curPlayer.index])

					# Event Strings
					if not key in ['Goal','Run','Possession/Turnover','Pass']:
						warn('\nWARNING: Key <%s> was identified as a string event.\nTherefore, no data was interpolated, instead, the same value was copied to the cell with the interpolated time index closest to the original time index.\nMAKE SURE YOU HAVE FORCED THE DATATYPE CORRECTLY.' %key)
						# if key == 'vNorm':
						# 	print(curEventExcerptPanda.loc[curRows,key].dtype )
						# 	print('still problematic')
						# 	pdb.set_trace()
						# 	Y = curEventExcerptPanda.loc[curRows,key]
						# 	f = InterpolatedUnivariateSpline(curTs,Y, k = 3) # order 1) = linear, 2) = quadratic, 3) = cubic
						# 	Y_int = f(X_int)
						# 	int_curKey = pd.DataFrame(Y_int, columns = [key])

					# Check whether there are less cells with content than cells in X_int AND the original X
					if len(isString) >= len(X_int) or len(isString) >= len(curRows):
						warn('\nWARNING: Identified key <%s> as a column with string events.\n(String events have a string in the row where the event happens, the remaining rows are empty.)\nBut this seems unlikely, as there were more events than time indices.\nThis may result in an error, is there are no cells to interpolate the string events to (they\'re currently transferred to the interpolated time value that lies closest to the original time value, i.e., 1 on 1)\n' %key)
						# (if there are more, it can't have been a string event)

					for stringEvent in isString:
						# print(len(curEventExcerptPanda))
						# print(typ(stringEvent))
						# print(len(curRows))
						# pdb.set_trace()
						timeOfEvent = curEventExcerptPanda['Ts'].iloc[stringEvent]#.iloc[stringEvent.index]
						# Find time in X_int that is closest to timeOfEvent
						# nearestTimeOfEvent = [abs(X_int - timeOfEvent) == min(abs(X_int - timeOfEvent))].index
						nearestTimeOfEvent = np.where(abs(X_int - timeOfEvent) == min(abs(X_int - timeOfEvent)))
						# Currently, it's possible that a time index is chosen that has no corresponding positional data. (it may be the one frame before, or the one frame after)

						# Put the stringEvent there, and leave the rest as an empty string i.e., ''
						ix_event = nearestTimeOfEvent[0]
						# int_curKey.loc[ix_event,key] = curEventExcerptPanda.loc[curRows,key].iloc[stringEvent]
						# interpolatedVals.loc[ix_event,key] = curEventExcerptPanda.loc[curRows,key].iloc[stringEvent]
						interpolatedVals.loc[ix_event,key] = curEventExcerptPanda[key].iloc[stringEvent]

				# # If they current key contains a string, it's not possible to interpolate.
				# # In some cases, it concerns:
				# # - event identifiers
				# # - player level identifiers
				# # - OR string events
				# # Event identifiers and player level identifiers can simply be copied:
				# if all(curEventExcerptPanda[key] == curEventExcerptPanda[key].iloc[0]):
				# 	# If they're all the same (i.e., event identifiers)
				# 	interpolatedVals.loc[:,key] = curEventExcerptPanda[key].iloc[0]
				# 	if not key in ['temporalAggregate','RefTeam','EventUID','School','Class','Group', 'Test', 'Exp','MatchContinent','MatchCountry','MatchID','HomeTeamContinent','HomeTeamCountry','HomeTeamAgeGroup','HomeTeamID','AwayTeamContinent','AwayTeamCountry','AwayTeamAgeGroup','AwayTeamID' ]:
				# 		warn('\nWARNING: Key <%s> was identified as an event identifier.\nTherefore, no data was interpolated, instead, the same value was copied for all cells.\n' %key)
				# 	break
				# elif all(curY == curY.iloc[0]) and not mustBeTeamLevelKey:
				# 	# If they're all the same for the current player (player level identifier)
				# 	interpolatedVals.loc[start_ix : end_ix,key] = curY.iloc[0]
				# 	if not key in ['TeamID','PlayerID']:
				# 		warn('\nWARNING: Key <%s> was identified as a player level identifier.\nTherefore, no data was interpolated, instead, the same value was copied for all cells of the current player.\n' %key)
				# 	continue
				# else:
				# 	if not key in ['Run','Possession/Turnover','Pass']:
				# 		warn('\nWARNING: Key <%s> was identified as a string event.\nTherefore, no data was interpolated, instead, the same value was copied to the cell with the interpolated time index closest to the original time index.\n' %key)

				# 	# Check whether there are less cells with content than cells in X_int AND the original X
				# 	if len(isString) >= len(X_int) or len(isString) >= len(curRows):
				# 		warn('\nWARNING: Identified key <%s> as a column with string events.\n(String events have a string in the row where the event happens, the remaining rows are empty.)\nBut this seems unlikely, as there were more events than time indices.\nThis may result in an error, is there are no cells to interpolate the string events to (they\'re currently transferred to the interpolated time value that lies closest to the original time value, i.e., 1 on 1)\n' %key)
				# 		# (if there are more, it can't have been a string event)

				# 	for stringEvent in isString:
				# 		timeOfEvent = curEventExcerptPanda['Ts'].iloc[stringEvent]
				# 		# Find time in X_int that is closest to timeOfEvent
				# 		# nearestTimeOfEvent = [abs(X_int - timeOfEvent) == min(abs(X_int - timeOfEvent))].index
				# 		nearestTimeOfEvent = np.where(abs(X_int - timeOfEvent) == min(abs(X_int - timeOfEvent)))
				# 		# Currently, it's possible that a time index is chosen that has no corresponding positional data. (it may be the one frame before, or the one frame after)

				# 		# Put the stringEvent there, and leave the rest as an empty string i.e., ''
				# 		ix_event = start_ix + nearestTimeOfEvent[0]
				# 		interpolatedVals.loc[ix_event,key] = curEventExcerptPanda[key].iloc[stringEvent]

				# 	continue





	return interpolatedVals

def savitzky_golay(y, window_size, order, deriv=0, rate=1):
    """Smooth (and optionally differentiate) data with a Savitzky-Golay filter.
    The Savitzky-Golay filter removes high frequency noise from data.
    It has the advantage of preserving the original shape and
    features of the signal better than other types of filtering
    approaches, such as moving averages techniques.
    Parameters
    ----------
    y : array_like, shape (N,)
        the values of the time history of the signal.
    window_size : int
        the length of the window. Must be an odd integer number.
    order : int
        the order of the polynomial used in the filtering.
        Must be less then `window_size` - 1.
    deriv: int
        the order of the derivative to compute (default = 0 means only smoothing)
    Returns
    -------
    ys : ndarray, shape (N)
        the smoothed signal (or it's n-th derivative).
    Notes
    -----
    The Savitzky-Golay is a type of low-pass filter, particularly
    suited for smoothing noisy data. The main idea behind this
    approach is to make for each point a least-square fit with a
    polynomial of high order over a odd-sized window centered at
    the point.
    Examples
    --------
    t = np.linspace(-4, 4, 500)
    y = np.exp( -t**2 ) + np.random.normal(0, 0.05, t.shape)
    ysg = savitzky_golay(y, window_size=31, order=4)
    import matplotlib.pyplot as plt
    plt.plot(t, y, label='Noisy signal')
    plt.plot(t, np.exp(-t**2), 'k', lw=1.5, label='Original signal')
    plt.plot(t, ysg, 'r', label='Filtered signal')
    plt.legend()
    plt.show()
    References
    ----------
    .. [1] A. Savitzky, M. J. E. Golay, Smoothing and Differentiation of
       Data by Simplified Least Squares Procedures. Analytical
       Chemistry, 1964, 36 (8), pp 1627-1639.
    .. [2] Numerical Recipes 3rd Edition: The Art of Scientific Computing
       W.H. Press, S.A. Teukolsky, W.T. Vetterling, B.P. Flannery
       Cambridge University Press ISBN-13: 9780521880688
    """
    import numpy as np
    from math import factorial

    try:
        window_size = np.abs(np.int(window_size))
        order = np.abs(np.int(order))
    except ValueError:
        raise ValueError("window_size and order have to be of type int")
    if window_size % 2 != 1 or window_size < 1:
        raise TypeError("window_size size must be a positive odd number")
    if window_size < order + 2:
        raise TypeError("window_size is too small for the polynomials order")
    order_range = range(order+1)
    half_window = (window_size -1) // 2
    # precompute coefficients
    b = np.mat([[k**i for i in order_range] for k in range(-half_window, half_window+1)])
    m = np.linalg.pinv(b).A[deriv] * rate**deriv * factorial(deriv)
    # pad the signal at the extremes with
    # values taken from the signal itself
    firstvals = y[0] - np.abs( y[1:half_window+1][::-1] - y[0] )
    lastvals = y[-1] + np.abs(y[-half_window-1:-1][::-1] - y[-1])
    y = np.concatenate((firstvals, y, lastvals))
    return np.convolve( m[::-1], y, mode='valid')
