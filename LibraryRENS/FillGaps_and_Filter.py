# 25-04-2018
# Generic module that can be used for filling gaps and filtering.

import pdb; #pdb.set_trace()
from warnings import warn
import numpy as np
import numpy as np
from scipy.interpolate import InterpolatedUnivariateSpline,interp1d
import pandas as pd
import math

if __name__ == '__main__':		
	process(df)
########################################################################	

def process(df):
	
	# For X and Y only?


	df_filled = fillGaps(df)

	df_filled_filtered = filter(df_filled)

	return df_filled_filtered
########################################################################	

def fillGaps(df):
	# Check if 'jumps' exist, because it should crop the window selected
	# A jump is based on the median of the framerates per frame. To be presize, a deviation 1.5 times the median
	# NB: Can also be based on an absolute window..
	
	everyPlayerIDs = pd.unique(df['PlayerID'])	
	uniqueTimes = pd.unique(df['Ts'])	
	smallestTime = min(uniqueTimes)
	biggestTime = max(uniqueTimes)

	# Compute dt for the whole dataset to establish threshold and framerate.
	allTs = df['Ts']
	t0 = allTs[:-1].reset_index(level=None, drop=False, inplace=False)
	t1 = allTs[1:].reset_index(level=None, drop=False, inplace=False)
	dt = t1-t0
	dataDrivenFrameRate = np.round(1/np.median(dt['Ts']),3) # in Hz. You might want to override this with an actual framerate
	dataDrivenThreshold = np.round(1.5/dataDrivenFrameRate,decimals = 3)

	interpolatedVals = pd.DataFrame()			

	for nthPlayer, everyPlayerID in enumerate(everyPlayerIDs):
		# HOLD THAT THOUGHT (prob not relevant) So far, I haven't had any datasets where there was missing data that would have needed to be interpolated.

		# The interpolation has to happen per player
		curRows = df.loc[df['PlayerID'] == everyPlayerID].index
		# # What to do with Frames that are null ?? --> they
		# curIdx = curRows[np.where(pd.notnull(curEventExcerptPanda.loc[curRows][key]))]

		curTs = df.loc[curRows]['Ts']
		t0 = curTs[:-1].reset_index(level=None, drop=False, inplace=False)
		t1 = curTs[1:].reset_index(level=None, drop=False, inplace=False)

		dt = t1-t0
		jumpSizes = dt['Ts'][dt['Ts']>(dataDrivenThreshold)] # Contains the size of the jump and the index with respect to t0 and t1

		# Separately check for smallestTime and biggestTime 
		# Not sure how this affects the rest of the code..
		if min(curTs) - min(allTs) > dataDrivenThreshold:
			# 'gap' for this player at the start
			gapSize = min(curTs) - min(allTs)
			# jumpSizes[0] = gapSize
			warn('\nWARNING:Found a jump in time (of <%ss>) BEFORE the first frame of the current player <%s>.\nCurrently, this gap is ignored when filling gaps.\n Adjust code here if you want to do something with it.\n' %(gapSize,everyPlayerID))

		if max(allTs) - max(curTs) > dataDrivenThreshold:
			# 'gap' for this player at the end
			gapSize = max(allTs) - max(curTs)
			# jumpSizes[len(curTs)] = gapSize
			warn('\nWARNING:Found a jump in time (of <%ss>) AFTER the first frame of the current player <%s>.\nCurrently, this gap is ignored when filling gaps.\n Adjust code here if you want to do something with it.\n' %(gapSize,everyPlayerID))


		if jumpSizes.empty:
			# No jumps found, simply interpolate with standardaized X_int
			freqInterpolatedData = dataDrivenFrameRate

			tStart = min(curTs)
			tEnd = max(curTs)
			tStartRound = np.round(tStart,math.ceil(math.log(freqInterpolatedData,10)))
			tEndRound = np.round(tEnd,math.ceil(math.log(freqInterpolatedData,10)))
			tmp = np.arange(tStartRound,tEndRound + 1/freqInterpolatedData, 1/freqInterpolatedData) # referring to eventTime

			X_int = np.round(tmp,math.ceil(math.log(freqInterpolatedData,10))) # automatically rounds it to the required number of significant numbers (decimal points) for the given sampling frequency (to avoid '0' being stored as 1.474746 E16 AND 1.374750987 E16)

			int_curPlayer = pd.DataFrame()
			for key in df.keys():
				Y = df.loc[curRows][key]
				f = InterpolatedUnivariateSpline(curTs,Y, order = 3) # order 1) = linear, 2) = quadratic, 3) = cubic
				Y_int = f(X_int)
				int_curKey = pd.DataFrame(Y_int, columns = [key])
				int_curPlayer = int_curPlayer.append(int_curKey)

				interpolatedVals.loc[][key]

		else:
			doSomething = []
			# Run gap filling procedure for every gap separately






	# Create optional input to delete gaps or to include gaps
	mustBeTeamLevelKey = False
	for nthPlayer, everyPlayerID in enumerate(everyPlayerIDs):

		# Prepare indices to Store it in the panda
		start_ix = (1 + nthPlayer) * len(X_int) - len(X_int) 
		end_ix = (1 + nthPlayer) * len(X_int) -1
		interpolatedVals.loc[start_ix : end_ix,'Ts'] = X_int # The same every time, only needs to be done once. Could use it as a check though, as they should always be the same
		interpolatedVals.loc[start_ix : end_ix,'EventTime'] = X_int - X_int[-1] # The same every time, only needs to be done once. Could use it as a check though, as they should always be the same
		interpolatedVals.loc[start_ix : end_ix,'EventIndex'] = np.arange(-len(X_int)+1,0+1) # The same every time, only needs to be done once. Could use it as a check though, as they should always be the same
		# Select the current rows (should capture one timeseries)
		# curRows = curEventExcerptPanda.loc[curEventExcerptPanda['PlayerID'] == everyPlayerID]['Ts'].index
		curRows = curEventExcerptPanda.loc[curEventExcerptPanda['PlayerID'] == everyPlayerID].index

		# Take only the indices that are notnull
		curIdx = curRows[np.where(pd.notnull(curEventExcerptPanda.loc[curRows][key]))]

		if len(curIdx) == 0:
			# Do nothing for this player
			# print('no data')
			mustBeTeamLevelKey = True
			continue

		curX = curEventExcerptPanda.loc[curIdx]['Ts']
		curY = curEventExcerptPanda.loc[curIdx][key]

		if any(isString):
			# If they current key contains a string, it's not possible to interpolate.
			# In some cases, it concerns:
			# - event identifiers
			# - player level identifiers
			# - OR string events
			# Event identifiers and player level identifiers can simply be copied:
			if all(curEventExcerptPanda[key] == curEventExcerptPanda[key].iloc[0]):
				# If they're all the same (i.e., event identifiers)
				interpolatedVals.loc[:,key] = curEventExcerptPanda[key].iloc[0]
				if not key in ['temporalAggregate','RefTeam','EventUID','School','Class','Group', 'Test', 'Exp']:
					warn('\nWARNING: Key <%s> was identified as an event identifier.\nTherefore, no data was interpolated, instead, the same value was copied for all cells.\n' %key)
				break
			elif all(curY == curY.iloc[0]) and not mustBeTeamLevelKey:
				# If they're all the same for the current player (player level identifier)
				interpolatedVals.loc[start_ix : end_ix,key] = curY.iloc[0]
				if not key in ['TeamID','PlayerID']:
					warn('\nWARNING: Key <%s> was identified as a player level identifier.\nTherefore, no data was interpolated, instead, the same value was copied for all cells of the current player.\n' %key)
				continue
			else:
				if not key in ['Run','Possession/Turnover','Pass']:
					warn('\nWARNING: Key <%s> was identified as a string event.\nTherefore, no data was interpolated, instead, the same value was copied to the cell with the interpolated time index closest to the original time index.\n' %key)

				# Check whether there are less cells with content than cells in X_int AND the original X
				if len(isString) >= len(X_int) or len(isString) >= len(curRows):
					warn('\nWARNING: Identified key <%s> as a column with string events.\n(String events have a string in the row where the event happens, the remaining rows are empty.)\nBut this seems unlikely, as there were more events than time indices.\nThis may result in an error, is there are no cells to interpolate the string events to (they\'re currently transferred to the interpolated time value that lies closest to the original time value, i.e., 1 on 1)\n' %key)
					# (if there are more, it can't have been a string event)
	
				for stringEvent in isString:
					timeOfEvent = curEventExcerptPanda['Ts'].iloc[stringEvent]
					# Find time in X_int that is closest to timeOfEvent
					# nearestTimeOfEvent = [abs(X_int - timeOfEvent) == min(abs(X_int - timeOfEvent))].index
					nearestTimeOfEvent = np.where(abs(X_int - timeOfEvent) == min(abs(X_int - timeOfEvent)))
					# Currently, it's possible that a time index is chosen that has no corresponding positional data. (it may be the one frame before, or the one frame after)

					# Put the stringEvent there, and leave the rest as an empty string i.e., '' 
					ix_event = start_ix + nearestTimeOfEvent[0]
					interpolatedVals.loc[ix_event,key] = curEventExcerptPanda[key].iloc[stringEvent]

				continue


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
			s = interp1d(curX, curY)		
			Y_int = s(X_int)				

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
			start_ix = start_ix + tmp

		# Store in panda
		interpolatedVals.loc[start_ix : end_ix,key] = Y_int

































		if True:# jumpSizes.empty:
			# No jumps found, simply interpolate with standardaized X_int
			# 	# No jumps found, data can be interpolated altogether
			# for jumpIdx in jumpSizes.index:
			# 	print(curTs[jumpIdx-3:jumpIdx+3])
			freqInterpolatedData = dataDrivenFrameRate

			tStart = min(curTs)
			tEnd = max(curTs)
			tStartRound = np.round(tStart,math.ceil(math.log(freqInterpolatedData,10)))
			tEndRound = np.round(tEnd,math.ceil(math.log(freqInterpolatedData,10)))
			tmp = np.arange(tStartRound,tEndRound + 1/freqInterpolatedData, 1/freqInterpolatedData) # referring to eventTime

			X_int = np.round(tmp,math.ceil(math.log(freqInterpolatedData,10))) # automatically rounds it to the required number of significant numbers (decimal points) for the given sampling frequency (to avoid '0' being stored as 1.474746 E16 AND 1.374750987 E16)

			print(tStart)
			print(tEnd)
			print(X_int)
			print(dataDrivenFrameRate)

			# Numeric columns only.



			pdb.set_trace()

			return



		print(jumpSizes)
		# other than or the jumps, resample the data to precisely have a dt of np.median(dt['Ts']) for every timeframe.


		warn('\nWARNING: The threshold for the size of a gap to be determined as a gap was based on the median of the sampling frequency.\nWith the current sampling frequency, any gap in time longer than <%ss> was considered a gap.\nYou may want to manually set the size of a gap that needs to be filled..\n' %dataDrivenThreshold)
		
		pdb.set_trace()


	return df_filled

def filter(df):

	return df_filtered