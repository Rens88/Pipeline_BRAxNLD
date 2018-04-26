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

def process(df):
	
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

	# Compute dt for the whole dataset to establish threshold and framerate.
	dataDrivenFrameRate,dataDrivenThreshold = deriveFramerate(df)
	everyPlayerIDs = pd.unique(df['PlayerID'])	

	# Prepare butterworth filter
	# Filter requirements.
	# order = 6
	# fs = dataDrivenFrameRate      # sample rate, Hz
	# cutoff = 0.001# desired cutoff frequency of the filter, Hz

	for nthPlayer, everyPlayerID in enumerate(everyPlayerIDs):

		curRows = df.loc[df['PlayerID'] == everyPlayerID].index
		curTs = df.loc[curRows,'Ts']
		for key in ['X','Y']:


			# Some fancy moving average filter
			windowSize = 1.5 # in seconds
			windowSizeForFilter = np.round((windowSize*2*dataDrivenFrameRate)+1,1)
			Y = df.loc[curRows,key].as_matrix()
			yhat = savitzky_golay(Y, windowSizeForFilter, 3) # window size 51, polynomial order 3
			df.loc[curRows,key] = yhat



			starts,ends = findJumps(curTs,dataDrivenThreshold,dataDrivenFrameRate,returnStartsEnds = True)
			if len(starts) > 1:
				warn('\WARNING: Jump in the data detected. Pipeline currently not equiped to deal with this.\nFinish the filterData modul ein FillGaps_and_filter.py.\n**********\n!!!!!!!!!!!')

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
	
	if 'checkForJumps' in kwargs:
		checkForJumps = kwargs['checkForJumps']
	else:
		checkForJumps = False

	# Check if 'jumps' exist, because it should crop the window selected
	# A jump is based on the median of the framerates per frame. To be presize, a deviation 1.5 times the median
	# NB: Can also be based on an absolute window..

	everyPlayerIDs = pd.unique(df['PlayerID'])	
	uniqueTimes = pd.unique(df['Ts'])	
	smallestTime = min(uniqueTimes)
	biggestTime = max(uniqueTimes)

	dataDrivenFrameRate,dataDrivenThreshold = deriveFramerate(df)

	interpolatedVals = pd.DataFrame([],columns = [df.keys()])			

	for nthPlayer, everyPlayerID in enumerate(everyPlayerIDs):
		# The interpolation has to happen per player
		curRows = df.loc[df['PlayerID'] == everyPlayerID].index
		curTs = df.loc[curRows]['Ts']

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

		if checkForJumps:
			X_int = findJumps(curTs,dataDrivenThreshold,dataDrivenFrameRate)
		else:
			X_int = determineX_int(curTs,dataDrivenFrameRate)

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
				f = InterpolatedUnivariateSpline(curTs,Y, k = 3) # order 1) = linear, 2) = quadratic, 3) = cubic
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
					int_curKey = pd.DataFrame([df.loc[curRows[0],key]]*len(X_int), columns = [key], index = [int_curPlayer.index])
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

def determineX_int(curTs,dataDrivenFrameRate):
	tStart = min(curTs)
	tEnd = max(curTs)
	# print(tStart)
	# print(tEnd)
	# print(dataDrivenFrameRate)
	tStartRound = np.round(tStart,math.ceil(math.log(dataDrivenFrameRate,10)))
	tEndRound = np.round(tEnd,math.ceil(math.log(dataDrivenFrameRate,10)))
	tmp = np.arange(tStartRound,tEndRound + 1/dataDrivenFrameRate, 1/dataDrivenFrameRate) # referring to eventTime
	X_int = np.round(tmp,math.ceil(math.log(dataDrivenFrameRate,10))) # automatically rounds it to the required number of significant numbers (decimal points) for the given sampling frequency (to avoid '0' being stored as 1.474746 E16 AND 1.374750987 E16)
	return X_int

def findJumps(curTs,dataDrivenThreshold,dataDrivenFrameRate,**kwargs):
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
		X_int = determineX_int(curTs,dataDrivenFrameRate)

	else:
		# Jumps found, make sure X_int skips the jumps as well
		# Adjust for jumps in time.
		jumpStarts = t0['Ts'][dt['Ts']>(np.median(dt['Ts'])*1.5)] # SHould it be the frame before jump starts?
		dataEnds = pd.concat([jumpStarts, lastData['Ts']],axis = 0) #-1 # jumpStarts - 1

		jumpEnds = t1['Ts'][dt['Ts']>(np.median(dt['Ts'])*1.5)]  # should it be the frame after jump ends?
		dataStarts = pd.concat([firstData['Ts'], jumpEnds],axis = 0) #-1 # jumpStarts - 1
	
		tmp = [np.arange(dataStarts.iloc[isx],en + 1/dataDrivenFrameRate,1/dataDrivenFrameRate) for isx,en in enumerate(dataEnds)]
		tmp = np.concatenate(tmp)
		X_int = np.round(tmp,math.ceil(math.log(dataDrivenFrameRate,10))) # automatically rounds it to the required number of significant numbers (decimal points) for the given sampling frequency (to avoid '0' being stored as 1.474746 E16 AND 1.374750987 E16)
		# # np.savetxt("C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\NP repository\\test.csv", X_int, delimiter=",")

	if returnStartsEnds:
		return dataStarts,dataEnds
	return X_int

def deriveFramerate(df):

	# Compute dt for the whole dataset to establish threshold and framerate.
	allTs = df['Ts']
	t0 = allTs[:-1].reset_index(level=None, drop=False, inplace=False)
	t1 = allTs[1:].reset_index(level=None, drop=False, inplace=False)
	dt = t1-t0
	dataDrivenFrameRate = np.round(1/np.median(dt['Ts']),3) # in Hz. You might want to override this with an actual framerate
	dataDrivenThreshold = np.round(1.5/dataDrivenFrameRate,decimals = 3)
	return dataDrivenFrameRate,dataDrivenThreshold

def savitzky_golay(y, window_size, order, deriv=0, rate=1):
    r"""Smooth (and optionally differentiate) data with a Savitzky-Golay filter.
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