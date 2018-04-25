# 16-04-2018 Rens Meerhoff
# Big update to temporalAggregation.
# It now automatically aggregates any of the (numeric) spatial aggregates that exist in attributeDict.
# In future, we could consider expanding which aggregates are taken. Functionality exists to aggregate for:
# Count, average, standard deviation, sum, min, max, median, Standard Error of Measurement, kurtosis and skewness.
#
# By using the following strings in aggrMeth_playerLevel or aggrMeth_popLevel (poLevel = population level)
# ['cnt', 'avg', 'std', 'sumVal', 'minVal', 'maxVal', 'med', 'sem', 'kur', 'ske']
#
# As for the individually spatially aggregated variables (i.e., measures that relate to one individual), the 
# temporal aggregates can be determined for all players ('allTeam'), the players of the reference team ('refTeam') and
# the players of the other (= non reference)  team ('othTeam').
# By default, all three are exported, but you could change this with <populations>
# 
# For these player-level variables, the order of aggregating is important too. First per player and then over time
# provides a more player based comparison. First over time and then per player provides insights over the time evolution.
# By default, it aggregates first per player, then per time (<perTimePerPlayer>), yielding a count equal to the number of players.
# Alternatively, you can specify the order with < aggregationOrder> as 'perPlayerPerTime', yielding a count equal to the number of time frames.
#
# NB: These default options are not (yet) embedded in the module's input. See start of 'process'


# 08-12-2017 Rens Meerhoff
# NB: From the raw data, this script should only access the always available Time, Entity, and Location.
# The script should not use the attributeDict directly, as this may differ depending on the data source.
# Instead, use targetEvents in the main process file where you can include the characteristics of the event either
# automatically, or manually (and always make the same format)


import csv
import pdb; #pdb.set_trace()
import numpy as np
import pandas as pd
from os.path import isfile, join, isdir
from os import listdir, path
from warnings import warn
import time
# From my own library:
import plotSnapshot
import safetyWarning
import countEvents2
from scipy import stats
import scipy
import math
from scipy.interpolate import InterpolatedUnivariateSpline, interp1d
import matplotlib.pyplot as plt


if __name__ == '__main__':

	process(targetEvents,aggregateLevel,rawDict,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring)
	# Aggregates a range of pre-defined features over a specific window:
	specific(rowswithinrange,aggregateString,rawDict,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring)	

def process(targetEvents,aggregateLevel,rawDict,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring,debuggingMode,skipEventAgg_curFile,fileIdentifiers,attrLabel):

	tTempAgg = time.time()
	FileID = "_".join(fileIdentifiers)

	## user inputs, could easily lift this out of function
	freqInterpolatedData = 10 # in Hz

	## Specify which aggregates are taken.
	aggregationOrder = ['perTimePerPlayer'] 

	populations = ['allTeam','refTeam','othTeam']
	aggregationOrders = aggregationOrder*len(populations)
	aggrMeth_playerLevel = ['avg', 'std'] # specifically for pertime perplayer
	aggrMeth_popLevel = ['avg', 'std','cnt']
	## If you want a combination of all possible methods, then use these strings: (NB, this explodes the number of features)
	# aggregationOrders = ['perTimePerPlayer','perTimePerPlayer','perTimePerPlayer','perPlayerPerTime','perPlayerPerTime','perPlayerPerTime']
	# populations = ['allTeam','refTeam','othTeam','allTeam','refTeam','othTeam']
	# aggrMeth_playerLevel = ['avg', 'std', 'sumVal', 'minVal', 'maxVal', 'med', 'sem', 'kur', 'ske']
	# aggrMeth_popLevel = ['avg', 'std', 'sumVal', 'minVal', 'maxVal', 'med', 'sem', 'kur', 'ske']

	
	# Create an empty dataFrame where the eventExcerpt will be stored.
	eventExcerptPanda = pd.DataFrame()
	# To which UID and refTeam should be added
	if aggregateLevel[0] == 'None':
		warn('\nWARNING: No temporal aggregate level indicated. \nNo temporally aggregated data exported.\nChange aggregateEvent = <%s> in USER INPUT.\n' %aggregateLevel[0])
		return exportData,exportDataString,exportFullExplanation,eventExcerptPanda,attrLabel

	elif len(targetEvents[aggregateLevel[0]]) == 0:
		warn('\nWARNING: No targetevents detected. \nCouldnt aggregate temporally. \nNo Data exported.\n')
		return exportData,exportDataString,exportFullExplanation,eventExcerptPanda,attrLabel

	# The export matrix includes a range of outcome measures that don't change per event (these are already in <exportData>)
	# Most outcome variables will be added by simply going through all the spatially aggregated variables
	# Some other outcome measures describe the current event and can also be created here:
	exportMatrix = [] # in which you can combine the exported data of each event in this trial / file
	fileIdentifiers = exportDataString.copy()
	exportDataString.append('temporalAggregate') # output string that identifies the current event
	exportFullExplanation.append('Level of temporal aggregation, based on <<%s>> event and counted chronologically' %aggregateLevel[0])
	exportDataString.append('EventUID') # output string that UNIQUELY identifies the current event
	exportFullExplanation.append('Unique identifier of the <<%s>> events.' %aggregateLevel[0])
	exportDataString.append('RefTeam')
	exportFullExplanation.append('Teamstring of the reference team the <<%s>> events refer to.' %aggregateLevel[0])

	# All the outcome measures that exist
	attrDictCols = [i for i in attributeDict.columns if not i in ['Ts','TeamID','PlayerID','X','Y']]
	# Omit outcome variables that are not numeric. These should be aggregated using countEvents2 (see below)
	# NB: May be this should be written more generic..
	attrDictCols_numeric = [tmp for tmp in attrDictCols if tmp not in ['Run', 'Possession/Turnover', 'Pass', 'Goal']]
	
	## FYI:
	# aggregateLevel[0] --> event ID
	# aggregateLevel[1] --> window (s)
	# aggregateLevel[2] --> lag (s)
	# Create an empty to export when there are no events??

	if type(targetEvents[aggregateLevel[0]]) == tuple:
		warn('\nWARNING: Make sure the format of the event fits in a list. \nIf not, the script will fail when there is only 1 event.\n')

	## Loop over every event
	for idx,currentEvent in enumerate(targetEvents[aggregateLevel[0]]):
		
		#####################################
		## Prepare export of current event ##
		#####################################

		## Determine the window
		if aggregateLevel[1] != None:
			tEnd = currentEvent[0] - aggregateLevel[2] # tEnd - lag
			tStart = currentEvent[0] - aggregateLevel[1] - aggregateLevel[2] # tEnd - window - lagg
			eventDescrString = 'During <%ss> seconds up until <%ss> before event <%s>.' %(aggregateLevel[1],aggregateLevel[2],aggregateLevel[0])
		else:
			# No window indicated, so take the full event (if possible)
			if len(currentEvent) < 3:
				warn('\nFATAL WARNING: No tStart indicated for this event, nor a window was given for the temporal aggregation.\nEither: 1) Indicate a window for the temporal aggregation.\nOr: 2) Export a tStart for the event you\'re aggregating.')
						
			tEnd = currentEvent[0]# - aggregateLevel[2]
			tStart = currentEvent[2]# - aggregateLevel[1]			
			eventDescrString =  'During the whole duration of the events <%s>.' %aggregateLevel[0]

		window = (tStart, tEnd)
		if window[0] == None or window[1] == None: # Check if both end and start are allocated
			warn('\nEvent %d skipped because tStart = <<%s>> and tEnd = <<%s>>.\n' %(idx,window[0],window[1]))
			continue

		# warn and/or shorten whenever there is overlap with the previous event
		if idx != 0:
			tEnd_prevEvent = targetEvents[aggregateLevel[0]][idx-1][0]
			if tEnd_prevEvent > tStart:
				warn('\nWARNING: Current event start <%ss> before previous event finished <%ss>.\nCurrently, this is allowed. You may want to:\n1) Restrict events to only cover unique periods\nor 2) Avoid windows that result into overlapping times.\n' %(tStart,tEnd_prevEvent))
		
		tmp = rawDict[rawDict['Ts'] > window[0]]
		rowswithinrange = tmp[tmp['Ts'] <= window[1]].index
		
		## Prepare a copy of exported data so far (which can be considered as file identifiers)
		exportCurrentData = exportData.copy()
		overallString = exportDataString.copy()
		overallExplanation = exportFullExplanation.copy()
		
		## Create the output string that identifies the current event
		aggregateString = '%s_%03d' %(aggregateLevel[0],idx)		
		if idx > 1000:
			aggregateString = '%s_%d' %(aggregateLevel[0],idx)
			warn('\nWARNING: More than 1000 occurrences of the same event.\nForced to change format for finding aggregateString.\nConsider pre-allocating the number of digits included by default.')
		
		exportCurrentData.append(aggregateString) # NB: String and explanation are defined before the for loop
		
		## Create unique event identifier 
		EventUID = FileID + '_' + aggregateString
		exportCurrentData.append(EventUID) # NB: String and explanation are defined before the for loop

		## Assign refTeam
		refTeam = currentEvent[1]
		exportCurrentData.append(refTeam) # NB: String and explanation are defined before the for loop	
		if refTeam == TeamAstring:
			# Team A becomes ref and team B becomes other
			ref = 'A'
			oth = 'B'
			othTeam = TeamBstring
		elif refTeam == TeamBstring:
			# Team B becomes ref and team A becomes other
			ref = 'B'
			oth = 'A'
			othTeam = TeamAstring
		else:
			warn('\nWARNING: refTeam <%s> did not correspond with TeamAstring <%s> or TeamBstring <%s>.\nCould not establish who the refernce team was.\nContinued with <%s> as refTeam.\n' %(refTeam,TeamAstring,TeamBstring,TeamAstring))
			ref = 'A'
			oth = 'B'
			refTeam = TeamAstring
			othTeam = TeamBstring

		## Change attribute keys to refer to refTeam and othTeam		
		# WEAKNESS: relies on attribute keys to end with 'A' or 'B' in team scenarios.
		keyAttr = attrLabel.keys()
		copyTheseKeys_A = [ikey for ikey in keyAttr if ikey[-1] == 'A']
		copyTheseKeys_B = [ikey for ikey in keyAttr if ikey[-1] == 'B']
		for ikey in copyTheseKeys_A:
			attrLabel.update({ikey[:-1] + '_ref': attrLabel[ikey].replace(TeamAstring,'refTeam')})
		for ikey in copyTheseKeys_B:
			attrLabel.update({ikey[:-1] + '_oth': attrLabel[ikey].replace(TeamBstring,'othTeam')})

		## All data in exportCurrentData are trial identifiers. Create a column in a new panda copying these:
		# Copy everything that exists in exportCurrentData up until here into a new dataFrame.
		currentEventID = pd.DataFrame([],columns = [exportDataString],index = [rawDict['Ts'][rowswithinrange].index])
		for i,val in enumerate(exportCurrentData):
			currentEventID[exportDataString[i]] = val

		if skipEventAgg_curFile:
			# load prev aggregated data??
			warn('\nContinued with previously aggregated event data.\nIf you want to add new (or revised) spatial aggregates, change <skipEventAgg> into <False>.\n')
			break	
		
		## Create an index that restarts per event
		times = rawDict.loc[rowswithinrange]['Ts'].unique()
		timesSorted = np.sort(times)
		curEventTime = pd.DataFrame([], columns = ['eventTimeIndex', 'eventTime'])
		for i,val in np.ndenumerate(timesSorted):
			oldIndex = rawDict.loc[rowswithinrange][rawDict.loc[rowswithinrange]['Ts'] == val].index
			eventTime = val - max(times)
			eventTimeIndex = 0 - len(times) + i[0] + 1

			tmp = pd.DataFrame([], columns = ['eventTimeIndex','eventTime'], index = oldIndex)
			tmp['eventTimeIndex'] = eventTimeIndex
			tmp['eventTime'] = eventTime
			curEventTime = curEventTime.append(tmp)

		## Create a new panda that has the identifiers of the current event and the rawdata
		curEventExcerptPanda = pd.concat([curEventTime, currentEventID, rawDict.loc[rowswithinrange], attributeDict.loc[rowswithinrange, attrDictCols]], axis=1) # Skip the duplicate 'Ts' columns
		# # !!!!!!!!!!!!!!!!!!!!!!!!!!!!! remember this for re_indexing
		# Use the new index as the index
		# curEventExcerptPanda = curEventExcerptPanda.set_index('eventTimeIndex', drop=True, append=False, inplace=False, verify_integrity=False)
		# # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!

		# Skip this step for Full?
		# For everything with a t_end --> add normalized time?
		# Interpolate data to allow for direct comparisons between events (also at dataset level)
		interpolatedCurEventExcerptPanda = \
		interpolateEventExcerpt(curEventExcerptPanda,freqInterpolatedData,tStart,tEnd,aggregateLevel)
		
		interpolatedCurEventExcerptPanda.to_csv('C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\NP repository\\test.csv')
		# store interpolatedVals instead of curEventExcerptPanda..
		curEventExcerptPanda.to_csv('C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\NP repository\\test2.csv')


		# Create a panda where all events are stored (before temporal aggregation)
		eventExcerptPanda = eventExcerptPanda.append(interpolatedCurEventExcerptPanda)
		print('Im heeeeeeeeeeeere')
		pdb.set_trace()

		#######################
		## End of prepration ##
		#######################
				
		###################################
		## Start of temporal aggregation ##
		###################################

		## Count exsiting events (goals, possession and passes)
		exportCurrentData,overallString,overallExplanation =\
		countEvents2.goals(window,aggregateLevel[0],targetEvents,TeamAstring,TeamBstring,exportCurrentData,overallString,overallExplanation)
		
		exportCurrentData,overallString,overallExplanation =\
		countEvents2.turnovers(window,aggregateLevel[0],targetEvents,TeamAstring,TeamBstring,exportCurrentData,overallString,overallExplanation)

		exportCurrentData,overallString,overallExplanation =\
		countEvents2.possessions(window,aggregateLevel[0],targetEvents,TeamAstring,TeamBstring,exportCurrentData,overallString,overallExplanation)
		
		exportCurrentData,overallString,overallExplanation =\
		countEvents2.passes(window,aggregateLevel[0],targetEvents,TeamAstring,TeamBstring,exportCurrentData,overallString,overallExplanation)

		## Systematically aggregate all existing spatial aggregates		
		for key in attrDictCols_numeric:
			#####################
			## Prepare the data #
			#####################

			# Include the current content
			# A silly work-around in case the data is not in np.float
			tmp = curEventExcerptPanda[key].index
			if type(curEventExcerptPanda[key][tmp[0]]) == float:
				curEventExcerptPanda[key] = curEventExcerptPanda[key].astype(np.float64)
			curContent = np.isnan(curEventExcerptPanda[key]) == False			
			tmpPlayerID = curEventExcerptPanda['PlayerID'][curContent]
			tmpTeampID = curEventExcerptPanda['TeamID'][curContent]

			## Make outcome variable dependent on current event's refTeam
			# NB: attrLabel is already changed earlier.			
			targetGroup = []
			if all(tmpPlayerID == 'groupRow'):
				targetGroup = 'groupRows'
				if key[-1] == ref:
					# Ref team
					newKey = key[:-1] + '_ref'
					curEventExcerptPanda[newKey] = curEventExcerptPanda[key]
					curEventExcerptPanda.drop(key, axis=1, inplace=True)
					key = newKey

				elif key[-1] == oth:
					# Other
					newKey = key[:-1] + '_oth'
					curEventExcerptPanda[newKey] = curEventExcerptPanda[key]
					curEventExcerptPanda.drop(key, axis=1, inplace=True)
					key = newKey
				else:
					# No reference assigend
					doNothing = [] # right?

			if all(tmpPlayerID == 'ball'): 
				if targetGroup == []:				
					targetGroup = 'ballRows'
				else:
					exit('\nFATAL ERROR: A variable seemed to be covering multiple sets (groupRows and ballRows).\nThis should be avoided or accounted for in the code.\n')
					# To avoid errors: either separate variables covering multiple sets, or add code here that joins targetGroups..
			
			if (all(tmpPlayerID != 'groupRow') and all(tmpPlayerID != 'ball')) and all(tmpTeampID != ''): # i.e., not group, nor ball, but with values in TeamID
				if targetGroup == []:
					targetGroup = 'playerRows'
				else:
					exit('\FATAL ERROR: A variable seemed to be covering multiple sets (playerRows and [groupRows and/or ballRows]).\nThis should be avoided or accounted for in the code.\n')
					# To avoid errors: either separate variables covering multiple sets, or add code here that joins targetGroups..
			
			# Temporal aggregation (across ALL player vals and/or team vals and/or ball vals)
			data = curEventExcerptPanda[key][curContent]
			exportCurrentData, overallString, overallExplanation = \
			aggregateTemporally(data,exportCurrentData,targetGroup,key,attrLabel[key],eventDescrString, overallString, overallExplanation,aggrMethods = aggrMeth_popLevel)

			# Finally, aggregate player level values per team and/or per time separately
			if targetGroup == 'playerRows':
				# When it's a playerRow, the outcome measure is also aggregated per player per team separately.
				for i in np.arange(len(aggregationOrders)):
					aggregationOrder = aggregationOrders[i]
					population = populations[i]
					exportCurrentData, overallString, overallExplanation = \
					aggregateTemporallyINCEPTION(population,aggregationOrder,aggrMeth_popLevel,aggrMeth_playerLevel,curEventExcerptPanda,key,refTeam,othTeam,attrLabel,eventDescrString, exportCurrentData, overallString, overallExplanation)



		# Create a matrix where all temporally aggregated data of each event are stored
		exportMatrix.append(exportCurrentData)

	if debuggingMode:
		elapsed = str(round(time.time() - tTempAgg, 2))
		print('Time elapsed during temporalAggregation: %ss' %elapsed)
	return exportMatrix,overallString,overallExplanation,eventExcerptPanda,attrLabel

# def resampleData(x,y,xnew,order): # order 1) = linear, 2) = quadratic, 3) = cubic
# 	f = InterpolatedUnivariateSpline(x, y, k=order)
# 	f = interp1d(x, y)

# 	ynew = f(xnew)   # use interpolation function returned by `interp1d`
# 	# limit ynew to values around original x
# 	# next(x[0] for x in enumerate(L) if x[1] > 0.7)
# 	return[ynew]

def interpolateEventExcerpt(curEventExcerptPanda,freqInterpolatedData,tStart,tEnd,aggregateLevel):
	#####################################
	## Create a version of curEventExcerptPanda with interpolated values (to have the same EXACT timestamps for all events)
	# replace 'eventTimeIndex'

	## INCLUDE it in the key level loop. Separately add all columns that are not in attributDictCols (which may not need to be interpolated??)
	# eventLevelInterpolation (for the purpose of aligning the various timeseries --> averaging, plotting, etc.)
	
	everyPlayerIDs = pd.unique(curEventExcerptPanda['PlayerID'])	
	# Create the time (X) that can be used for interpolation
	###### this one worked
	# tmp = np.arange(tStart,tEnd + 1/freqInterpolatedData, 1/freqInterpolatedData) # referring to eventTime
	tStartRound = np.round(tStart,math.ceil(math.log(freqInterpolatedData,10)))
	tEndRound = np.round(tEnd,math.ceil(math.log(freqInterpolatedData,10)))
	tmp = np.arange(tStartRound,tEndRound + 1/freqInterpolatedData, 1/freqInterpolatedData) # referring to eventTime

	X_int = np.round(tmp,math.ceil(math.log(freqInterpolatedData,10))) # automatically rounds it to the required number of significant numbers (decimal points) for the given sampling frequency (to avoid '0' being stored as 1.474746 E16 AND 1.374750987 E16)
	
	# Create an array that is empty and has the same size as the interpoloated values will have.
	newIndex = np.arange(0,len(everyPlayerIDs) * len(X_int))
	# interpolatedVals = pd.DataFrame(np.nan, index=newIndex, columns=[key])			
	interpolatedVals = pd.DataFrame(index=newIndex)			

	## admittedly not the prettiest way....
	# It requires a for loop per key, then a for loop per unique playerID....
	#
	# Interpolate each key separately
	for key in curEventExcerptPanda.keys():
	# Only interpolate numerical keys.
	# The other keys should be treated differently
	# Do the interpolation separately for every player

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
				
				# try:
				Y_int = s(X_int)				
				# except:
				# 	print(min(curX[1:-1]),min(X_int))		
				# 	print(max(curX[1:-1]),max(X_int))		
				# 	print('---')
				# 	print(curX)
				# 	print('----')
				# 	print(X_int)
				# 	pdb.set_trace()
				# 104.11 96.8
				# 111.81 111.8

				# 70.41 70.3
				# 85.32 85.3
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

	return interpolatedVals

def aggregateTemporallyINCEPTION(population,aggregationOrder,aggrMeth_popLevel,aggrMeth_playerLevel,curEventExcerptPanda,key,refTeam,othTeam,attrLabel,eventDescrString, exportCurrentData, overallString, overallExplanation):
	
	# Allplayers of refTeam
	curContent_refTeam = curEventExcerptPanda['TeamID'] == refTeam
	pivotedData_refTeam = curEventExcerptPanda[curContent_refTeam].pivot(index = 'eventTimeIndex', columns = 'PlayerID', values = key)
	# Allplayers of othTeam
	curContent_othTeam = curEventExcerptPanda['TeamID'] == othTeam
	pivotedData_othTeam = curEventExcerptPanda[curContent_othTeam].pivot(index = 'eventTimeIndex', columns = 'PlayerID', values = key)

	# All players
	curContent_allTeam = scipy.logical_or(curContent_othTeam,curContent_refTeam)
	pivotedData = curEventExcerptPanda[curContent_allTeam].pivot(index = 'eventTimeIndex', columns = 'PlayerID', values = key)

	# print(pivotedData_refTeam.keys())
	if '1.0' in pivotedData_refTeam.keys():
		pivotedData_refTeam['2.0'] = pivotedData_refTeam['1.0']
		pivotedData_refTeam['3.0'] = pivotedData_refTeam['1.0']
		pivotedData_refTeam['4.0'] = pivotedData_refTeam['1.0']
	if '5.0' in pivotedData_refTeam.keys():
		pivotedData_refTeam['6.0'] = pivotedData_refTeam['5.0']
		pivotedData_refTeam['7.0'] = pivotedData_refTeam['5.0']
		pivotedData_refTeam['8.0'] = pivotedData_refTeam['5.0']
		# pivotedData_refTeam.to_csv('C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\NP repository\\test.csv')

	if population == 'allTeam':
		tmpData = pivotedData
		popString = 'all'
	elif population == 'refTeam':
		tmpData = pivotedData_refTeam
		popString = 'ref'
	elif population == 'othTeam':
		tmpData = pivotedData_othTeam
		popString = 'oth'
	else:
		warn('\nWARNING: Did not recognize aggregation population (one of the teams, or both).\nSpecify it with either <allTeam>, <refTeam> or <othTeam>. \nContinued by default with <allTeam>.')
		
	# two ways:
	# 1) average per time per player (of a team)
	# 2) average per player (of a team) per time
	if aggregationOrder == 'perTimePerPlayer':
		axis = 0
	elif aggregationOrder == 'perPlayerPerTime':
		axis = 1
	else:
		warn('\nWARNING: Did not recognize aggregation order (first over time, or first over player).\nSpecify it with either <perTimePerPlayer> or <perPlayerPerTime>. \nContinued by default with <perPlayerPerTime>.')
		axis = 0

	count, avg, std, sumVal, minVal, maxVal, med, sem, kur, ske = \
	aggregateTemporally(tmpData,no_export = True, axis = axis) # axis = 0 ==> per player; axis = 1 ==> per time

	# # aggrMethStrings = ['avg', 'std', 'sumVal', 'minVal', 'maxVal', 'med', 'sem', 'kur', 'ske']
	# aggrMethStrings = ['avg', 'std']
	for meth in aggrMeth_playerLevel:
		if meth == 'avg':
			data = avg
		elif meth == 'std':
			data = std
		elif meth == 'sumVal':
			data = sumVal
		elif meth == 'minVal':
			data = minVal
		elif meth == 'maxVal':
			data = maxVal
		elif meth == 'med':
			data = med
		elif meth == 'sem':
			data = sem
		elif meth == 'kur':
			data = kur
		elif meth == 'ske':
			data = ske
		elif meth == 'cnt':
			data = count
		else:
			warn('\nFATAL WARNING: Could not identify aggregation method <%s>.' %meth)
		
		curKeyString = key + '_' + meth + '_' + aggregationOrder + '_' + popString

		exportCurrentData, overallString, overallExplanation = \
		aggregateTemporally(data,exportCurrentData,population,curKeyString,attrLabel[key],eventDescrString, overallString, overallExplanation, aggrMethods = aggrMeth_popLevel)

	return exportCurrentData, overallString, overallExplanation

def aggregateTemporally(data,*positional_parameters,**keyword_parameters):

	nAxis = 0
	if 'axis' in keyword_parameters:
		axis = keyword_parameters['axis']
		nAxis = (axis - 1) * -1
	else:
		axis = 0		
	no_export = False
	if 'no_export' in keyword_parameters:
		no_export = keyword_parameters['no_export']
	
	count = np.count_nonzero(~np.isnan(data), axis = axis)
	avg = np.nanmean(data, axis = axis)
	std = np.nanstd(data, axis = axis)
	sumVal = np.sum(data, axis = axis)
	minVal = np.min(data, axis = axis)
	maxVal = np.max(data, axis = axis)
	med = np.median(data, axis = axis)
	sem = stats.sem(data, axis = axis)
	kur = stats.kurtosis(data, axis = axis)
	ske = stats.skew(data, axis = axis)

	if no_export:
		return count, avg, std, sumVal, minVal, maxVal, med, sem, kur, ske

	# continue to create the strings in the full version
	exportCurrentData,targetGroup, curKeyString, curLabel, eventDescrString, overallString, overallExplanation = positional_parameters

	if 'aggrMethods' in keyword_parameters:
		aggrMethods = keyword_parameters['aggrMethods']
	else:
		aggrMethods = ['avg', 'std', 'sumVal', 'minVal', 'maxVal', 'med', 'sem', 'kur', 'ske']
		warn('\nWARNING: No aggregation methods specified, used them all.\nSpecify <aggrMethods> if you want to reduce the output.\n')

	if 'cnt' in aggrMethods:
		exportCurrentData.append(count)
		overallString.append(curKeyString + '_cnt')				
		overallExplanation.append('Number of occurences (i.e., n) of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))

	if 'avg' in aggrMethods:
		exportCurrentData.append(avg)
		overallString.append(curKeyString + '_avg')				
		overallExplanation.append('Average of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))

	if 'std' in aggrMethods:
		exportCurrentData.append(std)
		overallString.append(curKeyString + '_std')
		overallExplanation.append('Standard Deviation of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))

	if 'sum' in aggrMethods:
		exportCurrentData.append(sumVal)
		overallString.append(curKeyString + '_sum')
		overallExplanation.append('The Sum of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))

	if 'min' in aggrMethods:
		exportCurrentData.append(minVal)
		overallString.append(curKeyString + '_min')
		overallExplanation.append('Minimum of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))
	
	if 'max' in aggrMethods:
		exportCurrentData.append(maxVal)
		overallString.append(curKeyString + '_max')
		overallExplanation.append('Maximum of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))

	if 'med' in aggrMethods:
		exportCurrentData.append(med)
		overallString.append(curKeyString + '_med')
		overallExplanation.append('The Median of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))
	
	if 'sem' in aggrMethods:
		exportCurrentData.append(sem)
		overallString.append(curKeyString + '_sem')				
		overallExplanation.append('The Standard Error of Measurment (SEM) of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))
	
	if 'kur' in aggrMethods:
		exportCurrentData.append(kur)
		overallString.append(curKeyString + '_kur')				
		overallExplanation.append('Kurtosis of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))
	
	if 'ske' in aggrMethods:
		exportCurrentData.append(ske)
		overallString.append(curKeyString + '_ske')
		overallExplanation.append('Skewness of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))

	return exportCurrentData, overallString, overallExplanation