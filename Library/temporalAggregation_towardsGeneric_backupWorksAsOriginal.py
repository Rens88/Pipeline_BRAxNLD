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

import FillGaps_and_Filter
import csv
import pdb; #pdb.set_trace()
import numpy as np
import pandas as pd
from os.path import isfile, join, isdir
from os import listdir, path
from warnings import warn, filterwarnings
import time
# From my own library:
import logging
from datetime import datetime
from os.path import isfile, join, exists, realpath, abspath, split,dirname, isdir, basename
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

def process(targetEvents,aggregateLevel,rawDict,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring,debuggingMode,skipEventAgg_curFile,fileIdentifiers,attrLabel,aggregatePerPlayer,includeEventInterpolation,datasetFramerate):
	tTempAgg = time.time()
	FileID = "_".join(fileIdentifiers)
						
	eventExcerptPanda = checkIfDataExists(aggregateLevel,debuggingMode,tTempAgg,exportData,exportDataString,exportFullExplanation,attrLabel,targetEvents)

	attributeDict = cleanUp_ballSpeedAcceleration(rawDict,attributeDict) # something that could be part of clean-up:
	
	freqInterpolatedData,populations,aggregationOrders,aggrMeth_playerLevel,aggrMeth_popLevel = userInput()

	overallString_prev,overallString_longest,exportAsPanda,fileIdentifiers,exportDataString,exportFullExplanation = \
	prepareExportStrings(exportDataString,aggregateLevel,exportFullExplanation)


	# All the outcome measures that exist
	attrDictCols = [i for i in attributeDict.columns if not i in ['Ts','TeamID','PlayerID','X','Y']]
	# Omit outcome variables that are not numeric. These should be aggregated using countEvents2 (see below)
	# NB: May be this should be written more generic..
	attrDictCols_numeric = [tmp for tmp in attrDictCols if tmp not in ['Run', 'Possession/Turnover', 'Pass', 'Goal']]
	# print(attributeDict.keys())
	## FYI:
	# aggregateLevel[0] --> event ID
	# aggregateLevel[1] --> window (s)
	# aggregateLevel[2] --> lag (s)
	# Create an empty to export when there are no events??

	if type(targetEvents[aggregateLevel[0]]) == tuple:
		warn('\nWARNING: Make sure the format of the event fits in a list. \nIf not, the script will fail when there is only 1 event.\n')

	## Loop over every event
	for idx,currentEvent in enumerate(targetEvents[aggregateLevel[0]]):
		# print('This is event <%s>:\n%s' %(idx,currentEvent))
		
		#####################################
		## Prepare export of current event ##
		#####################################
		if currentEvent[0] == None:
			try:
				warn('\nWARNING: Event had no time of occurrence. Therefore, it was skipped.\nThese are the event contents:\ntime of event = %s\nRefTeam of event = %s\nend of event = %s' %currentEvent)
			except: # a lazy way to catch currentEvents with only 2 bits of info.
				warn('\nWARNING: Event had no time of occurrence. Therefore, it was skipped.\nThese are the event contents:\ntime of event = %s\nRefTeam of event = %s\n' %currentEvent)				
			continue
		## Determine the window
		if aggregateLevel[1] != None:
			# try:
			tEnd = currentEvent[0] - aggregateLevel[2] # tEnd - lag
			# except:
			# 	print(currentEvent)
			# 	print(aggregateLevel)
			# 	pdb.set_trace()
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
		overlapFoundHere = False
		if idx != 0:
			tEnd_prevEvent = targetEvents[aggregateLevel[0]][idx-1][0]
			# # store the time in-between events that can help determine the maximum window size you may want to choose
			# availableWindow.append(tEnd - tEnd_prevEvent)

			if tEnd_prevEvent > tStart:
				overlapFoundHere = True

		
		
		# tmp = rawDict[rawDict['Ts'] > window[0]] 
		tmp = attributeDict[attributeDict['Ts'] > window[0]]
		rowswithinrange = tmp[tmp['Ts'] <= window[1]].index

		if rowswithinrange.empty:
			# no data found, probably because the event times lie outside of the available positional data.
			######if min(rawDict['Ts']) < window[0] and max(rawDict['Ts']) > window[1]:
			if min(rawDict['Ts']) > window[1] or max(rawDict['Ts']) < window[0]:
				# Indeed, no data available for given event.
				warn('\nWARNING: No positional data available for when the requested event occurred.\nAs a work-around, this event was skipped.\nThis is an indication that the positional data and the event data do not lign up.\nmin(Ts) = %s\nmax(Ts) = %s\nEvent:\n%s' %(min(rawDict['Ts']),max(rawDict['Ts']),currentEvent))
				# pdb.set_trace()
				continue
		
		## Prepare a copy of exported data so far (which can be considered as file identifiers)

		exportCurrentData = exportData.copy()
		overallString = exportDataString.copy()
		overallExplanation = exportFullExplanation.copy()
		
		# print('\nBefore appending trial identifiers:')
		# print('len(exportCurrentData) = %s' %len(exportCurrentData))
		# print('len(overallString) = %s' %len(overallString))
		# print('len(overallExplanation) = %s' %len(overallExplanation))

		## Create the output string that identifies the current event
		aggregateString = '%s_%03d' %(aggregateLevel[0],idx)		
		if idx > 1000:
			aggregateString = '%s_%d' %(aggregateLevel[0],idx)
			warn('\nWARNING: More than 1000 occurrences of the same event.\nForced to change format for finding aggregateString.\nConsider pre-allocating the number of digits included by default.')
		
		exportCurrentData.append(aggregateString) # NB: String and explanation are defined before the for loop
		
		## Create unique event identifier 
		EventUID = FileID + '_' + aggregateString
		exportCurrentData.append(EventUID) # NB: String and explanation are defined before the for loop

		# not yet a generic solution.
		# TO DO: WRITE THIS GENERICALLY
		if aggregateLevel[0] == 'Turnovers':
			eventClassification = currentEvent[6]
			exportCurrentData.append(eventClassification)
		else:
			exportCurrentData.append('Undefined')
			warn('\nWARNING: need to add a generic eventClassification.\n')

		## Assign refTeam
		refTeam = currentEvent[1]
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
			warn('\nWARNING: refTeam <%s> did not correspond with TeamAstring <%s> or TeamBstring <%s>.\nCould not establish who the reference team was.\nContinued with <%s> as refTeam.\n' %(refTeam,TeamAstring,TeamBstring,TeamAstring))
			ref = 'A'
			oth = 'B'
			refTeam = TeamAstring
			othTeam = TeamBstring
		exportCurrentData.append(refTeam) # NB: String and explanation are defined before the for loop	
		exportCurrentData.append(othTeam) # NB: String and explanation are defined before the for loop	
		eventOverlap = currentEvent[-1] # should always be the last value in currentEvent
		exportCurrentData.append(eventOverlap) # NB: String and explanation are defined before the for loop
		if not type(eventOverlap) == bool:
			warn('\nWARNING: Something went wrong with appending the boolean that indicates whether there was overlap in the events. See computeEvents.py.\n')
		if not eventOverlap == overlapFoundHere:
			warn('\nWARNING: Current event start <%ss> before previous event finished <%ss>.\nThis should have been dealt with in computeEvents.\nStrange..' %(tStart,tEnd_prevEvent))

		## Change attribute keys to refer to refTeam and othTeam		
		# WEAKNESS: relies on attribute keys to end with 'A' or 'B' in team scenarios.
		keyAttr = attrLabel.keys()
		copyTheseKeys_A = [ikey for ikey in keyAttr if ikey[-1] == 'A']
		copyTheseKeys_B = [ikey for ikey in keyAttr if ikey[-1] == 'B']

		# Write a warning: If a gropuRow that doesn't end in A or B, warn about the weakness
		groupRowsInds = rawDict[rawDict['PlayerID'] == 'groupRow'].index
		playerRowsInds = rawDict[rawDict['PlayerID'] != 'groupRow'].index

		for attrKey in attributeDict.keys():
			if attrKey in ['PlayerID','TeamID','Ts','X','Y']:
				continue
			if all(attributeDict[attrKey].isnull()):
				# No data at all..
				warn('\nWARNING: No data was found in <%s>.\nEither something went wrong in the analysis, or this feature was missing from the start..\n' %attrKey)
				# Take it out of attrDictCols_numeric
				if attrKey in attrDictCols_numeric:
					attrDictCols_numeric.remove(attrKey)

				continue
			
			## if all(attributeDict.loc[groupRowsInds,attrKey].notnull()):
				# assumes that all grouprows have a value
				# however, whenever only one team is visible for some time (for example during the break), this is possible!
				# So I formulated an alternative:
				# Of the group key, all player rows should be zero
		
			if all(attributeDict.loc[playerRowsInds,attrKey].isnull()):				
				if attrKey in ['Run', 'Goal', 'Possession/Turnover', 'Pass']:
					continue
				if not (attrKey[-1] == 'A' or attrKey[-1] == 'B'):
					warn('\nWARNING: The pipeline recognizes team spatial aggregates based on whether they end with <A> or <B> (case-sensitive).\nIt seems that a team spatial aggregate <%s> did not end with A or B.\nIf you want the spatial aggregate to be split based on the reference team, change the name of the outcome variable to end with A or B.\n' %attrKey)
					print('------Group Rows in rawDict---------')
					print(rawDict.loc[groupRowsInds,['PlayerID']])
					print('------Group Rows---------')
					print(attributeDict.loc[groupRowsInds,['PlayerID',attrKey]])
					print('------Player Rows---------')
					print(attributeDict.loc[playerRowsInds,attrKey])
					pdb.set_trace()
			else:

				if attrKey[-1] == 'A' or attrKey[-1] == 'B':
					warn('\nWARNING: The pipeline recognizes team spatial aggregates based on whether they end with <A> or <B> (case-sensitive).\nIt seems that a player spatial aggregate <%s> ended with A or B.\nThis may cause problems. Avoid using A or B as the last string of any player level outcome variables.\n' %attrKey)
					print(rawDict.loc[groupRowsInds,['PlayerID']])
					print(attributeDict.loc[groupRowsInds,['PlayerID',attrKey]])
					print('There where this many not nulls:\n%s' %sum(attributeDict.loc[groupRowsInds,attrKey].notnull()))
					print('Out of %s' %len(groupRowsInds))
					print('Temporary explanation: its an empty variable (such as team surface which requires at least 3 players at every timestep. To test this, Im letting it continue and it should be picked up further down.')
					print('************')
					# pdb.set_trace()

		for ikey in copyTheseKeys_A:
			attrLabel.update({ikey[:-1] + '_ref': attrLabel[ikey].replace(TeamAstring,'refTeam')})
		for ikey in copyTheseKeys_B:
			attrLabel.update({ikey[:-1] + '_oth': attrLabel[ikey].replace(TeamBstring,'othTeam')})

		## All data in exportCurrentData are trial identifiers. Create a column in a new panda copying these:
		# Copy everything that exists in exportCurrentData up until here into a new dataFrame.
		currentEventID = pd.DataFrame([],columns = exportDataString,index = rawDict['Ts'][rowswithinrange].index,dtype = object)
		# currentEventID = pd.DataFrame(['test']*1571,columns = ['test'],index = rawDict['Ts'][rowswithinrange].index,dtype = object)
		
		for i,val in enumerate(exportCurrentData):
			currentEventID[exportDataString[i]] = val

		if skipEventAgg_curFile:
			# it's probably here because the currentEventID is used to identify the trial in the eventAggFile
			# load prev aggregated data??
			warn('\nContinued with previously aggregated event data.\nIf you want to add new (or revised) spatial aggregates, change <skipEventAgg> into <False>.\n')
			break	
		
		## Create an index that restarts per event
		times = rawDict.loc[rowswithinrange]['Ts'].unique()
		timesSorted = np.sort(times)
		curEventTime = pd.DataFrame([], columns = ['eventTimeIndex', 'eventTime'],dtype = np.float64)
		for i,val in np.ndenumerate(timesSorted):
			oldIndex = rawDict.loc[rowswithinrange][rawDict.loc[rowswithinrange]['Ts'] == val].index
			eventTime = val - max(times)
			eventTimeIndex = 0 - len(times) + i[0] + 1

			tmp = pd.DataFrame([], columns = ['eventTimeIndex','eventTime'], index = oldIndex)
			tmp['eventTimeIndex'] = eventTimeIndex
			tmp['eventTime'] = eventTime
			curEventTime = curEventTime.append(tmp)


		if isinstance(curEventTime.index, pd.core.index.MultiIndex):
			warn('\nFATAL WARNING: Somehow, the index of <curEventTime> turned out as a multi-index.\nOn Francium (and probably on Linux in general), this will result into problems with pd.concat.\nThe solution is to reset the index and then assign a new index (that still represents the same order as the order dataframes that are concatenated).\n')
		if isinstance(currentEventID.index, pd.core.index.MultiIndex):
			warn('\nFATAL WARNING: Somehow, the index of <currentEventID> turned out as a multi-index.\nOn Francium (and probably on Linux in general), this will result into problems with pd.concat.\nThe solution is to reset the index and then assign a new index (that still represents the same order as the order dataframes that are concatenated).\n')
		if isinstance(rawDict.loc[rowswithinrange].index, pd.core.index.MultiIndex):
			warn('\nFATAL WARNING: Somehow, the index of <rawDict.loc[rowswithinrange]> turned out as a multi-index.\nOn Francium (and probably on Linux in general), this will result into problems with pd.concat.\nThe solution is to reset the index and then assign a new index (that still represents the same order as the order dataframes that are concatenated).\n')
		if isinstance(attributeDict.loc[rowswithinrange, attrDictCols].index, pd.core.index.MultiIndex):
			warn('\nFATAL WARNING: Somehow, the index of <attributeDict.loc[rowswithinrange, attrDictCols]> turned out as a multi-index.\nOn Francium (and probably on Linux in general), this will result into problems with pd.concat.\nThe solution is to reset the index and then assign a new index (that still represents the same order as the order dataframes that are concatenated).\n')

		## Create a new panda that has the identifiers of the current event and the rawdata
		curEventExcerptPanda = pd.concat([curEventTime, currentEventID, rawDict.loc[rowswithinrange], attributeDict.loc[rowswithinrange, attrDictCols]], axis=1) # Skip the duplicate 'Ts' columns


		if includeEventInterpolation:
			interpolatedCurEventExcerptPanda = \
			interpolateEventExcerpt(curEventExcerptPanda,freqInterpolatedData,tStart,tEnd,aggregateLevel,refTeam,datasetFramerate)
		else:
			interpolatedCurEventExcerptPanda = curEventExcerptPanda

		#######################
		## End of prepration ##
		#######################
				
		###################################
		## Start of temporal aggregation ##
		###################################
		# print('\nBefore counting events:')
		# print('len(exportCurrentData) = %s' %len(exportCurrentData))
		# print('len(overallString) = %s' %len(overallString))
		# print('len(overallExplanation) = %s' %len(overallExplanation))

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
		optionalInput = {'window':[],'eventDescrString':[],'overallExplanation':[],'attrLabel':[]}
		optionalInput['window'] = window
		optionalInput['eventDescrString'] = eventDescrString
		optionalInput['overallExplanation'] = overallExplanation
		optionalInput['attrLabel'] = attrLabel

		exportCurrentData, overallString, overallExplanation =\
		aggregateCurrentEvent(attrDictCols_numeric,attributeDict,interpolatedCurEventExcerptPanda,TeamAstring,TeamBstring,refTeam,othTeam,ref,oth,exportCurrentData,aggrMeth_popLevel,aggrMeth_playerLevel,aggregatePerPlayer,aggregationOrders,overallString,optionalInput,populations)
		
		# print(len(overallString))
		# print(len(exportCurrentData))
		# print(len(overallString_prev))
		if overallString_prev != []:
			if len(overallString_prev) > len(overallString):
				missingOutput = [s for s in overallString_prev if not s in overallString]
				warn('\nWARNING: The following features were previously exported, but not currently:\n<%s>\nThe missing values were replaced with <NaN>.\n' %missingOutput)

			elif len(overallString_prev) < len(overallString):
				missingOutput = [s for s in overallString if not s in overallString_prev]
				warn('\nWARNING: The following features were currently exported, but not previously:\n<%s>\nThe missing values were replaced with <NaN>.\n' %missingOutput)
					# The number of features exported per event was not the same.\nThis will result in an alignment problem in the output.\nExported previously:\n%s\n\nExported currently:\n%s\n' %(overallString_prev,overallString))
		overallString_prev = overallString
		
		if len(overallString) > len(overallString_longest):
			overallString_longest = overallString

		if not len(overallString) == len(exportCurrentData):
			warn('\nWARNING: Not every output data had an outputstring.\nThis could be an indication that the order of exporting data was incorrect.\nBesides, for clarity it is always better to immediately write up a dataString.\n')
		# Create a panda where all events are stored (before temporal aggregation)
		# This includes the ref_team edits.
		eventExcerptPanda = eventExcerptPanda.append(interpolatedCurEventExcerptPanda)

		# Here, add the player IDs for player levels vars that were aggregated at the player level
		if not aggregatePerPlayer == []:
			for i in np.arange(0,22):
				overallString.append('Player_%02d' %(i+1))
				overallExplanation.append('The PlayerID of the <%sth> player as represented in the individually aggregated variables for the current event.' %i)
				
				if len(players) >= i + 1:
					exportCurrentData.append(players[i])				
				else:
					exportCurrentData.append(np.nan)

		# print(type(overallString))
		# print(type(exportCurrentData))
		# print(len(overallString))
		# print(len(exportCurrentData))

		currentAsPanda = pd.DataFrame([[d for d in exportCurrentData]],columns=overallString,index = [idx])
		exportAsPanda = pd.concat([exportAsPanda, currentAsPanda],axis = 0)

		# exportAsPanda
		# Create a matrix where all temporally aggregated data of each event are stored
		#####exportMatrix.append(exportCurrentData)

	#####warn('\nTO DO: !!!!!: Rather than converting pandas to lists at the end of temporalAggregation, I should re-write the export to be based on pandas. Much safer.\nAlso, export should happen in temporalAggregation.\n')
	#####exportAsPanda = exportAsPanda[overallString_longest]
	#####exportAsPanda_toMatrix = exportAsPanda.as_matrix()
	#####exportAsPanda_toMatrix_toList = exportAsPanda_toMatrix.tolist()
	#####exportMatrix = exportAsPanda_toMatrix_toList

	exportMatrix = exportAsPanda[overallString_longest]
	if debuggingMode:
		elapsed = str(round(time.time() - tTempAgg, 2))
		print('***** Time elapsed during temporalAggregation: %ss' %elapsed)
	return exportMatrix,overallString,overallExplanation,eventExcerptPanda,attrLabel

#################################################################################################################################################
######################################################## SUB MODULES ############################################################################
#################################################################################################################################################
#################################################################################################################################################

def checkIfDataExists(aggregateLevel,debuggingMode,tTempAgg,exportData,exportDataString,exportFullExplanation,attrLabel,targetEvents):
	# Create an empty dataFrame where the eventExcerpt will be stored.
	eventExcerptPanda = pd.DataFrame()

	if aggregateLevel[0] == 'None':
		warn('\nWARNING: No temporal aggregate level indicated. \nNo temporally aggregated data exported.\nChange aggregateEvent = <%s> in USER INPUT.\n' %aggregateLevel[0])
		if debuggingMode:
			elapsed = str(round(time.time() - tTempAgg, 2))
			print('***** Time elapsed during temporalAggregation: %ss' %elapsed)
		return exportData,exportDataString,exportFullExplanation,eventExcerptPanda,attrLabel

	elif len(targetEvents[aggregateLevel[0]]) == 0:
		warn('\nWARNING: No targetevents detected. \nCouldnt aggregate temporally. \nNo Data exported.\n')
		if debuggingMode:
			elapsed = str(round(time.time() - tTempAgg, 2))
			print('***** Time elapsed during temporalAggregation: %ss' %elapsed)
		return exportData,exportDataString,exportFullExplanation,eventExcerptPanda,attrLabel

	return eventExcerptPanda
def cleanUp_ballSpeedAcceleration(rawDict,attributeDict):
	# something that could be part of clean-up:
	if not all(np.isnan(attributeDict.loc[rawDict['PlayerID'] == 'ball','Acceleration'])):
		attributeDict.loc[rawDict['PlayerID'] == 'ball','Acceleration'] = np.nan
		warn('\nWARN: Some datasets also give the acceleration of the ball.\nTo avoid conflicts, these input values will be overwritten with empty values.\nIf you are in fact interested in Acceleration of the ball, then create a new feature that refers to the ball specifically (ballAcceleration).\n')
	if not all(np.isnan(attributeDict.loc[rawDict['PlayerID'] == 'ball','Speed'])):
		attributeDict.loc[rawDict['PlayerID'] == 'ball','Speed'] = np.nan
		warn('\nWARN: Some datasets also give the Speed of the ball.\nTo avoid conflicts, these input values will be overwritten with empty values.\nIf you are in fact interested in Speed of the ball, then create a new feature that refers to the ball specifically (ballSpeed).\n')
	return attributeDict

def prepareExportStrings(exportDataString,aggregateLevel,exportFullExplanation):
	overallString_prev = []
	overallString_longest = []
	# exportAsPanda = pd.DataFrame([])
	warn('\nWARNING: Because for some trials Acceleration and speed are missing, Im creating these columns in advance.\nGood enough for a temporary work-around.\BUT I foresee it will be problematic with any readAttributCols that end up being (partially) missing.\n***\n***\n***\n***\n***\n***\n***\n***\n***\n***\n***\n***\n')
	exportAsPanda = pd.DataFrame([],columns = ['Acceleration_avg_perTimePerPlayer_all_cnt', 'Acceleration_avg_perTimePerPlayer_all_avg', 'Acceleration_avg_perTimePerPlayer_all_std', 'Acceleration_std_perTimePerPlayer_all_cnt', 'Acceleration_std_perTimePerPlayer_all_avg', 'Acceleration_std_perTimePerPlayer_all_std', 'Acceleration_avg_perTimePerPlayer_ref_cnt', 'Acceleration_avg_perTimePerPlayer_ref_avg', 'Acceleration_avg_perTimePerPlayer_ref_std', 'Acceleration_std_perTimePerPlayer_ref_cnt', 'Acceleration_std_perTimePerPlayer_ref_avg', 'Acceleration_std_perTimePerPlayer_ref_std', 'Acceleration_avg_perTimePerPlayer_oth_cnt', 'Acceleration_avg_perTimePerPlayer_oth_avg', 'Acceleration_avg_perTimePerPlayer_oth_std', 'Acceleration_std_perTimePerPlayer_oth_cnt', 'Acceleration_std_perTimePerPlayer_oth_avg', 'Acceleration_std_perTimePerPlayer_oth_std', 'Speed_avg_perTimePerPlayer_all_cnt', 'Speed_avg_perTimePerPlayer_all_avg', 'Speed_avg_perTimePerPlayer_all_std', 'Speed_std_perTimePerPlayer_all_cnt', 'Speed_std_perTimePerPlayer_all_avg', 'Speed_std_perTimePerPlayer_all_std', 'Speed_avg_perTimePerPlayer_ref_cnt', 'Speed_avg_perTimePerPlayer_ref_avg', 'Speed_avg_perTimePerPlayer_ref_std', 'Speed_std_perTimePerPlayer_ref_cnt', 'Speed_std_perTimePerPlayer_ref_avg', 'Speed_std_perTimePerPlayer_ref_std', 'Speed_avg_perTimePerPlayer_oth_cnt', 'Speed_avg_perTimePerPlayer_oth_avg', 'Speed_avg_perTimePerPlayer_oth_std', 'Speed_std_perTimePerPlayer_oth_cnt', 'Speed_std_perTimePerPlayer_oth_avg', 'Speed_std_perTimePerPlayer_oth_std'])
	## If you want a combination of all possible methods, then use these strings: (NB, this explodes the number of features)
	# aggregationOrders = ['perTimePerPlayer','perTimePerPlayer','perTimePerPlayer','perPlayerPerTime','perPlayerPerTime','perPlayerPerTime']
	# populations = ['allTeam','refTeam','othTeam','allTeam','refTeam','othTeam']
	# aggrMeth_playerLevel = ['avg', 'std', 'sumVal', 'minVal', 'maxVal', 'med', 'sem', 'kur', 'ske']
	# aggrMeth_popLevel = ['avg', 'std', 'sumVal', 'minVal', 'maxVal', 'med', 'sem', 'kur', 'ske']
	

	# The export matrix includes a range of outcome measures that don't change per event (these are already in <exportData>)
	# Most outcome variables will be added by simply going through all the spatially aggregated variables
	# Some other outcome measures describe the current event and can also be created here:
	#####exportMatrix = [] # in which you can combine the exported data of each event in this trial / file

	fileIdentifiers = exportDataString.copy()
	exportDataString.append('temporalAggregate') # output string that identifies the current event
	exportFullExplanation.append('Level of temporal aggregation, based on <<%s>> event and counted chronologically' %aggregateLevel[0])
	exportDataString.append('EventUID') # output string that UNIQUELY identifies the current event
	exportFullExplanation.append('Unique identifier of the <<%s>> events.' %aggregateLevel[0])
	# not yet a generic solution.
	# TO DO: WRITE THIS GENERICALLY
	
	exportDataString.append('eventClassification')
	exportFullExplanation.append('Classification of the current event.')
	exportDataString.append('RefTeam')
	exportFullExplanation.append('Teamstring of the reference team the <<%s>> events refer to.' %aggregateLevel[0])
	exportDataString.append('OthTeam')
	exportFullExplanation.append('Teamstring of the other team (i.e., not reference) the <<%s>> events refer to.' %aggregateLevel[0])
	exportDataString.append('eventOverlap')
	exportFullExplanation.append('Boolean that indicates whether there was overlap with the previous event (True) or if there was NO overlap (False).')

	return overallString_prev,overallString_longest,exportAsPanda,fileIdentifiers,exportDataString,exportFullExplanation

def userInput():
	## user inputs, could easily lift this out of function
	freqInterpolatedData = 10 # in Hz

	## Specify which aggregates are taken.
	aggregationOrder = ['perTimePerPlayer'] 

	populations = ['allTeam','refTeam','othTeam']
	aggregationOrders = aggregationOrder*len(populations)
	aggrMeth_playerLevel = ['avg', 'std'] # specifically for pertime perplayer
	aggrMeth_popLevel = ['avg', 'std','cnt']
	return freqInterpolatedData,populations,aggregationOrders,aggrMeth_playerLevel,aggrMeth_popLevel


def interpolateEventExcerpt(curEventExcerptPanda,freqInterpolatedData,tStart,tEnd,aggregateLevel,refTeam,datasetFramerate):
	#####################################
	## Create a version of curEventExcerptPanda with interpolated values (to have the same EXACT timestamps for all events)
	# replace 'eventTimeIndex'

	# Create the time (X) that can be used for interpolation (X_int)
	tStartRound = np.round(tStart,math.ceil(math.log(freqInterpolatedData,10)))
	tEndRound = np.round(tEnd,math.ceil(math.log(freqInterpolatedData,10)))
	tmp = np.arange(tStartRound,tEndRound + 1/freqInterpolatedData, 1/freqInterpolatedData) # referring to eventTime
	X_int = np.round(tmp,math.ceil(math.log(freqInterpolatedData,10))) # automatically rounds it to the required number of significant numbers (decimal points) for the given sampling frequency (to avoid '0' being stored as 1.474746 E16 AND 1.374750987 E16)

	fatalIssue = False
	interpolatedVals,fatalIssue = FillGaps_and_Filter.fillGaps(curEventExcerptPanda,eventInterpolation = True,fixed_X_int = X_int,aggregateLevel = aggregateLevel,datasetFramerate = datasetFramerate)
	if 'fatalIssue' in interpolatedVals:		
		fatalIssue = True
	if fatalIssue == True:
		warn('\nWARNING: uhoh... picked up a fatal error later on in the pipeline. Likely a processing error (not a data issue).')
		print(interpolatedVals)
		pdb.set_trace()
	# interpolatedVals.to_csv('C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\NP repository\\test.csv')

	##### This WORKED as well:
	# interpolatedVals = FillGaps_and_Filter.fillGapsConsistentTimestamps(curEventExcerptPanda,X_int,aggregateLevel,refTeam)
	#
	##### This works as well:
	### interpolatedVals = FillGaps_and_Filter.fillGaps(curEventExcerptPanda, checkForJumps = True)
	### for ix,q in enumerate(np.sort(interpolatedVals['Ts'].unique())):
	### 	interpolatedVals.loc[interpolatedVals['Ts'] == q  ,'eventTimeIndex'] = ix - len(np.sort(interpolatedVals['Ts'].unique())) + 1

	return interpolatedVals

def aggregateTemporallyINCEPTION(population,aggregationOrder,aggrMeth_popLevel,aggrMeth_playerLevel,curEventExcerptPanda,key,refTeam,othTeam,attrLabel,eventDescrString, exportCurrentData, overallString, overallExplanation):
	
	# Allplayers of refTeam
	curContent_refTeam = curEventExcerptPanda['TeamID'] == refTeam
	try:
		pivotedData_refTeam = curEventExcerptPanda[curContent_refTeam].pivot(index = 'eventTimeIndex', columns = 'PlayerID', values = key)
	except:
		print('failed again, pdb set.')
		print('population = %s' %population)
		print('aggregationOrder = %s' %aggregationOrder)
		print('aggrMeth_popLevel = %s' %aggrMeth_popLevel)
		print('aggrMeth_playerLevel = %s' %aggrMeth_playerLevel)
		print('curEventExcerptPanda = %s' %curEventExcerptPanda)
		print('key = %s' %key)
		print('refTeam = %s' %refTeam)
		print('othTeam = %s' %othTeam)
		print('eventDescrString = %s' %eventDescrString)
		print('exportCurrentData = %s' %exportCurrentData)
		print('overallString = %s' %overallString)
		print('overallExplanation = %s' %overallExplanation)
		pdb.set_trace()
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

		if data.size == 0:
			# if it's empty, export as nans
			exportCurrentData, overallString, overallExplanation = \
			aggregateTemporally(data,exportCurrentData,population,curKeyString,attrLabel[key],eventDescrString, overallString, overallExplanation, aggrMethods = aggrMeth_popLevel,exportNans = True)
		else:
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
	
	exportNans = False
	if 'exportNans' in keyword_parameters:
		exportNans = keyword_parameters['exportNans']

	if not exportNans:
		count = np.count_nonzero(~np.isnan(data), axis = axis)
		avg = np.nanmean(data, axis = axis)
		std = np.nanstd(data, axis = axis)
		sumVal = np.sum(data, axis = axis)
		minVal = np.min(data, axis = axis)
		maxVal = np.max(data, axis = axis)
		# filterwarnings('error')
		# try:
		med = np.nanmedian(data, axis = axis)
		# except:
		# 	print(data)
		# 	print('HERES A PROBLEM AFTER A MEDIAN')
		# 	pdb.set_trace()
		sem = stats.sem(data, axis = axis)
		kur = stats.kurtosis(data, axis = axis)
		ske = stats.skew(data, axis = axis)
	else:
		count = np.nan
		avg = np.nan
		std = np.nan
		sumVal = np.nan
		minVal = np.nan
		maxVal = np.nan
		med = np.nan
		sem = np.nan
		kur = np.nan
		ske = np.nan		

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



def aggregateCurrentEvent(attrDictCols_numeric,attributeDict,interpolatedCurEventExcerptPanda,TeamAstring,TeamBstring,refTeam,othTeam,ref,oth,exportCurrentData,aggrMeth_popLevel,aggrMeth_playerLevel,aggregatePerPlayer,aggregationOrders,overallString,optionalInput,populations):

	if not optionalInput['window'] == []:
		window = optionalInput['window']
	else:
		window = (0,0)

	if not optionalInput['eventDescrString'] == []:
		eventDescrString = optionalInput['eventDescrString']
	else:
		eventDescrString = []

	# if not optionalInput['overallString'] == []:
	# 	overallString = optionalInput['overallString']
	# else:
	# 	overallString = []

	if not optionalInput['overallExplanation'] == []:
		overallExplanation = optionalInput['overallExplanation']
	else:
		overallExplanation = []		

	if not optionalInput['attrLabel'] == []:
		attrLabel = optionalInput['attrLabel']
	else:
		attrLabel = []		

	# # skippable?
	# eventDescrString
	# overallString
	# overallExplanation
	# aggrMeth_playerLevel
	# attrLabel

	# print(attrDictCols_numeric)
	for key in attrDictCols_numeric:
		# print('THIS IS THE key: %s' %key)

		#####################
		## Prepare the data #
		#####################
		targetGroup = []

		if all(attributeDict[key].isnull()):
			# No data at all..
			warn('\nWARNING: No data was found in <%s>.\nEither something went wrong in the analysis, or this feature was missing from the start..\n**********\nWeird. Definitely shouldn\'t happen here. Probably an issue with exporting and/or loading the data.\n' %attrKey)
			# Take it out of attrDictCols_numeric
			if key in attrDictCols_numeric:
				attrDictCols_numeric.remove(key)

			continue

		# Include the current content
		# A silly work-around in case the data is not in np.float
		tmp = interpolatedCurEventExcerptPanda[key].index

		try:
			if type(interpolatedCurEventExcerptPanda[key][tmp[0]]) == float:
				interpolatedCurEventExcerptPanda[key] = interpolatedCurEventExcerptPanda[key].astype(np.float64)
		except:
			warn('\nWARNING: Tried to set the dataType to np.float64, but failed.\nA known exception occurs when the data is empty. This should in turn lead to exporting NaNs below.\n')
			print(window)
			print(interpolatedCurEventExcerptPanda)
			print(interpolatedCurEventExcerptPanda[key])
			print(key)
			print(tmp)
			print(type(interpolatedCurEventExcerptPanda[key]))
			# pdb.set_trace()
			
		curContent = np.isnan(interpolatedCurEventExcerptPanda[key]) == False			
		tmpPlayerID = interpolatedCurEventExcerptPanda['PlayerID'][curContent]
		tmpTeampID = interpolatedCurEventExcerptPanda['TeamID'][curContent]
		tmpPlayersUnique = tmpPlayerID.unique()
		players = np.sort(tmpPlayersUnique.astype(str))		# THIS IS THE SAME, regardless of which key is used. So, only once, export player IDs			
		### players = np.sort(interpolatedCurEventExcerptPanda.loc[curContent,'PlayerID'].unique())		# THIS IS THE SAME, regardless of which key is used. So, only once, export player IDs			

		## Make outcome variable dependent on current event's refTeam
		# NB: attrLabel is already changed earlier.			
		# print('WAS THERE ANY CURRENT CONTENT? %s' %any(curContent))

		if not any(curContent):
			# apparently, there were no values for the current key during the whole event excerpt
			# This could be a team variable that is empty
			uniqueTeams = interpolatedCurEventExcerptPanda['TeamID'].unique()
			uniqueTeams = [st for st in uniqueTeams if type(st) == str]

			safeEnoughToBeAgroupRow = False
			safeEnoughToBeAplayerRow = False
			if len(uniqueTeams) == 1:
				# there was only 1 team, so very plausible that there were missing values at the grouprows for the current variable
				safeEnoughToBeAgroupRow = True				

			nPlA = len(interpolatedCurEventExcerptPanda.loc[interpolatedCurEventExcerptPanda['TeamID'] == TeamAstring,'PlayerID'].unique())
			nPlB = len(interpolatedCurEventExcerptPanda.loc[interpolatedCurEventExcerptPanda['TeamID'] == TeamBstring,'PlayerID'].unique())

			if nPlA < 3 or nPlB < 3:
				# there was at least one team with less than 3 players (therefore, it is impossible to compute the surface)
				safeEnoughToBeAgroupRow = True
			else: # and check some more possibilities
				# check how many frames for each unique player there are
				for iA in interpolatedCurEventExcerptPanda.loc[interpolatedCurEventExcerptPanda['TeamID'] == TeamAstring,'PlayerID'].unique():
					# if there is a player that only had 1 frame and the nPlA is only 4, then it is treated similarly as nPlA < 3
					if sum(interpolatedCurEventExcerptPanda['PlayerID'] == iA) == 1 and nPlA == 4:
						safeEnoughToBeAgroupRow = True
						warn('\nWARNING: Assumed that if there is only 1 occurence for a player, it was impossible to compute some of the team variables. (such as surface etc.)\n')
						break
				# same for B
				if not safeEnoughToBeAgroupRow: # only necessary if it hasn't yet been found
					for iB in interpolatedCurEventExcerptPanda.loc[interpolatedCurEventExcerptPanda['TeamID'] == TeamBstring,'PlayerID'].unique():
						# if there is a player that only had 1 frame and the nPlA is only 4, then it is treated similarly as nPlA < 3
						if sum(interpolatedCurEventExcerptPanda['PlayerID'] == iB) == 1 and nPlB == 4:
							safeEnoughToBeAgroupRow = True
							warn('\nWARNING: Assumed that if there is only 1 occurence for a player, it was impossible to compute some of the team variables. (such as surface etc.)\n')
							break

				if not safeEnoughToBeAgroupRow: # only necessary if it hasn't yet been found
					# check how many unique players exist at each timepoint
					tmp = interpolatedCurEventExcerptPanda.loc[interpolatedCurEventExcerptPanda['TeamID'] == TeamAstring,['Ts','PlayerID']]
					tmp = tmp.pivot(columns ='Ts', values='PlayerID')
					for c in tmp.keys():
						tmp1 = tmp.loc[tmp[c].notnull(),c]

						# if there are never more than 3 different players, the surface measures could not have been computed.
						if len(tmp1.unique()) < 3:
							safeEnoughToBeAgroupRow = True
							warn('\nWARNING: At no point during the excerpt there were 3 or more players of this team. Therefore, the data is missing and it is safe to assume it was a grouprow.')
						break

				if not safeEnoughToBeAgroupRow: # only necessary if it hasn't yet been found
				# check how many unique players exist at each timepoint, same for Team B
					tmp = interpolatedCurEventExcerptPanda.loc[interpolatedCurEventExcerptPanda['TeamID'] == TeamBstring,['Ts','PlayerID']]
					tmp = tmp.pivot(columns ='Ts', values='PlayerID')
					for c in tmp.keys():
						tmp1 = tmp.loc[tmp[c].notnull(),c]

						# if there are never more than 3 different players, the surface measures could not have been computed.
						if len(tmp1.unique()) < 3:
							safeEnoughToBeAgroupRow = True
							warn('\nWARNING: At no point during the excerpt there were 3 or more players of this team. Therefore, the data is missing and it is safe to assume it was a grouprow.')
							break

			if nPlA == 0 and nPlB == 0:
				safeEnoughToBeAgroupRow = False
				safeEnoughToBeAplayerRow = True
			
			if safeEnoughToBeAgroupRow:
				# only one team found, so indeed a team was missing
				targetGroup = 'groupRows'
				if key[-1] == ref:
					# Ref team
					newKey = key[:-1] + '_ref'
					interpolatedCurEventExcerptPanda[newKey] = interpolatedCurEventExcerptPanda[key]
					interpolatedCurEventExcerptPanda.drop(key, axis=1, inplace=True)
					key = newKey

				elif key[-1] == oth:
					# Other
					newKey = key[:-1] + '_oth'
					interpolatedCurEventExcerptPanda[newKey] = interpolatedCurEventExcerptPanda[key]
					interpolatedCurEventExcerptPanda.drop(key, axis=1, inplace=True)
					key = newKey

			if safeEnoughToBeAgroupRow or safeEnoughToBeAplayerRow:
				try:
					curLabelKey = attrLabel[key]
				except:
					print('\nWARNING: Could not find a label for the current attribute <%s>.\nMost likeley problem:' %key)
					print('\nFORMAT USER INPUT ERROR: The number of columns to be read as attributes does not correspond to the number of labels given.\nMake sure that for each column that is read from the data there is a label.\nAs a working solution, the attributes will be given <no_label> as a label.\n********* PLEASE UPDATE USER INPUT ********\n*******************************************')
					warn('')
					curLabelKey = 'NO_LABEL'

				# Temporal aggregation (across ALL player vals and/or team vals and/or ball vals)
				data = pd.DataFrame([])
				exportCurrentData, overallString, overallExplanation = \
				aggregateTemporally(data,exportCurrentData,targetGroup,key,curLabelKey,eventDescrString, overallString, overallExplanation,aggrMethods = aggrMeth_popLevel, exportNans = True)


				#### continue writing this statement
				warn('\nWARNING: Encountered an event where there was only data available for 1 team.\nAs there was no content for the current key <%s>, it must have been a team feature (IF IT WAS NOT, THEN THIS NEEDS TO BE LOOKED AT).\nThe analysis continues by aggregating this feature as a NaN.\nConsider excluding this event listwise.\n' %key)
				continue
			else:
				# probably go back  to the original debugging output?
				warn('\nWARNING: Although there were no values for this key <%s>, it was not likely to be a team feature, because both teamstrings existed in the excerpt.\nIt may be that the teamstring existed but the values where all nans.\nIf this occurs, you could go back to the solution written above this warning.\nAs a safety measure, this will at the moment lead to an error.\n')
				print('CHECK WHY THE SERIES OF THE CURRENT KEY HAD NO DATA. THERE SHOULD BE A CSV FILE IN YOUR PIPELINE FOLDER WITH THE CURRENT EVENT.')

		if all(tmpPlayerID == 'groupRow'):
			targetGroup = 'groupRows'
			if key[-1] == ref:
				# Ref team
				newKey = key[:-1] + '_ref'
				interpolatedCurEventExcerptPanda[newKey] = interpolatedCurEventExcerptPanda[key]
				interpolatedCurEventExcerptPanda.drop(key, axis=1, inplace=True)
				key = newKey

			elif key[-1] == oth:
				# Other
				newKey = key[:-1] + '_oth'
				interpolatedCurEventExcerptPanda[newKey] = interpolatedCurEventExcerptPanda[key]
				interpolatedCurEventExcerptPanda.drop(key, axis=1, inplace=True)
				key = newKey
			else:
				# No reference assigend
				doNothing = [] # right?

		if all(tmpPlayerID == 'ball'): 
			if targetGroup == []:				
				targetGroup = 'ballRows'
			else:
				interpolatedCurEventExcerptPanda.to_csv('interpolatedCurEventExcerptPanda_complete.csv')
				interpolatedCurEventExcerptPanda.loc[curContent].to_csv('interpolatedCurEventExcerptPanda_curContent.csv')
				rawDict.loc[rowswithinrange].to_csv('rawDict_rowswithinrange.csv')
				rawDict.to_csv('rawDict_complete.csv')
				attributeDict.loc[rowswithinrange].to_csv('attributeDict_rowswithinrange.csv')
				attributeDict.to_csv('attributeDict_complete.csv')
				attributeDict.loc[attributeDict['PlayerID'] == 'groupRow'].to_csv('attributeDict_allGropuRows.csv')
				print('Apparently, targetGroup has already been allocated as a grouprow:' )
				print('targetGroup = %s' %targetGroup)
				print('----')
				print('Previously - when this error occurred, tmpPlayerID was empty:')
				print('tmpPlayerID = %s' %tmpPlayerID)
				print('Lets see if there was a curContent:')
				print('curContent = %s' %any(curContent))
				print('How about a teamID:')
				print('tmpTeampID = %s' %tmpTeampID)
				print('and what about players:')
				print('players = %s' %players)
				print('This is the key: <%s>' %key)
				warn('\nFATAL ERROR: A variable seemed to be covering multiple sets (groupRows and ballRows).\nThis should be avoided or accounted for in the code.\n')
				print('possibly a result of not having any rows within range:')
				print('window[0] = %s' %window[0])
				print('window[1] = %s' %window[1])
				print('min(rawDict[\'Ts\']) = %s' %min(rawDict['Ts']))
				print('max(rawDict[\'Ts\']) = %s' %max(rawDict['Ts']))
				print('rowswithinrange = %s' %rowswithinrange)
				print('number of players of team A within the excerpt nPlA = %s' %nPlA)
				print('number of players of team B within the excerpt nPlB = %s' %nPlB)
				pdb.set_trace()
				exit()
				# To avoid errors: either separate variables covering multiple sets, or add code here that joins targetGroups..
		
		if (all(tmpPlayerID != 'groupRow') and all(tmpPlayerID != 'ball')) and all(tmpTeampID != ''): # i.e., not group, nor ball, but with values in TeamID
			if targetGroup == []:
				targetGroup = 'playerRows'
			else:
				warn('\FATAL ERROR: A variable seemed to be covering multiple sets (playerRows and [groupRows and/or ballRows]).\nThis should be avoided or accounted for in the code.\n')
				exit()
				# To avoid errors: either separate variables covering multiple sets, or add code here that joins targetGroups..

		if targetGroup == []:
			if key == 'Acceleration' or key == 'Speed':
				warn('\nWARN: A known exception of a variable covering multiple sets. Some datasets also give the acceleration and speed of the ball.\nTo avoid conflicts, these input values will be ignored here at the level of temporal aggregation.\nIf you are in fact interested in Acceleration or Speed of the ball, then create a new feature that refers to the ball specifically (ballSpeed and ballAcceleration).\n')
				curContent = (np.isnan(interpolatedCurEventExcerptPanda[key]) == False) & (interpolatedCurEventExcerptPanda['PlayerID'] != 'ball')
				# curContent = interpolatedCurEventExcerptPanda.loc[curContent,'PlayerID'] != 'ball'
				targetGroup = 'playerRows'

			else:
				warn('\nFATAL WARNING: Somehow, targetGroup was still empty. My best guess is that it has to be a playerRow, but this is JUST GUESSING.\n!!!!!!!!!!!!!!!! NEED TO FIX THIS !!!!!!!!!!!!!!!!!!!!!!!!!!\nKey = <%s>' %key)
				targetGroup = 'playerRows'
				rawDict.to_csv('rawDict_complete_noTargetGroup.csv')
				attributeDict.to_csv('attributeDict_complete_noTargetGroup.csv')

		# Temporal aggregation (across ALL player vals and/or team vals and/or ball vals)
		data = interpolatedCurEventExcerptPanda[key][curContent]
		try:
			curLabelKey = attrLabel[key]
		except:
			print('\nWARNING: Could not find a label for the current attribute <%s>.\nMost likeley problem:' %key)
			print('\nFORMAT USER INPUT ERROR: The number of columns to be read as attributes does not correspond to the number of labels given.\nMake sure that for each column that is read from the data there is a label.\nAs a working solution, the attributes will be given <no_label> as a label.\n********* PLEASE UPDATE USER INPUT ********\n*******************************************')
			warn('')
			curLabelKey = 'NO_LABEL'
		# print('\nBefore aggregating all players and/or gropuRows:')
		# print('len(exportCurrentData) = %s' %len(exportCurrentData))
		# # print('len(overallString) = %s' %len(overallString))
		# # print('len(overallExplanation) = %s' %len(overallExplanation))

		exportCurrentData, overallString, overallExplanation = \
		aggregateTemporally(data,exportCurrentData,targetGroup,key,curLabelKey,eventDescrString, overallString, overallExplanation,aggrMethods = aggrMeth_popLevel)
		# print('targetGroup = %s' %targetGroup)
		# Finally, aggregate player level values per team and/or per time separately
		if targetGroup == 'playerRows':
			# When it's a playerRow, the outcome measure is also aggregated per player per team separately.
			# print('\nBefore aggregating playerRows:')
			# print('len(exportCurrentData) = %s' %len(exportCurrentData))
			# print('len(overallString) = %s' %len(overallString))
			# print('len(overallExplanation) = %s' %len(overallExplanation))

			for i in np.arange(len(aggregationOrders)):
				aggregationOrder = aggregationOrders[i]
				population = populations[i]

				exportCurrentData, overallString, overallExplanation = \
				aggregateTemporallyINCEPTION(population,aggregationOrder,aggrMeth_popLevel,aggrMeth_playerLevel,interpolatedCurEventExcerptPanda,key,refTeam,othTeam,attrLabel,eventDescrString, exportCurrentData, overallString, overallExplanation)
			
			# print('\nBefore aggregate per player:')
			# print('len(exportCurrentData) = %s' %len(exportCurrentData))
			# print('len(overallString) = %s' %len(overallString))
			# print('len(overallExplanation) = %s' %len(overallExplanation))

			if key in aggregatePerPlayer:
				
				# Create the attrlabel for 22 players (that may remain empty)
				for i in np.arange(0,22):

					newKey = '%s_%02d' %(key,i+1)
					attrLabel[newKey] = attrLabel[key]# + 'Of the %sth player (see PlayerID_XX).' %i+1

					if len(players) >= i + 1:
						curPlayerData = interpolatedCurEventExcerptPanda.loc[interpolatedCurEventExcerptPanda['PlayerID'] == players[i],key]
						curPlayerData = curPlayerData[np.isnan(curPlayerData) == False]

						data = curPlayerData
						targetGroup = '%sth player' %(i+1)

						# there are still players, so continue to put players in the aggreagte
						exportCurrentData, overallString, overallExplanation = \
						aggregateTemporally(data,exportCurrentData,targetGroup,newKey,attrLabel[newKey],eventDescrString, overallString, overallExplanation,aggrMethods = aggrMeth_popLevel)

					else:
						# Less than 22 players, for the remaining players, export NaN
						data = pd.DataFrame([])
						exportCurrentData, overallString, overallExplanation = \
						aggregateTemporally(data,exportCurrentData,targetGroup,newKey,attrLabel[newKey],eventDescrString, overallString, overallExplanation,aggrMethods = aggrMeth_popLevel,exportNans = True)
		# # here
		# print('\nAt the end:')
		# print('len(exportCurrentData) = %s' %len(exportCurrentData))
		# # print('len(overallString) = %s' %len(overallString))
		# print('len(overallExplanation) = %s' %len(overallExplanation))

		# if len(exportCurrentData) > 165:
		# 	pdb.set_trace()
	return exportCurrentData, overallString, overallExplanation 