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
import countEvents2
from scipy import stats

if __name__ == '__main__':

	process(targetEvents,aggregateLevel,rawDict,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring)
	# Aggregates a range of pre-defined features over a specific window:
	specific(rowswithinrange,aggregateString,rawDict,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring)	

def process(targetEvents,aggregateLevel,rawDict,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring,debuggingMode,tobeDeletedWithWarning,skipEventAgg_curFile,fileIdentifiers,attrLabel):
	tTempAgg = time.time()
	FileID = "_".join(fileIdentifiers)

	if aggregateLevel[0] == 'None':
		warn('\nWARNING: No temporal aggregate level indicated. \nNo temporally aggregated data exported.\nChange aggregateEvent = <%s> in USER INPUT.\n' %aggregateLevel[0])
		return exportData,exportDataString,exportFullExplanation,eventExcerptPanda

	elif len(targetEvents[aggregateLevel[0]]) == 0:
		warn('\nWARNING: No targetevents detected. \nCouldnt aggregate temporally. \nNo Data exported.\n')
		return exportData,exportDataString,exportFullExplanation,eventExcerptPanda

	# The export matrix includes a range of outcome measures that don't change per event (these are already in <exportData>)
	# Most outcome variables will be added by simply going through all the spatially aggregated variables
	# Some other outcome measures describe the current event and can also be created here:
	exportMatrix = []
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

	eventExcerptPanda = pd.DataFrame()
	# To which UID and refTeam should be added

	if type(targetEvents[aggregateLevel[0]]) == tuple:
		warn('\nWARNING: Make sure the format of the event fits in a list. \nIf not, the script will fail when there is only 1 event.\n')

	for idx,currentEvent in enumerate(targetEvents[aggregateLevel[0]]):
		
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
		
		exportCurrentData = exportData.copy()
		overallString = exportDataString.copy()
		overallExplanation = exportFullExplanation.copy()
		
		# Create the output string that identifies the current event
		aggregateString = '%s_%03d' %(aggregateLevel[0],idx)		
		if idx > 1000:
			aggregateString = '%s_%d' %(aggregateLevel[0],idx)
			warn('\nWARNING: More than 1000 occurrences of the same event.\nForced to change format for finding aggregateString.\nConsider pre-allocating the number of digits included by default.')
		
		exportCurrentData.append(aggregateString) # NB: String and explanation are defined before the for loop
		
		# Create unque event identifier
		EventUID = FileID + aggregateString
		exportCurrentData.append(EventUID) # NB: String and explanation are defined before the for loop

		# Assign refTeam
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

		#########################################
		#########################################
		#########################################

		# All data in exportCurrentData are trial identifiers. Create a column in a new panda copying these:
		currentEventID = pd.DataFrame([],columns = [exportDataString],index = [rawDict['Ts'][rowswithinrange].index])
		for i,val in enumerate(exportCurrentData):
			currentEventID[exportDataString[i]] = val
		
		if skipEventAgg_curFile:
			warn('\nContinued with previously aggregated event data.\nIf you want to add new (or revised) spatial aggregates, change <skipEventAgg> into <False>.\n')
			break	

		# Create a new panda that has the identifiers of the current event and the rawdata
		curEventExcerptPanda = pd.concat([currentEventID, rawDict.loc[rowswithinrange], attributeDict.loc[rowswithinrange, attrDictCols]], axis=1) # Skip the duplicate 'Ts' columns

		# Create an index that restarts per event
		times = curEventExcerptPanda['Ts'].unique()
		timesSorted = np.sort(times)
		newColumn = pd.DataFrame([], columns = ['eventTimeIndex', 'eventTime'])
		for i,val in np.ndenumerate(timesSorted):
			oldIndex = curEventExcerptPanda[curEventExcerptPanda['Ts'] == val].index
			eventTime = val - max(times)
			# It seems that the index can actually be negative.
			# If problematic, change the '0' below to e.g. '1000'
			eventTimeIndex = 0 - len(times) + i[0] + 1

			tmp = pd.DataFrame([], columns = ['eventTimeIndex','eventTime'], index = oldIndex)
			tmp['eventTimeIndex'] = eventTimeIndex
			tmp['eventTime'] = eventTime
			newColumn = newColumn.append(tmp)

		# Add the new index
		curEventExcerptPanda = pd.concat([newColumn,curEventExcerptPanda], axis=1) # Skip the duplicate 'Ts' columns	


		# # !!!!!!!!!!!!!!!!!!!!!!!!!!!!! remember this for re_indexing
		# Use the new index as the index
		# curEventExcerptPanda = curEventExcerptPanda.set_index('eventTimeIndex', drop=True, append=False, inplace=False, verify_integrity=False)
		# # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!





		# Find the rows for each type of outcome measure
		# groupRows = curEventExcerptPanda['PlayerID'] == 'groupRow'
		# playerRows = curEventExcerptPanda['TeamID'] != ''.index
		# playerRows_ref = curEventExcerptPanda['TeamID'][playerRows] == refTeam
		# playerRows_oth = curEventExcerptPanda['TeamID'][playerRows] != refTeam
		# # For ball vars, take the ball rows only
		# # currently, there are no ball variables, but if you'd want them in the future, then change it here:
		# ballRows = curEventExcerptPanda['TeamID'] == 'ball'.index
		# PROBLEM
		# With new index, can't distinghuish between different sets by using index...
		# SOLUTION
		# Multi index?

		# curEventExcerptPanda.to_csv('C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\NP repository\\test.csv')
		# key = 'TeamCentXA'
		# key = 'distToCent'
		# # print(np.isnan(curEventExcerptPanda[key][0]))
		# # print(curEventExcerptPanda['PlayerID'][curContent])
		# curContent = np.isnan(curEventExcerptPanda[key]) == False
		# print('is group var:')
		# tmpPlayerID = curEventExcerptPanda['PlayerID'][curContent]
		# print(all(tmpPlayerID == 'groupRow'))
		# print('is ball var:')
		# print(all(tmpPlayerID == 'ball'))

		# # Foutje met team var herkennen???? (fals positive for player var)
		# tmpTeampID = curEventExcerptPanda['TeamID'][curContent]
		# print('is player var:')
		# print((all(tmpPlayerID != 'groupRow') and all(tmpPlayerID != 'ball')) and all(tmpTeampID != ''))
		# pdb.set_trace()

		# pdb.set_trace()
		
		# For all team level variables, either:
		# 1) change the names coressponding to the refTeam
		# or 2) keep both categorizations.
		# Let's go with option 1 for now.
		for key in attrDictCols_numeric:
			key = 'distToCent'
			print(key)
			curContent = np.isnan(curEventExcerptPanda[key]) == False
			
			tmpPlayerID = curEventExcerptPanda['PlayerID'][curContent]
			tmpTeampID = curEventExcerptPanda['TeamID'][curContent]
			print('---')
			targetGroup = []
			if all(tmpPlayerID == 'groupRow'):
				targetGroup = 'groupRows'
				if key[-1] == ref:
					# Ref team
					newKey = key[:-2] + '_ref'
					curEventExcerptPanda[newKey] = curEventExcerptPanda[key]
					curEventExcerptPanda.drop(key, axis=1, inplace=True)
					attrLabel[newKey] = attrLabel[key]
					del attrLabel[key]
					key = newKey
				elif key[-1] == oth:
					# Other
					newKey = key[:-2] + '_oth'
					curEventExcerptPanda[newKey] = curEventExcerptPanda[key]
					curEventExcerptPanda.drop(key, axis=1, inplace=True)
					attrLabel[newKey] = attrLabel[key]
					attrLabel.drop(key, axis=1, inplace=True)
					attrLabel[newKey] = attrLabel[key]
					del attrLabel[key]
					key = newKey
				else:
					# No reference assigend
					doNothing = [] # right?

			if all(tmpPlayerID == 'ball'): 
				if targetGroup == []:				
					targetGroup = 'ballRows'
				else:
					exit('\nFATAL ERROR: A variable seemed to be covering multiple sets (groupRows and ballRows).\nThis should be avoided or accounted for in the code.\n')
					# To avoid errors: either separate variables covering multipel sets, or add code here that joins targetGroups..
			
			if (all(tmpPlayerID != 'groupRow') and all(tmpPlayerID != 'ball')) and all(tmpTeampID != ''): # i.e., not group, nor ball, but with values in TeamID
				if targetGroup == []:
					targetGroup = 'playerRows'
				else:
					exit('\FATAL ERROR: A variable seemed to be covering multiple sets (playerRows and [groupRows and/or ballRows]).\nThis should be avoided or accounted for in the code.\n')
					# To avoid errors: either separate variables covering multipel sets, or add code here that joins targetGroups..
			
			# exportCurrentData,overallString,overallExplanation
			data = curEventExcerptPanda[key][curContent]
			exportCurrentData, overallString, overallExplanation = \
			aggregateTemporally(data,exportCurrentData,targetGroup,key,attrLabel[key],eventDescrString, overallString, overallExplanation)
	
			# if per player, also stats per per team...
			# need to add string to the key.
			# where <data> is averaged (etc.) per player per team.
			if targetGroup == 'playerRows':
				# When it's a playerRow, the outcome measure is also aggregated per player per team separately.
				# for allAggregationMethods:

				## Method 1
				method = 'perTimePerPlayer' # method 1, axis = 1
				population = 'allTeam'
				exportCurrentData, overallString, overallExplanation = \
				aggregateTemporallyINCEPTION(population,method,curEventExcerptPanda,key,refTeam,othTeam,attrLabel,eventDescrString, exportCurrentData, overallString, overallExplanation)
				
				population = 'refTeam'
				exportCurrentData, overallString, overallExplanation = \
				aggregateTemporallyINCEPTION(population,method,curEventExcerptPanda,key,refTeam,othTeam,attrLabel,eventDescrString, exportCurrentData, overallString, overallExplanation)
				
				population = 'othTeam'
				exportCurrentData, overallString, overallExplanation = \
				aggregateTemporallyINCEPTION(population,method,curEventExcerptPanda,key,refTeam,othTeam,attrLabel,eventDescrString, exportCurrentData, overallString, overallExplanation)
				
				## Method 2
				method = 'perPlayerPerTime' # method 2, axis = 0
				population = 'allTeam'
				exportCurrentData, overallString, overallExplanation = \
				aggregateTemporallyINCEPTION(population,method,curEventExcerptPanda,key,refTeam,othTeam,attrLabel,eventDescrString, exportCurrentData, overallString, overallExplanation)
				
				population = 'refTeam'
				exportCurrentData, overallString, overallExplanation = \
				aggregateTemporallyINCEPTION(population,method,curEventExcerptPanda,key,refTeam,othTeam,attrLabel,eventDescrString, exportCurrentData, overallString, overallExplanation)

				population = 'othTeam'
				exportCurrentData, overallString, overallExplanation = \
				aggregateTemporallyINCEPTION(population,method,curEventExcerptPanda,key,refTeam,othTeam,attrLabel,eventDescrString, exportCurrentData, overallString, overallExplanation)
				
				print(exportCurrentData)
				print('\n---\n')
				print(overallString)
				print('\n---\n')
				print(overallExplanation)
				print('\n---\n')
				pdb.set_trace()
				
				# Also export new keys?? Change Attr labels enzo?

				# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				# This is where you left it.
				# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				



				
















				######### put this in a function ############
				# All players
				# NB NOT IN USE YET
				pivotedData = curEventExcerptPanda.pivot(index = 'eventTimeIndex', columns = 'PlayerID', values = key)

				# Allplayers of refTeam
				curContent_refTeam = curEventExcerptPanda['TeamID'] == refTeam
				pivotedData_refTeam = curEventExcerptPanda[curContent_refTeam].pivot(index = 'eventTimeIndex', columns = 'PlayerID', values = key)
				# Allplayers of othTeam
				curContent_othTeam = curEventExcerptPanda['TeamID'] == othTeam
				pivotedData_othTeam = curEventExcerptPanda[curContent_othTeam].pivot(index = 'eventTimeIndex', columns = 'PlayerID', values = key)
				
				# two ways:
				# 1) average per time per player (of a team)
				# 2) average per player (of a team) per time

				# Function should be run for:
				# allTeam, method 1
				# allTeam, method 2
				# refTeam, method 1
				# refTeam, method 2
				# othTeam, method 1
				# othTeam, method 2


				# Method 1
				# def aggregateTemporallyINCEPTION(data,method,):
				
				countRefTeam_perTime, avgRefTeam_perTime, stdRefTeam_perTime, sumValRefTeam_perTime, minValRefTeam_perTime, maxValRefTeam_perTime, medRefTeam_perTime, semRefTeam_perTime, kurRefTeam_perTime, skeRefTeam_perTime = \
				aggregateTemporally(pivotedData_refTeam,[], axis = 1) # axis = 0 ==> per player; axis = 1 ==> per time

				# # This one can be omitted..
				# countRefTeam_perTimePerPlayer, avgRefTeam_perTimePerPlayer, stdRefTeam_perTimePerPlayer, sumValRefTeam_perTimePerPlayer, minValRefTeam_perTimePerPlayer, maxValRefTeam_perTimePerPlayer, medRefTeam_perTimePerPlayer, semRefTeam_perTimePerPlayer, kurRefTeam_perTimePerPlayer, skeRefTeam_perTimePerPlayer = \
				# aggregateTemporally(avgRefTeam_perTime,[], axis = 0) # axis = 0 ==> per player; axis = 1 ==> per time

				# use the full functionality of aggregatTemporrally
				data = avgRefTeam_perTime # and repeat for all others that you're interested in.
				# depends on method
				curKeyString = key + '_avgPerTimePerPlayer' # change this avg

				# depends on refTeam
				curTargetGroup = 'refTeam' # change this othTeam

				# 'Avg of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, curTargetGroup, eventDescrString)
				# average of distance_to_ball (distance to ball in meters). For <playerRows>. During the last 10 seconds up until 0 seconds before a goal.
				
				## make this a subfunction
				# It should be run for all:
				# count, avg, std, sumVal, minVal, maxVal, med, sem, kur, ske
				exportCurrentData, overallString, overallExplanation = \
				aggregateTemporally(data,exportCurrentData,curTargetGroup,curKeyString,attrLabel[key],eventDescrString, overallString, overallExplanation)
				print(exportCurrentData)
				print('\n---\n')
				print(overallString)
				print('\n---\n')
				print(overallExplanation)
				print('\n---\n')
				print(data)
				pdb.set_trace()
				######### put this in a function ############

				# Method 2
				countRefTeam_perPlayer, avgRefTeam_perPlayer, stdRefTeam_perPlayer, sumValRefTeam_perPlayer, minValRefTeam_perPlayer, maxValRefTeam_perPlayer, medRefTeam_perPlayer, semRefTeam_perPlayer, kurRefTeam_perPlayer, skeRefTeam_perPlayer = \
				aggregateTemporally(pivotedData_refTeam,[], axis = 0) # axis = 0 ==> per player; axis = 1 ==> per time

				countRefTeam_perPlayerPerTime, avgRefTeam_perPlayerPerTime, stdRefTeam_perPlayerPerTime, sumValRefTeam_perPlayerPerTime, minValRefTeam_perPlayerPerTime, maxValRefTeam_perPlayerPerTime, medRefTeam_perPlayerPerTime, semRefTeam_perPlayerPerTime, kurRefTeam_perPlayerPerTime, skeRefTeam_perPlayerPerTime = \
				aggregateTemporally(avgRefTeam_perPlayer,[], axis = 0) # axis = 0 ==> per player; axis = 1 ==> per time
				# and then the same for all other outcome measures.

				# and then the same for othTeam
				pdb.set_trace()
				# tmp_player = pd.Series(tmp_player)

				# tmp_time = pd.Series(tmp_time)
				print(tmp_player)
				print(type(tmp_player))
				tmp_player_time = aggregateTemporally(tmp_player,[], axis = 0)  
				tmp_time_player = aggregateTemporally(tmp_time,[], axis = 0) 

				print(tmp_player_time)
				print(tmp_time_player)

				# print(np.nanmean(pivotedData_refTeam, axis = 1)) # axis = 1 ==> per time
				pdb.set_trace()

				# # data = curEventExcerptPanda.pivot(index='PlayerID', columns=key)

				# data.to_csv('C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\NP repository\\test.csv')
				# pivotedData.to_csv('C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\NP repository\\testPivoted.csv')
				# # print(data)


				pdb.set_trace()
				curContent_othTeam = curEventExcerptPanda['TeamID'][curContent] == othTeam
				data = curEventExcerptPanda[key][curContent_othTeam]

				print(data)
				print(np.mean(data,axis = 0))
				# print(np.mean(data,axis = 1))
				pdb.set_trace()
				aggregatePerTime_ref = np.mean(data,axis = 0)# also per team
				aggregatePerTime_oth # also per team
				
				aggregatePerTimePerPlayer_ref
				aggregatePerTimePerPlayer_oth

				# for every timestep
				# method 2
				aggregatePerPlayer_ref # also per team					
				aggregatePerPlayer_oth # also per team

				aggregatePerPlayerPerTime_ref
				aggregatePerPlayerPerTime_oth


				data_aggregatedPerPlayerOfRefTeam = aggregateTemporallyINCEPTION()

				exportCurrentData, overallString, overallExplanation = \
				aggregateTemporally(data,targetGroup,curKeyString,attrLabel[key],eventDescrString,exportCurrentData, overallString, overallExplanation)

				# for every player of not refTeam
				# 	data_aggregatedPerPlayerOfOtherTeam = aggregateTemporallyINCEPTION()

				exportCurrentData, overallString, overallExplanation = \
				aggregateTemporally(data,targetGroup,curKeyString,attrLabel[key],eventDescrString,exportCurrentData, overallString, overallExplanation)

				print(exportCurrentData)
				print(overallString)
				print(overallExplanation)
				pdb.set_trace()

			
			continue
			pdb.set_trace()


			pdb.set_trace()
			print(curEventExcerptPanda[key][groupRows])
			# For team vars, take the team rows only, AND SWAP PER REFTEAM
			if any(curEventExcerptPanda[key][groupRows] != None):
				print('not equal to none')
			if not all(curEventExcerptPanda[key][groupRows] == None):
				print('not equal to none method 2')
				pdb.set_trace()
				if key[-1] == ref:
					# Ref team
					newKey = key[-1] + '_ref'
					curEventExcerptPanda[newKey] = curEventExcerptPanda[key]
					curEventExcerptPanda.drop(key, axis=1, inplace=True)

				elif key[-1] == oth:
					# Other
					newKey = key[-1] + '_oth'
					curEventExcerptPanda[newKey] = curEventExcerptPanda[key]
					curEventExcerptPanda.drop(key, axis=1, inplace=True)
				else:
					# No reference assigend
					doNothing = [] # right?

				curIdx = groupRows

			if not all(curEventExcerptPanda[key][ballRows] == None):
			# For ball vars, take the ball rows only
				if curIdx == []:
					curIdx = ballRows
				else:
					warn('\nWARNING: A variable seemed to be covering multiple sets (groupRows and ballRows).\nThis should be avoided and may lead to errors.\n')

			# if not all(curEventExcerptPanda[key][playerRows] == None):
			# # For ball vars, take the ball rows only
			# 	if curIdx == []:
			# 		curIdx = playerRows				else:
			# 		warn('\nWARNING: A variable seemed to be covering multiple sets (groupRows and ballRows).\nThis should be avoided and may lead to errors.\n')

			# 	else:
			# 		warn('\nWARNING: A variable seemed to be covering multiple sets (playerRows and [groupRows and/or ballRows]).\nThis should be avoided and may lead to errors.\n')



			# For player vars, take the player value rows PER PLAYER
			
			# For team vars, take the team rows only, AND SWAP PER REFTEAM


			np.sum(curEventExcerptPanda[key][curIdx])
			np.min(curEventExcerptPanda[key][curIdx])
			np.max(curEventExcerptPanda[key][curIdx])
			
			np.nanmean(curEventExcerptPanda[key][curIdx])
			np.nanstd(curEventExcerptPanda[key][curIdx])
			np.nanvar(curEventExcerptPanda[key][curIdx])
			np.median(curEventExcerptPanda[key][curIdx])
			stats.sem(curEventExcerptPanda[key][curIdx])
			stats.kurtosis(curEventExcerptPanda[key][curIdx])
			stats.skew(curEventExcerptPanda[key][curIdx])

		eventExcerptPanda = eventExcerptPanda.append(curEventExcerptPanda)


		
		## Count exsiting events (goals, possession and passes)
		exportCurrentData,overallString,overallExplanation =\
		countEvents2.goals(window,aggregateLevel[0],targetEvents,TeamAstring,TeamBstring,exportCurrentData,overallString,overallExplanation)
		
		exportCurrentData,overallString,overallExplanation =\
		countEvents2.turnovers(window,aggregateLevel[0],targetEvents,TeamAstring,TeamBstring,exportCurrentData,overallString,overallExplanation)

		exportCurrentData,overallString,overallExplanation =\
		countEvents2.possessions(window,aggregateLevel[0],targetEvents,TeamAstring,TeamBstring,exportCurrentData,overallString,overallExplanation)
	
		exportCurrentData,overallString,overallExplanation =\
		countEvents2.passes(window,aggregateLevel[0],targetEvents,TeamAstring,TeamBstring,exportCurrentData,overallString,overallExplanation)


		## Aggregate timeseries variables for the given time-period
		# ## By default, it uses (where easily possible) mean (avg), standard deviation (std), minimum (min), maximum (max), sum
		# # Ideas:
		# # - something to think about: the aggregation method (avg, std, min, max, sum) should also be data-driven. Perhaps it needs to be placed outside of this script?
		# tmp,overallString,overallExplanation = \
		# specific(rowswithinrange,aggregateLevel[0],rawDict,attributeDict,exportCurrentData,overallString,overallExplanation,TeamAstring,TeamBstring,tobeDeletedWithWarning)



		# for key in attrDictCols_numeric:
		# 	if any(curEventExcerptPanda[key][groupRows] != None):
		# 		print('not equal to none')
		# 	if not all(curEventExcerptPanda[key][groupRows] == None):
		# 		print('not equal to none method 2')

		# 	# For team vars, take the team rows only, AND SWAP PER REFTEAM
		# 	if any(curEventExcerptPanda[key][groupRows] != None):
		# 		if key[-1] == 'A':

		# 	# For ball vars, take the ball rows only

		# 	# For player vars, take the player value rows PER PLAYER
			
		# 	# For team vars, take the team rows only, AND SWAP PER REFTEAM
		# 	groupRows = curEventExcerptPanda['PlayerID'] == 'groupRow'.index

		# 	# playerRows = curEventExcerptPanda['TeamID'] == 'groupRow'.index
		# 	playerRows = curEventExcerptPanda['TeamID'] != ''.index
		# 	playerRows_ref = curEventExcerptPanda['TeamID'][playerRows] == refTeam
		# 	playerRows_oth = curEventExcerptPanda['TeamID'][playerRows] != refTeam


			
		# 	refTeam

		# 	rowswithinrangeTeam = rawDict['Ts'][rowswithinrange].index[rawDict['PlayerID'][rowswithinrange] == 'groupRow']
		# 	rowswithinrangeBall = rawDict['Ts'][rowswithinrange].index[rawDict['PlayerID'][rowswithinrange] == 'ball']
		# 	rowswithinrangePlayer = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'][rowswithinrange] != '']
		# 	rowswithinrangePlayerA = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'][rowswithinrange] == TeamAstring]

		# 	groupRows = curEventExcerptPanda['PlayerID'][curEventExcerptPanda[key] != None] == 'groupRow'.index

		# 	np.sum(curEventExcerptPanda[key])
		# 	np.min(curEventExcerptPanda[key])
		# 	np.max(curEventExcerptPanda[key])
			
		# 	np.nanmean(curEventExcerptPanda[key])
		# 	np.nanstd(curEventExcerptPanda[key])
		# 	np.nanvar(curEventExcerptPanda[key])
		# 	np.median(curEventExcerptPanda[key])
		# 	stats.sem(curEventExcerptPanda[key])
		# 	stats.kurtosis(curEventExcerptPanda[key])
		# 	stats.skew(curEventExcerptPanda[key])
			
		# 	pdb.set_trace()
		# pdb.set_trace()

		# key in label per event

		tmp,overallString,overallExplanation = \
		generic(rowswithinrange,aggregateLevel[0],rawDict,attributeDict,exportCurrentData,overallString,overallExplanation,TeamAstring,TeamBstring,tobeDeletedWithWarning)
		curEventExcerptPanda

		exportMatrix.append(tmp)

		# Create / append / exporteer een dataframe met alle windowsinrange (evt met huidige index, maar die moet later gereset)
		# NB: use eventExcerptPanda (which already has the trial characteristics)


	if debuggingMode:
		elapsed = str(round(time.time() - tTempAgg, 2))
		print('Time elapsed during temporalAggregation: %ss' %elapsed)
	return exportMatrix,overallString,overallExplanation,eventExcerptPanda

	# Per event
def generic(rowswithinrange,aggregateString,rawData,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring,tobeDeletedWithWarning):

	# Add a row for attacking as A or B
	for key in attributeDict.keys():

		# Both teams

		# Attacking Team

		# Defending Team


		doSometing = []
		pdb.set_trace()

	return 	exportData,exportDataString,exportFullExplanation

# def aggregateTemporally(data,targetGroup,curKeyString,attrLabel,eventDescrString,exportCurrentData, overallString, overallExplanation):
# 	curAggMethodd = '_avg'
# 	exportCurrentData.append(np.nanmean(data))
# 	overallString.append(curKeyString + curAggMethodd)				
# 	overallExplanation.append('Average of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))

# 	curAggMethodd = '_std'
# 	exportCurrentData.append(np.nanstd(data))
# 	overallString.append(curKeyString + curAggMethodd)
# 	overallExplanation.append('Standard Deviation of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))

# 	curAggMethodd = '_sum'
# 	exportCurrentData.append(np.sum(data))
# 	overallString.append(curKeyString + curAggMethodd)
# 	overallExplanation.append('The Sum of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))

# 	curAggMethodd = '_min'
# 	exportCurrentData.append(np.min(data))
# 	overallString.append(curKeyString + curAggMethodd)
# 	overallExplanation.append('Minimum of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))
	
# 	curAggMethodd = '_max'
# 	exportCurrentData.append(np.max(data))
# 	overallString.append(curKeyString + curAggMethodd)
# 	overallExplanation.append('Maximum of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))

# 	curAggMethodd = '_med'
# 	exportCurrentData.append(np.median(data))
# 	overallString.append(curKeyString + curAggMethodd)
# 	overallExplanation.append('The Median of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))
	
# 	curAggMethodd = '_sem'
# 	exportCurrentData.append(stats.sem(data))
# 	overallString.append(curKeyString + curAggMethodd)				
# 	overallExplanation.append('The Standard Error of Measurment (SEM) of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))
	
# 	curAggMethodd = '_kur'
# 	exportCurrentData.append(stats.kurtosis(data))
# 	overallString.append(curKeyString + curAggMethodd)				
# 	overallExplanation.append('Kurtosis of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))
	
# 	curAggMethodd = '_ske'
# 	exportCurrentData.append(stats.skew(data))
# 	overallString.append(curKeyString + curAggMethodd)
# 	overallExplanation.append('Skewness of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))

# 	return exportCurrentData, overallString, overallExplanation

def aggregateTemporally(data,*positional_parameters,**keyword_parameters):

	nAxis = 0
	if 'axis' in keyword_parameters:
		axis = keyword_parameters['axis']
		nAxis = (axis - 1) * -1
	else:
		axis = 0		
	
	# The aggregation process: stick to this order!!
	# print(np.shape(data))
	# # for every not axis, check how many
	# # print(np.shape(data))
	# # [print(i) for i in np.arange()]
	# print('newcount:')
	# print(np.count_nonzero(~np.isnan(data), axis = axis))

	# count = np.shape(data)
	# if len(count) > 1:
	# 	count = [np.shape(data)[axis] for i in np.arange(np.shape(data)[nAxis])]
	# # tmp = data != np.nan
	# # count = np.shape(data)[axis]
	# print('---')
	# print(count)
	# print('---')
	count = np.count_nonzero(~np.isnan(data), axis = axis)
	# if count[0] == 74:
	# 	print(data)
	avg = np.nanmean(data, axis = axis)
	std = np.nanstd(data, axis = axis)
	sumVal = np.sum(data, axis = axis)
	minVal = np.min(data, axis = axis)
	maxVal = np.max(data, axis = axis)
	med = np.median(data, axis = axis)
	sem = stats.sem(data, axis = axis)
	kur = stats.kurtosis(data, axis = axis)
	ske = stats.skew(data, axis = axis)

	# exportCurrentData.append(np.shape(data)[axis])
	# exportCurrentData.append(np.nanmean(data, axis = axis))
	# exportCurrentData.append(np.nanstd(data, axis = axis))
	# exportCurrentData.append(np.array(np.sum(data, axis = axis)))
	# exportCurrentData.append(np.array(np.min(data, axis = axis)))
	# exportCurrentData.append(np.array(np.max(data, axis = axis)))
	# exportCurrentData.append(np.median(data, axis = axis))
	# exportCurrentData.append(stats.sem(data, axis = axis))
	# exportCurrentData.append(stats.kurtosis(data, axis = axis))
	# exportCurrentData.append(stats.skew(data, axis = axis))
	
	if len(positional_parameters) != 7:
		return count, avg, std, sumVal, minVal, maxVal, med, sem, kur, ske
		# print('hiiii')
		# print(data.loc[axis].index)
		# print(np.shape(avg)[0])
		# print(avg)
		# # pdb.set_trace()
		# alternativeExport = pd.DataFrame([count, avg, std, sumVal, minVal, maxVal, med, sem, kur, ske], columns = ['count', 'avg', 'std', 'sumVal', 'minVal', 'maxVal', 'med', 'sem', 'kur', 'ske'], index = data.loc[axis].index)
		# # alternativeExport = pd.DataFrame([np.asarray(count), np.asarray(avg), np.asarray(std), np.asarray(sumVal), np.asarray(minVal), np.asarray(maxVal), np.asarray(med), np.asarray(sem), np.asarray(kur), np.asarray(ske)], columns = ['count', 'avg', 'std', 'sumVal', 'minVal', 'maxVal', 'med', 'sem', 'kur', 'ske'], index = data.loc[axis].index)

		# print(alternativeExport)

		# pdb.set_trace()
		# # return early if only looking for the aggregated values
		# return alternativeExport

	# continue to create the strings in the full version
	exportCurrentData,targetGroup, curKeyString, curLabel, eventDescrString, overallString, overallExplanation = positional_parameters

	exportCurrentData.append(count)
	exportCurrentData.append(avg)
	exportCurrentData.append(std)
	exportCurrentData.append(sumVal)
	exportCurrentData.append(minVal)
	exportCurrentData.append(maxVal)
	exportCurrentData.append(med)
	exportCurrentData.append(sem)
	exportCurrentData.append(kur)
	exportCurrentData.append(ske)

	overallString.append(curKeyString + '_cnt')				
	overallExplanation.append('Number of occurences (i.e., n) of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))

	overallString.append(curKeyString + '_avg')				
	overallExplanation.append('Average of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))

	overallString.append(curKeyString + '_std')
	overallExplanation.append('Standard Deviation of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))

	overallString.append(curKeyString + '_sum')
	overallExplanation.append('The Sum of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))

	overallString.append(curKeyString + '_min')
	overallExplanation.append('Minimum of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))
	
	overallString.append(curKeyString + '_max')
	overallExplanation.append('Maximum of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))

	overallString.append(curKeyString + '_med')
	overallExplanation.append('The Median of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))
	
	overallString.append(curKeyString + '_sem')				
	overallExplanation.append('The Standard Error of Measurment (SEM) of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))
	
	overallString.append(curKeyString + '_kur')				
	overallExplanation.append('Kurtosis of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))
	
	overallString.append(curKeyString + '_ske')
	overallExplanation.append('Skewness of <%s> (%s). For <%s>. %s' %(curKeyString, curLabel, targetGroup, eventDescrString))

	return exportCurrentData, overallString, overallExplanation


def specific(rowswithinrange,aggregateString,rawData,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring,tobeDeletedWithWarning):
	# Per Team and for both teams (for vNorm, the team aggregate -techinically spatial aggregation - still has to be made)
	vel = [val for idx,val in enumerate(attributeDict['vNorm'][rowswithinrange])]
	velA = [val for idx,val in enumerate(attributeDict['vNorm'][rowswithinrange]) if rawData['TeamID'][rowswithinrange[idx]] == TeamAstring]
	velB = [val for idx,val in enumerate(attributeDict['vNorm'][rowswithinrange]) if rawData['TeamID'][rowswithinrange[idx]] == TeamBstring]

	if velA == []:
		warn('\nWARNING: No values found for <%s>\nCould be a result of an incomplete dataset.\nConsider cleaning the data again and/or running the spatial aggregation again.\nOr, you could delete the spatially aggregated file:\n<%s>\n!!!!!!!!!!!!!!!!!!!!\n' %(TeamAstring,tobeDeletedWithWarning))
	if velB == []:
		warn('\nWARNING: No values found for <%s>\nCould be a result of an incomplete dataset.\nConsider cleaning the data again and/or running the spatial aggregation again.\nOr, you could delete the spatially aggregated file:\n<%s>\n!!!!!!!!!!!!!!!!!!!!\n' %(TeamBstring,tobeDeletedWithWarning))
	# pritn(vel)
	# print(velA)
	# print(velB)
	# pdb.set_trace()

	
	# Average
	avgSpeed = np.nanmean(vel)
	avgSpeedA = np.nanmean(velA)
	avgSpeedB = np.nanmean(velB)

	# Std	
	stdSpeed = np.nanstd(vel)
	stdSpeedA = np.nanstd(velA)
	stdSpeedB = np.nanstd(velB)

	# Min	
	minSpeed = np.nanmin(vel)
	minSpeedA = np.nanmin(velA)
	minSpeedB = np.nanmin(velB)

	# Max	
	maxSpeed = np.nanmax(vel)
	maxSpeedA = np.nanmax(velA)
	maxSpeedB = np.nanmax(velB)

	###########################################################
	# Export
	# Averages
	exportDataString.append('avgSpeed')
	exportData.append(avgSpeed)
	
	exportFullExplanation.append('Average speed (in m/s) of all players per %s.' %aggregateString)

	exportDataString.append('avgSpeedA')
	exportData.append(avgSpeedA)
	exportFullExplanation.append('Average speed (in m/s) of all players from %s per %s.' %(TeamAstring,aggregateString))


	exportDataString.append('avgSpeedB')
	exportData.append(avgSpeedB)
	exportFullExplanation.append('Average speed (in m/s) of all players from %s per %s.' %(TeamBstring,aggregateString))

	# Stds	
	exportDataString.append('stdSpeed')
	exportData.append(stdSpeed)
	exportFullExplanation.append('Standard deviation speed (in m/s) of all players per %s.' %aggregateString)

	exportDataString.append('stdSpeedA')
	exportData.append(stdSpeedA)
	exportFullExplanation.append('Standard deviation speed (in m/s) of all players from %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('stdSpeedB')
	exportData.append(stdSpeedB)
	exportFullExplanation.append('Standard deviation speed (in m/s) of all players from %s per %s.' %(TeamBstring,aggregateString))

	# minmax
	exportDataString.append('minSpeed')
	exportData.append(minSpeed)
	exportFullExplanation.append('Minimum speed (in m/s) of all players per %s.' %aggregateString)

	exportDataString.append('minSpeedA')
	exportData.append(minSpeedA)
	exportFullExplanation.append('Minimum speed (in m/s) of all players from %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('minSpeedB')
	exportData.append(minSpeedB)
	exportFullExplanation.append('Minimum speed (in m/s) of all players from %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('maxSpeed')
	exportData.append(maxSpeed)
	exportFullExplanation.append('Maximum speed (in m/s) of all players per %s.' %aggregateString)

	exportDataString.append('maxSpeedA')
	exportData.append(maxSpeedA)
	exportFullExplanation.append('Maximum speed (in m/s) of all players from %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('maxSpeedB')
	exportData.append(maxSpeedB)
	exportFullExplanation.append('Maximum speed (in m/s) of all players from %s per %s.' %(TeamBstring,aggregateString))


	#########################################

	exportDataString.append('avgSpreadA')
	exportData.append(np.nanmean(attributeDict['SpreadA'][rowswithinrange]))
	exportFullExplanation.append('The average spread (= averge player distance to team centroid) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('avgSpreadB')
	exportData.append(np.nanmean(attributeDict['SpreadB'][rowswithinrange]))
	exportFullExplanation.append('The average spread (= averge player distance to team centroid) of %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('stdSpreadA')
	exportData.append(np.nanstd(attributeDict['SpreadA'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the spread (= averge player distance to team centroid) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('stdSpreadB')
	exportData.append(np.nanstd(attributeDict['SpreadB'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the spread (= averge player distance to team centroid) of %s per %s.' %(TeamBstring,aggregateString))


	exportDataString.append('avg_stdSpreadA')
	exportData.append(np.nanmean(attributeDict['stdSpreadA'][rowswithinrange]))
	exportFullExplanation.append('The average uniformity around center (= std player distance to team centroid) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('avg_stdSpreadB')
	exportData.append(np.nanmean(attributeDict['stdSpreadB'][rowswithinrange]))
	exportFullExplanation.append('The average uniformity around center (= std player distance to team centroid) of %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('std_stdSpreadA')
	exportData.append(np.nanstd(attributeDict['stdSpreadA'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the uniformity around center (= std player distance to team centroid) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('std_stdSpreadB')
	exportData.append(np.nanstd(attributeDict['stdSpreadB'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the uniformity around center (= std player distance to team centroid) of %s per %s.' %(TeamBstring,aggregateString))

	
	exportDataString.append('avgWidthA')
	exportData.append(np.nanmean(attributeDict['WidthA'][rowswithinrange]))
	exportFullExplanation.append('The average width (= widest X distance between players) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('avgWidthB')
	exportData.append(np.nanmean(attributeDict['WidthB'][rowswithinrange]))
	exportFullExplanation.append('The average width (= widest X distance between players) of %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('stdWidthA')
	exportData.append(np.nanstd(attributeDict['WidthA'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the width (= widest X distance between players) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('stdWidthB')
	exportData.append(np.nanstd(attributeDict['WidthB'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the width (= widest X distance between players) of %s per %s.' %(TeamBstring,aggregateString))


	exportDataString.append('avgLengthA')
	exportData.append(np.nanmean(attributeDict['LengthA'][rowswithinrange]))
	exportFullExplanation.append('The average length (= widest Y distance between players) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('avgLengthB')
	exportData.append(np.nanmean(attributeDict['LengthB'][rowswithinrange]))
	exportFullExplanation.append('The average length (= widest Y distance between players) of %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('stdLengthA')
	exportData.append(np.nanstd(attributeDict['LengthA'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the length (= widest Y distance between players) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('stdLengthB')
	exportData.append(np.nanstd(attributeDict['LengthB'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the length (= widest Y distance between players) of %s per %s.' %(TeamBstring,aggregateString))
	

	exportDataString.append('avgSurfaceA')
	exportData.append(np.nanmean(attributeDict['SurfaceA'][rowswithinrange]))
	exportFullExplanation.append('The average surface area (in m^2) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('avgSurfaceB')
	exportData.append(np.nanmean(attributeDict['SurfaceB'][rowswithinrange]))
	exportFullExplanation.append('The average surface area (in m^2) of %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('stdSurfaceA')
	exportData.append(np.nanstd(attributeDict['SurfaceA'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the surface area (in m^2) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('stdSurfaceB')
	exportData.append(np.nanstd(attributeDict['SurfaceB'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the surface area (in m^2) of %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('avgsumVerticesA')
	exportData.append(np.nanmean(attributeDict['SumVerticesA'][rowswithinrange]))
	exportFullExplanation.append('The average circumference (in m) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('avgsumVerticesB')
	exportData.append(np.nanmean(attributeDict['SumVerticesB'][rowswithinrange]))
	exportFullExplanation.append('The average circumference (in m) of %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('stdsumVerticesA')
	exportData.append(np.nanstd(attributeDict['SumVerticesA'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the circumference (in m) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('stdsumVerticesB')
	exportData.append(np.nanstd(attributeDict['SumVerticesB'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the circumference (in m) of %s per %s.' %(TeamBstring,aggregateString))


	exportDataString.append('avgShapeRatioA')
	exportData.append(np.nanmean(attributeDict['ShapeRatioA'][rowswithinrange]))
	exportFullExplanation.append('The average shape ratio (longest rib / shortest rib) (values < 1 should not be possible) (the closer to 1, the more uniformly spread out) of %s per %s (NB: this is a measure of spread and should correlate with stdSpreadA).' %(TeamAstring,aggregateString))

	exportDataString.append('avgShapeRatioB')
	exportData.append(np.nanmean(attributeDict['ShapeRatioB'][rowswithinrange]))
	exportFullExplanation.append('The average shape ratio (longest rib / shortest rib) (values < 1 should not be possible) (the closer to 1, the more uniformly spread out) of %s per %s (NB: this is a measure of spread and should correlate with stdSpreadA).' %(TeamBstring,aggregateString))

	exportDataString.append('stdShapeRatioA')
	exportData.append(np.nanstd(attributeDict['ShapeRatioA'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the shape ratio (longest rib / shortest rib) (values < 1 should not be possible) (the closer to 1, the more uniformly spread out) of %s per %s (NB: this is a measure of spread and should correlate with stdSpreadA).' %(TeamAstring,aggregateString))

	exportDataString.append('stdShapeRatioB')
	exportData.append(np.nanstd(attributeDict['ShapeRatioB'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the shape ratio (longest rib / shortest rib) (values < 1 should not be possible) (the closer to 1, the more uniformly spread out) of %s per %s (NB: this is a measure of spread and should correlate with stdSpreadA).' %(TeamBstring,aggregateString))

	# Unused:
	# attributeDict['Possession/Turnover ']	attributeDict['Pass']	attributeDict['distFrame']	attributeDict['Goal ']	attributeDict['currentPossession'] attributeDict['TeamCentXA']	attributeDict['TeamCentYA']	attributeDict['TeamCentXB']	attributeDict['TeamCentYB']

	return 	exportData,exportDataString,exportFullExplanation

def findRows(aggregateLevel,targetEvents,rawDict,TeamAstring,TeamBstring,currentEvent):

	specialCase = False
	skipCurrentEvent = False
	if type(targetEvents[aggregateLevel[0]]) != list:			
		# A special case: there was only one event identified, which means that <enumerate> now 
		# goes through the contents of that specific event, rather than iterating over the events.
		# Improvised solution is to overwrite currentEvent and subsequently terminate early.
		#
		# necessary to adjust input style of aggregateLevel[0] (which determines currentEvent)
		# find a better way to store aggregateLevel ?
		# Gebeurt alleen bij full?
		currentEvent = targetEvents[aggregateLevel[0]]
		# fileAggregateID = aggregateString + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')'
		specialCase = True
	# Determine tStart and tEnd
	if aggregateLevel[0] == 'Possession' or aggregateLevel[0] == 'Full' or aggregateLevel[0] == 'Run':
		# These are the events that have a fixed window. Technically it combines 2 events. The start of a targetEvent and the end of a targetEvent.
		# E.g., from possession start to possession end.
		# E.g., from start of the file to the end.
		# E.g., from the start of an attack to the end.
		# In general terms:
		# Anything that has a start and an end time should be captured here.
		tStart = currentEvent[0]
		tEnd = currentEvent[1]
		# fileAggregateID = aggregateString + '_' 'window(all)_lag(none)'
	else:
		# And these are the remaining ones. The ones for which the ijk-algorithm should be designed.
		# Here, the window is determined by the user input: window-size and lag (and possibly in the future aggregation method)

		tEnd = currentEvent[0] - aggregateLevel[2]
		tStart = tEnd - aggregateLevel[1]
		# fileAggregateID = aggregateString + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')'

	# Determine the rows corresponding to the current event.
	if tEnd == None or tStart == None: # Check if both end and start are allocated
		skipCurrentEvent = True
		return None,None,None,None,None,None,None,skipCurrentEvent
	# Find index of rows within tStart and tEnd
	if round(tStart,2) <= round(tEnd,2):
		window = (tStart,tEnd)
	else:
		window = (tEnd,tStart)
		warn('\nSTRANGE: tStart <%s> was bigger than tEnd <%s>.\nSwapped them to determine window' %(tStart,tEnd))
	
	tmp = rawDict[rawDict['Ts'] > window[0]]
	rowswithinrange = tmp[tmp['Ts'] <= window[1]].index
	del(tmp)

	rowswithinrangeTeam = rawDict['Ts'][rowswithinrange].index[rawDict['PlayerID'][rowswithinrange] == 'groupRow']
	rowswithinrangeBall = rawDict['Ts'][rowswithinrange].index[rawDict['PlayerID'][rowswithinrange] == 'ball']
	rowswithinrangePlayer = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'][rowswithinrange] != '']
	rowswithinrangePlayerA = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'][rowswithinrange] == TeamAstring]
	rowswithinrangePlayerB = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'][rowswithinrange] == TeamBstring]

	return window,rowswithinrange,rowswithinrangeTeam,rowswithinrangeBall,rowswithinrangePlayer,rowswithinrangePlayerA,rowswithinrangePlayerB,specialCase,skipCurrentEvent

def determineWindow(aggregateLevel,targetEvents,rawDict,TeamAstring,TeamBstring,currentEvent):

	specialCase = False
	skipCurrentEvent = False
	if type(targetEvents[aggregateLevel[0]]) != list:			
		# A special case: there was only one event identified, which means that <enumerate> now 
		# goes through the contents of that specific event, rather than iterating over the events.
		# Improvised solution is to overwrite currentEvent and subsequently terminate early.
		#
		# necessary to adjust input style of aggregateLevel[0] (which determines currentEvent)
		# find a better way to store aggregateLevel ?
		# Gebeurt alleen bij full?
		currentEvent = targetEvents[aggregateLevel[0]]
		# fileAggregateID = aggregateString + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')'
		specialCase = True
	# Determine tStart and tEnd
	if aggregateLevel[0] == 'Possession' or aggregateLevel[0] == 'Full' or aggregateLevel[0] == 'Run':
		# These are the events that have a fixed window. Technically it combines 2 events. The start of a targetEvent and the end of a targetEvent.
		# E.g., from possession start to possession end.
		# E.g., from start of the file to the end.
		# E.g., from the start of an attack to the end.
		# In general terms:
		# Anything that has a start and an end time should be captured here.
		tStart = currentEvent[0]
		tEnd = currentEvent[1]
		# fileAggregateID = aggregateString + '_' 'window(all)_lag(none)'
	else:
		# And these are the remaining ones. The ones for which the ijk-algorithm should be designed.
		# Here, the window is determined by the user input: window-size and lag (and possibly in the future aggregation method)

		tEnd = currentEvent[0] - aggregateLevel[2]
		tStart = tEnd - aggregateLevel[1]
		# fileAggregateID = aggregateString + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')'

	# Determine the rows corresponding to the current event.
	if tEnd == None or tStart == None: # Check if both end and start are allocated
		skipCurrentEvent = True
		return None,None,skipCurrentEvent
	# Find index of rows within tStart and tEnd
	if round(tStart,2) <= round(tEnd,2):
		window = (tStart,tEnd)
	else:
		window = (tEnd,tStart)
		warn('\nSTRANGE: tStart <%s> was bigger than tEnd <%s>.\nSwapped them to determine window' %(tStart,tEnd))

	# # Determine reference team
	# print(targetEvents)
	# pdb.set_trace()
	
	# tmp = rawDict[rawDict['Ts'] > window[0]]
	# rowswithinrange = tmp[tmp['Ts'] <= window[1]].index
	# del(tmp)

	# rowswithinrangeTeam = rawDict['Ts'][rowswithinrange].index[rawDict['PlayerID'][rowswithinrange] == 'groupRow']
	# rowswithinrangeBall = rawDict['Ts'][rowswithinrange].index[rawDict['PlayerID'][rowswithinrange] == 'ball']
	# rowswithinrangePlayer = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'][rowswithinrange] != '']
	# rowswithinrangePlayerA = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'][rowswithinrange] == TeamAstring]
	# rowswithinrangePlayerB = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'][rowswithinrange] == TeamBstring]

	return window,specialCase,skipCurrentEvent
def aggregateTemporallyINCEPTION(population,method,curEventExcerptPanda,key,refTeam,othTeam,attrLabel,eventDescrString, exportCurrentData, overallString, overallExplanation):
	
	# # Part of input:
	# population = 'allTeam'
	# population = 'refTeam'
	# population = 'othTeam'

	# method = 'perTimePerPlayer' # method 1, axis = 1
	# method = 'perPlayerPerTime' # method 2, axis = 0


	######### put this in a function ############
	# All players
	pivotedData = curEventExcerptPanda.pivot(index = 'eventTimeIndex', columns = 'PlayerID', values = key)

	# Allplayers of refTeam
	curContent_refTeam = curEventExcerptPanda['TeamID'] == refTeam
	pivotedData_refTeam = curEventExcerptPanda[curContent_refTeam].pivot(index = 'eventTimeIndex', columns = 'PlayerID', values = key)
	# Allplayers of othTeam
	curContent_othTeam = curEventExcerptPanda['TeamID'] == othTeam
	pivotedData_othTeam = curEventExcerptPanda[curContent_othTeam].pivot(index = 'eventTimeIndex', columns = 'PlayerID', values = key)
	
	# two ways:
	# 1) average per time per player (of a team)
	# 2) average per player (of a team) per time

	# Function should be run for:
	# allTeam, method 1
	# allTeam, method 2
	# refTeam, method 1
	# refTeam, method 2
	# othTeam, method 1
	# othTeam, method 2

	# Subfunction input:
	# Population (refTeam AND othTeam, refTeam, or othTeam)
	# Method (1 or 2)

	if population == 'allTeam':
		tmpData = pivotedData
	elif population == 'refTeam':
		tmpData = pivotedData_refTeam

	elif population == 'othTeam':
		tmpData = pivotedData_othTeam
	else:
		warn('\nWARNING: Did not recognize aggregation population (one of the teams, or both).\nSpecify it with either <allTeam>, <refTeam> or <othTeam>. \nContinued by default with <allTeam>.')

	if method == 'perTimePerPlayer':
		axis = 1
	elif method == 'perPlayerPerTime':
		axis = 0
	else:
		warn('\nWARNING: Did not recognize aggregation method (first over time, or first over player).\nSpecify it with either <perTimePerPlayer> or <perPlayerPerTime>. \nContinued by default with <perPlayerPerTime>.')
		axis = 0

	count, avg, std, sumVal, minVal, maxVal, med, sem, kur, ske = \
	aggregateTemporally(tmpData,[], axis = axis) # axis = 0 ==> per player; axis = 1 ==> per time

	## make this a subfunction / for loop
	# It should be run for all:
	aggrMethStrings = ['avg', 'std', 'sumVal', 'minVal', 'maxVal', 'med', 'sem', 'kur', 'ske']
	for nth, data in enumerate([avg, std, sumVal, minVal, maxVal, med, sem, kur, ske]):
		
		curKeyString = key + '_' + aggrMethStrings[nth] + '_' + method

		exportCurrentData, overallString, overallExplanation = \
		aggregateTemporally(data,exportCurrentData,population,curKeyString,attrLabel[key],eventDescrString, overallString, overallExplanation)

	# print(data)
	return exportCurrentData, overallString, overallExplanation
	
	######### put this in a function ############