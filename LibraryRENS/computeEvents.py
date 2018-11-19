# 08-12-2017 Rens Meerhoff

import csv
import pdb; #pdb.set_trace()
import numpy as np
from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs, sep, walk
from warnings import warn
import math
import random
# From my own library:
import plotSnapshot
import safetyWarning
import pandas as pd
import time
import student_LT_computeEvents

if __name__ == '__main__':

	process(targetEvents,aggregateLevel,rawPanda,attrPanda,eventsPanda,debuggingMode)

	# Compute some random events, demonstration purposes only
	randomExample()
	#####################################################################################
	#####################################################################################

def process(targetEvents,aggregateLevel,rawPanda,attrPanda,eventsPanda,TeamAstring,TeamBstring,dataFolder,cleanFname,debuggingMode,skipComputeEvents_curFile):
	tComputeEvents = time.time()
	eventClassified = False
	if skipComputeEvents_curFile:
		# Load previously computed target events
		targetFolder = dataFolder + sep + 'existingTargets' + sep
		preComputedTargetFolder = targetFolder + 'preComputed' + sep
		
		# find a way to iterate over all keys.
		files = listdir(preComputedTargetFolder)
		curFiles = [f[:-4] for f in files if cleanFname[:-12] + '_preComputed_Event_' in f]
		curKeys = [cf.split('_')[-1] for cf in curFiles]

		for key in curKeys:
			targetEventsFname = preComputedTargetFolder + cleanFname[:-12] + '_preComputed_Event_' + key +  '.csv'
			targetEvents.update({key:[]})
			df_targetEvents = pd.read_csv(targetEventsFname)
			for idx in df_targetEvents.index:
				curList = [df_targetEvents.loc[idx,key] for key in df_targetEvents.keys()]

				if key == 'Turnovers':
					# tuple was stored as string, convert it to tuple again.
					tmp = curList[6].split(', ')
					tmp0 = tmp[0].replace('(','')
					tmp1 = tmp[1].replace(')','')

					curList[6] = (float(tmp0),float(tmp1))

				curList[-1] = bool(curList[-1])
				curList = curList[1:] # drop the index
				curTuple = tuple(curList)
				targetEvents[key].append(curTuple)

		if debuggingMode:
			elapsed = str(round(time.time() - tComputeEvents, 2))
			print('***** Time elapsed during computeEvents: %ss' %elapsed)
		
		return targetEvents

	# For demonstration purposes, generate some random events
	if aggregateLevel[0].lower() == 'random':
		targetEvents = \
		addRandomEvents(rawPanda,targetEvents,TeamAstring,TeamBstring)	

	if aggregateLevel[0].lower() == 'regular':
		targetEvents = \
		addRegularEvents(rawPanda,targetEvents,TeamAstring,TeamBstring,aggregateLevel)	

	### TO DO, PUT IN SEPARATE FUNCTION
	if 'Turnovers' in targetEvents and targetEvents['Turnovers'] != []:
		
		if len(targetEvents['Turnovers'][0]) == 6: 
			# it's the Turnovers with XY
			# So determine in or outside of 16m
			for idx,val in enumerate(targetEvents['Turnovers']):
				curTs = val[0]
				refTeam = val[1]
				if refTeam == TeamAstring:
					othTeam = TeamBstring
				elif refTeam == TeamBstring:
					othTeam = TeamAstring
				else:
					warn('\nFATAL WARNING: Could not determine othTeam in Turnovers events...\nrefTeam = <%s>\nTeamAstring = <%s>\nTeamBstring = <%s>\n' %(refTeam,TeamAstring,TeamBstring))
					pdb.set_trace()

				curX = val[5][0]
				curY = val[5][1]
				
				# NOT TAKING FIELD DIMENSIONS INTO ACCOUNT
				warn('\nWARNING: I\'m not (yet) taking field dimensions into account.\nPenalty box is based on generic measures (same as in plotSnapshot)\n')
				warn('\nWARNING: At computEvents, I\'m now only very crudely verifying if the Turnovers happened on the refTeam\'s side of the pitch.\n')
				avgRefTeam = np.nanmean(rawPanda.loc[rawPanda['TeamID'] == refTeam,'X'])
				avgOthTeam = np.nanmean(rawPanda.loc[rawPanda['TeamID'] == othTeam,'X'])

				label = 'outside_16m'
				if curY >= -20 and curY <= 20:
					if curX <= -33.5: # left attacking
						label = 'inside_16m'
						if avgRefTeam < avgOthTeam:
							warn('\nWARNING: Suspected turnover on own half.\nCheck event data and mismatches in Turnovers.\nCurrently, these event labels are overwritten as <outside_16m>.\n')
							label = 'outside_16m'
					elif curX >= 33.5: # right attacking
						label = 'inside_16m'
						if avgRefTeam > avgOthTeam:
							warn('\nWARNING: Suspected turnover on own half.\nCheck event data and mismatches in Turnovers.\nCurrently, these event labels are overwritten as <outside_16m>.\n')
							label = 'outside_16m'
			
				# write a checkup based on the guessed data driven playing side
				# only for inside
				eventClassified = True
				newVal = (val[0],val[1],val[2],val[3],val[4],val[5],label)
				targetEvents['Turnovers'][idx] = newVal

	targetEvents,eventClassified = \
	student_LT_computeEvents.process(targetEvents,aggregateLevel,rawPanda,attrPanda,eventsPanda,TeamAstring,TeamBstring,eventClassified)

	# export it
	targetFolder = dataFolder + sep + 'existingTargets' + sep
	if not exists(targetFolder):
		warn('\nFATAL WARNING: folder <existingTargets> in dataFolder <%s> NOT found.\nShould have been created in initialization.py\n' %dataFolder)

	preComputedTargetFolder = targetFolder + 'preComputed' + sep
	

	# Deal with overlapping targets
	# by default, the last value added to the tuple informs about whether there was any overlap with the previous event. The second to last about the time difference between events
	# & export it 
	for key in targetEvents.keys():
		# Deal with overlap
		excludeOverlappingEvents= True
		methodOverlap = 'include' # or 'limitWindow' or 'exclude'
		targetEvents = overlappingTargets(targetEvents,key,aggregateLevel,excludeOverlappingEvents,methodOverlap)
		
		# Export it
		targetEventsFname = preComputedTargetFolder + cleanFname[:-12] + '_preComputed_Event_' + key +  '.csv'
		tmp = pd.DataFrame.from_dict(targetEvents[key])
		tmp.to_csv(targetEventsFname) # filename only needs match	
	
	if debuggingMode:
		elapsed = str(round(time.time() - tComputeEvents, 2))
		print('***** Time elapsed during computeEvents: %ss' %elapsed)
	
	return targetEvents,eventClassified

############################################################
############################################################

def overlappingTargets(targetEvents,key,aggregateLevel,excludeOverlappingEvents,methodOverlap):

	availableWindow = []
	nOriginal = len(targetEvents[key])

	targetEvents_new = {key:[]}
	# for idx,currentEvent in enumerate(targetEvents[key]):
	for idx in np.arange(0,nOriginal):
		currentEvent = targetEvents[key][idx]
		targetEvents_new[key].append(currentEvent)
		if currentEvent[0] == None:
			try:
				warn('\nWARNING: Event had no time of occurrence. Therefore, it was skipped.\nThese are the event contents:\ntime of event = %s\nRefTeam of event = %s\nend of event = %s' %currentEvent)
			except: # a lazy way to catch currentEvents with only 2 bits of info.
				warn('\nWARNING: Event had no time of occurrence. Therefore, it was skipped.\nThese are the event contents:\ntime of event = %s\nRefTeam of event = %s\n' %currentEvent)				
			continue
		## Determine the window
		if aggregateLevel[1] != None:
			tEnd = currentEvent[0] - aggregateLevel[2] # tEnd - lag
			tStart = currentEvent[0] - aggregateLevel[1] - aggregateLevel[2] # tEnd - window - lagg
		else:
			tEnd = currentEvent[0] - aggregateLevel[2]
			# No window indicated, so take the full event (if possible)
			if len(currentEvent) < 3 or aggregateLevel[2] == None:
				warn('\nWARNING: No tStart indicated for this event, nor a window was given for the temporal aggregation.\nCould not determine whether the event overlapped the previous event.\n')
				tStart = None		
			else:				
				tStart = currentEvent[2]# - aggregateLevel[1]			


		dtPrev = tEnd # by default, dtPrev is as big as the window (only relevant for the first event)
		targetEvents_new[key][-1] = currentEvent + (dtPrev,False,)

		if idx != 0:

			# # store the time in-between events that can help determine the maximum window size you may want to choose
			dtPrev = tEnd - tEnd_prevEvent
			targetEvents_new[key][-1] = currentEvent + (dtPrev,False,)
			availableWindow.append(dtPrev)

			if tStart != None:
				if tEnd_prevEvent > tStart:
					targetEvents_new[key][-1] = currentEvent + (dtPrev,True,)
					currentEvent = targetEvents_new[key][-1]

					if excludeOverlappingEvents:					
						if methodOverlap == 'exclude':
							# simply exclude

							targetEvents_new[key].remove(currentEvent)

						elif methodOverlap == 'limitWindow':
							# or limit the window size 
							# and report window limit
							tStart = tEnd_prevEvent
							warn('UNFINISHED')
						elif methodOverlap == 'include':
							donothing = [] # just leave it in there
						else:
							warn('\nWARNING: Only <exclude> or <limitWindow> are valid inputs for <methodOverlap> when excluding events in temporalAggregation.\nBy default, overlapping events are skipped.\n')
			# else:
			# 	print('hi')
			# 	targetEvents_new[key][-1] = currentEvent + (False,)
		tEnd_prevEvent = tEnd

	targetEvents[key] = targetEvents_new[key]

	if nOriginal != len(targetEvents[key]):
		nRemoved = nOriginal - len(targetEvents[key])
		warn('\nWARNING: <%s> out of <%s> targetEvents were removed because the window would have overlapped the previous event.\nConsider choosing a window based on the availableWindows:\n%s' %(nRemoved,nOriginal,availableWindow))

	return targetEvents

def addRandomEvents(rawPanda,targetEvents,TeamAstring,TeamBstring):

	# Number of random events
	nRandom = 5
	tStart = math.ceil(targetEvents['Full'][0][2])
	tEnd = math.floor(targetEvents['Full'][0][0])

	timeRange = range(tStart,tEnd)
	
	if nRandom > len(timeRange):
		warn('\nWARNING: Number of random events was larger than seconds in the datasets.\nThe number was reduced to the number of seconds in the dataset.\n')
		nRandom = len(timeRange)
	randomTimes = random.sample(timeRange, nRandom)
	
	randomEvents = []
	teamStrings = (TeamAstring,TeamBstring)

	for randomTime in randomTimes:
		diff = abs(rawPanda['Ts'] - randomTime)
		idxEqual = diff[diff == min(diff)].index
		randomTeam = random.randint(0,1)

		if not type(idxEqual) == np.int64:
			randomEvents.append((rawPanda['Ts'][idxEqual[0]],teamStrings[randomTeam]))	
		else:
			randomEvents.append((rawPanda['Ts'][idxEqual],teamStrings[randomTeam]))	

	targetEvents = {**targetEvents,'Random':randomEvents}
	# If an error occurs here, then this may be a problem with Linux.
	# Replace with: (and an if statement to check if targetEvents isempty)
	# targetEvents = targetEvents.update({'Random':randomEvents})

	return targetEvents

def addRegularEvents(rawPanda,targetEvents,TeamAstring,TeamBstring,aggregateLevel):

	# aggregateLevel[0] --> event ID
	# aggregateLevel[1] --> window (s)
	# aggregateLevel[2] --> lag (s)


	tStart = math.ceil(targetEvents['Full'][0][2])
	tEnd = math.floor(targetEvents['Full'][0][0])


	# Number of random events
	nRandom = 5
	timeRange = range(tStart,tEnd)
	# From end to start, to make sure that the end is included	
	regularTimes = np.arange(tEnd,tStart,-aggregateLevel[1])
	regularTimes = np.sort(regularTimes)

	regularEvents = []
	# teamStrings = (TeamAstring,TeamBstring)

	for regularTime in regularTimes:
		diff = abs(rawPanda['Ts'] - regularTime)
		idxEqual = diff[diff == min(diff)].index
		# randomTeam = random.randint(0,1)

		if not type(idxEqual) == np.int64:
			regularEvents.append((rawPanda['Ts'][idxEqual[0]],TeamAstring))	
		else:
			regularEvents.append((rawPanda['Ts'][idxEqual],TeamAstring))	

	targetEvents = {**targetEvents,'Regular':regularEvents}
	# If an error occurs here, then this may be a problem with Linux.
	# Replace with: (and an if statement to check if targetEvents isempty)
	# targetEvents = targetEvents.update({'Regular':regularEvents})

	return targetEvents