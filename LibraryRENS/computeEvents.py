# 08-12-2017 Rens Meerhoff

import csv
import pdb; #pdb.set_trace()
import numpy as np
from os.path import isfile, join, isdir
from os import listdir, path
from warnings import warn
import math
import random
# From my own library:
import plotSnapshot
import safetyWarning
import pandas as pd
import time
import student_XX_computeEvents

if __name__ == '__main__':

	process(targetEvents,aggregateLevel,rawPanda,attrPanda,eventsPanda,debuggingMode)

	# Compute some random events, demonstration purposes only
	randomExample()
	#####################################################################################
	#####################################################################################

def process(targetEvents,aggregateLevel,rawPanda,attrPanda,eventsPanda,TeamAstring,TeamBstring,debuggingMode):
	tComputeEvents = time.time()

	# For demonstration purposes, generate some random events
	if aggregateLevel[0].lower() == 'random':
		targetEvents = \
		addRandomEvents(rawPanda,targetEvents,TeamAstring,TeamBstring)	

	if aggregateLevel[0].lower() == 'regular':
		targetEvents = \
		addRegularEvents(rawPanda,targetEvents,TeamAstring,TeamBstring,aggregateLevel)	

	if 'Turnovers' in targetEvents:
		
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
					warn('\nFATAL WARNING: Could not deterine othTeam in Turnovers events...\nrefTeam = <%s>\nTeamAstring = <%s>\nTeamBstring = <%s>\n' %(refTeam,TeamAstring,TeamBstring))
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

				newVal = (val[0],val[1],val[2],val[3],val[4],val[5],label)
				targetEvents['Turnovers'][idx] = newVal

	targetEvents = \
	student_XX_computeEvents.process(targetEvents,aggregateLevel,rawPanda,attrPanda,eventsPanda,TeamAstring,TeamBstring)

	# Deal with overlapping targets
	# by default, the last value added to the tuple informs about whether there was any overlap with the previous event.
	excludeOverlappingEvents= True
	methodOverlap = 'include' # or 'limitWindow' or 'exclude'
	targetEvents = overlappingTargets(targetEvents,aggregateLevel,excludeOverlappingEvents,methodOverlap)

	
	if debuggingMode:
		elapsed = str(round(time.time() - tComputeEvents, 2))
		print('***** Time elapsed during computeEvents: %ss' %elapsed)
	
	return targetEvents

############################################################
############################################################

def overlappingTargets(targetEvents,aggregateLevel,excludeOverlappingEvents,methodOverlap):

	availableWindow = []
	nOriginal = len(targetEvents[aggregateLevel[0]])

	targetEvents_new = {aggregateLevel[0]:[]}
	# for idx,currentEvent in enumerate(targetEvents[aggregateLevel[0]]):
	for idx in np.arange(0,nOriginal):
		currentEvent = targetEvents[aggregateLevel[0]][idx]
		targetEvents_new[aggregateLevel[0]].append(currentEvent)
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
			# No window indicated, so take the full event (if possible)
			if len(currentEvent) < 3:
				warn('\nFATAL WARNING: No tStart indicated for this event, nor a window was given for the temporal aggregation.\nEither: 1) Indicate a window for the temporal aggregation.\nOr: 2) Export a tStart for the event you\'re aggregating.')
						
			tEnd = currentEvent[0]# - aggregateLevel[2]
			tStart = currentEvent[2]# - aggregateLevel[1]			

		targetEvents_new[aggregateLevel[0]][-1] = currentEvent + (False,)

		if idx != 0:

			# # store the time in-between events that can help determine the maximum window size you may want to choose
			availableWindow.append(tEnd - tEnd_prevEvent)


			if tEnd_prevEvent > tStart:
				targetEvents_new[aggregateLevel[0]][-1] = currentEvent + (True,)
				currentEvent = targetEvents_new[aggregateLevel[0]][-1]

				if excludeOverlappingEvents:					
					if methodOverlap == 'exclude':
						# simply exclude

						targetEvents_new[aggregateLevel[0]].remove(currentEvent)

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
			# 	targetEvents_new[aggregateLevel[0]][-1] = currentEvent + (False,)
		tEnd_prevEvent = tEnd

	targetEvents[aggregateLevel[0]] = targetEvents_new[aggregateLevel[0]]

	if nOriginal != len(targetEvents[aggregateLevel[0]]):
		nRemoved = nOriginal - len(targetEvents[aggregateLevel[0]])
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