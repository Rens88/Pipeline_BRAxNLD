# A student's template.
# To make your module function with the pipeline:
# 0) copy the whole example folder, and replace XX with your initials
# 1) edit the name of the student's function in computeEvents.py
# 2) edit the student function that's imported at the top of computeEvents.py
# 	(where it now says "import student_XX_computeEvents")
# 3) change the string content of <studentFolder> in the 'process_Template' (NB: you can make a copy of process_Template and call it process_StudentXX)

import numpy as np
import math
import pandas as pd
from warnings import warn
import pdb; #pdb.set_trace()
import random

## Here, you can clarify which functions exist in this module.
if __name__ == '__main__': 

	process(targetEvents,aggregateLevel,rawPanda,attrPanda,eventsPanda,TeamAstring,TeamBstring)

	# This is an example that can be used to see how you compute an event
	addRandomEvents(rawPanda,targetEvents,TeamAstring,TeamBstring)
	
## Here, you specifiy what each function does
def process(targetEvents,aggregateLevel,rawPanda,attrPanda,eventsPanda,TeamAstring,TeamBstring):
	targetEvents = addRandomEvents(rawPanda,targetEvents,TeamAstring,TeamBstring)
	targetEvents = attackEvents(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring)
	# print('################')
	# print(targetEvents)
	# print(len(targetEvents['attack']))
	# pdb.set_trace()
	

	return targetEvents

#LT: afronden nodig?
def attackEvents(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring):
	if 'shotNotOnTarget' not in targetEvents or 'shotOnTarget' not in targetEvents or 'goal' not in targetEvents:
		warn('\nWARNING: Shots and goals are not labeled. Check importEvents.\n')

	consecutiveTs = 5 #consecutive timestamps to call it an attack
	timeCount = 0
	timeFrequenty = 0.1 #in seconds
	beginTime = 0
	endTime = 0
	attackStart = False
	# secondsForHalfTime = 60 # 1 minute, seconds to determine if it is half time

	#labels for event result
	noShotLabel = 0
	shotNotOnTargetLabel = 1
	shotOnTargetLabel = 2
	goalLabel = 3

	allDict = rawPanda.join(attrPanda, lsuffix='_raw', rsuffix='_attr')

	opponentsHalf = allDict[allDict['opponentsHalf'] == 1]

	attackEvents = []

	previousTime = 0
	for idx,i in enumerate(pd.unique(opponentsHalf['Ts_raw'])):
		curTime = round(i,1)
		curTeamOpponentsHalf = opponentsHalf['TeamID'][opponentsHalf['Ts_raw'] == i].values[0]
		eventSet = False

		#determine beginTime of attack. Attack starts if player with ball is for 0.5 seconds in final third
		if round(curTime - previousTime,1) == timeFrequenty and all(opponentsHalf['inZone'][opponentsHalf['Ts_raw'] == i] == 1) and not attackStart:
			timeCount = timeCount + 1
		else:
			timeCount = 0

		if timeCount == (consecutiveTs - 1):
			beginTime = round(curTime - (consecutiveTs - 1) * timeFrequenty,1)
			attackStart = True

		#determine endTime of attack. attack ends if ball is for 0.5 seconds on own half or opponent obtains possession, or match ends during attack.
		if ((curTime - previousTime) > (consecutiveTs - 1) * timeFrequenty or curTime == max(opponentsHalf['Ts_raw'])) and attackStart:
			endTime = previousTime
			attackStart = False

		#LT: begin en endTime omdraaien?
		if endTime > 0:
			if 'shotNotOnTarget' in targetEvents:
				for idx, i in enumerate(targetEvents['shotNotOnTarget']):
					if i[0] >= beginTime and i[0] < endTime:
						attackEvents.append((beginTime,curTeamOpponentsHalf,endTime,shotNotOnTargetLabel))
						eventSet = True
						break
			if 'shotOnTarget' in targetEvents:
				if not eventSet:
					for idx, i in enumerate(targetEvents['shotOnTarget']):
						if i[0] >= beginTime and i[0] < endTime:
							attackEvents.append((beginTime,curTeamOpponentsHalf,endTime,shotOnTargetLabel))
							eventSet = True
							break
			if 'goal' in targetEvents:
				if not eventSet:
					for idx, i in enumerate(targetEvents['goal']):
						if i[0] >= beginTime and i[0] < endTime:
							attackEvents.append((beginTime,curTeamOpponentsHalf,endTime,goalLabel))
							eventSet = True
							break
			if not eventSet:
				attackEvents.append((beginTime,curTeamOpponentsHalf,endTime,noShotLabel))

		previousTime = curTime
		endTime = 0

	targetEvents = {**targetEvents,'attack': attackEvents}

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

	targetEvents = {**targetEvents,'RandomStudent':randomEvents}

	return targetEvents
