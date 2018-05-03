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
	print('################')
	print(targetEvents)

	return targetEvents

#LT: afronden nodig?
def attackEvents(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring):
	consecutiveTs = 5 #consecutive timestamps to call it an attack
	timeCount = 0
	timeFrequenty = 0.1 #in seconds
	beginTime = 0
	endTime = 0
	secondsForHalfTime = 60 # 1 minute, seconds to determine if it is half time

	inZone = rawPanda[attrPanda['inZone'] == 1]

	attackEvents = []

	previousTime = 0
	for idx,i in enumerate(pd.unique(inZone['Ts'])):
		curTime = round(i,1)
		curTeamInZone = inZone['TeamID'][inZone['Ts'] == i]

		#determine current team
		if all(curTeamInZone == TeamAstring):
			curTeam = TeamAstring
		elif all(curTeamInZone == TeamBstring):
			curTeam = TeamBstring

		#determine beginTime of attack
		if round(curTime - previousTime,1) == timeFrequenty:
			timeCount = timeCount + 1
		else:
			timeCount = 0

		if timeCount == (consecutiveTs - 1):
			beginTime = round(curTime - (consecutiveTs - 1) * timeFrequenty,1)

		#determine endTime of attack, also if match ends during attack
		if (curTime - previousTime) > (consecutiveTs - 1) * timeFrequenty or (curTime - previousTime) > secondsForHalfTime or curTime == max(inZone['Ts']):
			endTime = previousTime

		if endTime > 0:
			attackEvents.append((beginTime,curTeam,endTime))

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
