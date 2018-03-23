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

	return targetEvents

def addRandomEvents(rawPanda,targetEvents,TeamAstring,TeamBstring):

	# Number of random events
	nRandom = 5
	tStart = math.ceil(targetEvents['Full'][0])
	tEnd = math.floor(targetEvents['Full'][1])

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
