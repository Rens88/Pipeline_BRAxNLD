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
import student_LT_computeEvents

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

	targetEvents = \
	student_LT_computeEvents.process(targetEvents,aggregateLevel,rawPanda,attrPanda,eventsPanda,TeamAstring,TeamBstring)
	
	if debuggingMode:
		elapsed = str(round(time.time() - tComputeEvents, 2))
		print('Time elapsed during computeEvents: %ss' %elapsed)
	
	return targetEvents

############################################################
############################################################

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

	targetEvents = {**targetEvents,'Random':randomEvents}

	return targetEvents