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

	targetEvents = \
	student_XX_computeEvents.process(targetEvents,aggregateLevel,rawPanda,attrPanda,eventsPanda,TeamAstring,TeamBstring)
	
	if debuggingMode:
		elapsed = str(round(time.time() - tComputeEvents, 2))
		print('***** Time elapsed during computeEvents: %ss' %elapsed)
	
	return targetEvents

############################################################
############################################################

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