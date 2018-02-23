# 08-02-2018 Rens Meerhoff
# Script can be generically used to import information from attributeDict. If the data is not organized similarly, it will simply export empty targetEvents. In the future, we can include LMP-based import of these kinds of data (or perhaps standardize the format earlier in the pipeline).
# Now, the script returns a dictionary of 'targetEvents' with the following format:
# targetEvents['Goals'] = (instant (s) a goal was scored, team that scored)
# targetEvents['Possession'] = (instant possession started (s), instant possession ended (s), team that had possession)
# targetEvents['Turnovers'] = (instant (s) a turnover occurred, team that turned over the ball)
# targetEvents['Passes'] = (instant(s) a pass occurred, team that made the pass, nth possession the pass belongs to)

# 05-02-2018 Rens Meerhoff
# Based on countExistingEvents.py:
# Script now only exports timestamp (in seconds) and identifier of existing events.
#
# Previously:
# 06-12-2017 Rens Meerhoff
# Script to obtain game/trial-based results of existing events (goals, possession changes and passes)
# NP specific, as there is no event / attribute data for FDP

import pdb; #pdb.set_trace()
from warnings import warn
import numpy as np
from os.path import isfile, join, isdir
from os import listdir
import CSVexcerpt
import exportCSV

import safetyWarning

if __name__ == '__main__':
	process(rawDict,attributeDict,TeamAstring,TeamBstring)

	goals(rawDict,attributeDict,TeamAstring,TeamBstring,targetEvents)
	possession(rawDict,attributeDict,TeamAstring,TeamBstring,targetEvents)	
	passes(rawDict,attributeDict,TeamAstring,TeamBstring,possessionCharacteristics,targetEvents)
	full(rawDict,targetEvents)

def process(rawDict,attributeDict,TeamAstring,TeamBstring):
	targetEvents = {}
	########################################################################################
	####### Import existing events ######################################################
	########################################################################################

	## goals
	targetEvents = goals(rawDict,attributeDict,TeamAstring,TeamBstring,targetEvents)
	## possession / turnovers
	targetEvents = possession(rawDict,attributeDict,TeamAstring,TeamBstring,targetEvents)
	## Pass
	# NB: If available, possession should be computed before passes to incorporate number of consecutive passes
	# Could implement a safer procedure by starting with an empty dictionary and then adding the eventspecific dictionary after it's done the module (exporting an empty dictionary if info not available), then, the possession dictionary HAS to exist before going through
	targetEvents = passes(rawDict,attributeDict,TeamAstring,TeamBstring,targetEvents)
	targetEvents = full(rawDict,targetEvents)
	return targetEvents

def full(rawDict,targetEvents):
	targetEvents = {**targetEvents,'Full':[]}
	TsS = rawDict['Time']['TsS']
	targetEvents['Full'] = (float(min(TsS)),float(max(TsS)))
	return targetEvents

def goals(rawDict,attributeDict,TeamAstring,TeamBstring,targetEvents):
	targetEvents = {**targetEvents,'Goals':[]}

	if not 'Goal' in attributeDict.keys():
		# No Goal information, so return immediately
		return targetEvents

	count = 0
	goals = [i for i in attributeDict['Goal'] if i  != '' ]

	goalsOut = np.ones((len(goals),2),dtype='int')*-1

	for idx,i in enumerate(attributeDict['Goal']):
		if not i == '':
			goalsOut[count,1] = rawDict['Time']['TsS'][idx]
			if TeamAstring in i:
				goalsOut[count,0] = 0
				targetEvents['Goals'].append((float(rawDict['Time']['TsS'][idx]),TeamAstring))
			elif TeamBstring in i:
				goalsOut[count,0] = 1
				targetEvents['Goals'].append((float(rawDict['Time']['TsS'][idx]),TeamBstring))
			else:
				warn('\n\nCould not recognize team:\n<<%s>>' %i)
			count = count + 1

	return targetEvents

#################################################################
#################################################################

def possession(rawDict,attributeDict,TeamAstring,TeamBstring,targetEvents):
	targetEvents = {**targetEvents,'Possession':[],'Turnovers':[]} 

	if not 'Possession/Turnover' in attributeDict.keys():
		# No Possession/Turnover information, so return immediately
		return targetEvents

	possessionEvent = [(i,val) for i,val in enumerate(attributeDict['Possession/Turnover']) if val  != '' ]
	dt = []

	for idx,i in enumerate(possessionEvent):
		curFrame = i[0] # frame
		curTime = rawDict['Time']['TsS'][i[0]]
		curStatus = i[1]
		# Determine per event who has possession from that frame onward

		if not idx == len(possessionEvent)-1:
			endPossession = possessionEvent[idx+1][0]-1
		else:
			# CHECK THIS could be incorrect. When in doubt, double check:
			# As there is no record at the end saying that possession ended, I just assumed that the last frame was the end of teh current possession
			endPossession = None#[iq for iq,iv in enumerate(rawDict['Entity']['TeamID']) if iv == '' and rawDict['Time']['TsS'][iq] == max(rawDict['Time']['TsS'])]


		if 'Start' in curStatus:
			if TeamAstring in curStatus:
				# it's about Team A
				currentPossession = TeamAstring
			elif TeamBstring in curStatus:		
				# it's about Team B
				currentPossession = TeamBstring
			else:
				# exception added:
				if curStatus == 'Start A possession ':
					currentPossession = TeamAstring
				else:
					warn('\nCouldnt identify possession.\n%s' %curStatus)

		elif 'End' in curStatus:
			currentPossession = None

		elif 'Turnover' in curStatus:		
			dt.append(float(rawDict['Time']['TsS'][curFrame+1]-rawDict['Time']['TsS'][curFrame])) # a cheecky way to read frame rate from data
			if currentPossession == TeamAstring:
				currentPossession = TeamBstring
				targetEvents['Turnovers'].append((curTime,TeamAstring))
			elif currentPossession == TeamBstring:
				currentPossession = TeamAstring
				targetEvents['Turnovers'].append((curTime,TeamBstring))
			else:
				currentPossession = None

		else:
			# Based on next status 
			if possessionEvent[idx+1][1][0:8] == 'Turnover' and possessionEvent[idx+1][1][-len(TeamAstring)-1:-1] == TeamAstring:
				# The next turnover goes to TeamA, so:
				currentPossession = TeamBstring
				warn('\nIndirectly assessed event (based on next event):\n<<%s>>\nas <<%s>>' %(curStatus,currentPossession))

			elif possessionEvent[idx+1][1][0:8] == 'Turnover' and possessionEvent[idx+1][1][-len(TeamBstring)-1:-1] == TeamBstring:
				currentPossession = TeamAstring
				warn('\nIndirectly assessed event (based on next event):\n<<%s>>\nas <<%s>>' %(curStatus,currentPossession))			
			else:
				warn('\nCouldnt identify event:\n%s\n\n' %curStatus)
				currentPossession = None

		if curFrame == None:
			in1 = None
		else:
			in1 = float(rawDict['Time']['TsS'][curFrame])
		if endPossession == None:
			in2 = None
		else:
			in2 = float(rawDict['Time']['TsS'][endPossession])
		targetEvents['Possession'].append((in1,in2,currentPossession))								

	if dt == []:
		warn('\n!!!!\nExisting attributes seem to be missing.\nCouldnt find turnovers to estimate dt.\nOutput set as 999.')
		frameTime = 0.1
		overwriteOutput = True
	else:
		if round(sum(dt) / len(dt),7) == round(dt[0],7):
			# Safe to assume that:
			frameTime = dt[0]
		else:
			frameTime = round(sum(dt) / len(dt),7)
			warn('\nNot sure about frameTime. Check that:\n frameTime = %f' %frameTime)

	return targetEvents

#################################################################
#################################################################

def passes(rawDict,attributeDict,TeamAstring,TeamBstring,targetEvents):
	targetEvents = {**targetEvents,'Passes':[]}

	if not 'Pass' in attributeDict.keys():
		# No Pass information, so return immediately
		return targetEvents

	count = 0
	passes = [i for i in attributeDict['Pass'] if i  != '' ]

	for idx,i in enumerate(attributeDict['Pass']):
		if not i == '':
			if TeamAstring in i:
				targetEvents['Passes'].append((float(rawDict['Time']['TsS'][idx]),TeamAstring,None))				
			elif TeamBstring in i:
				targetEvents['Passes'].append((float(rawDict['Time']['TsS'][idx]),TeamBstring,None))								
			else:
				warn('\n\nCould not recognize team:\n<<%s>>' %i)
			if 'oal' in i:
				# ITS A GOAL NOT A PASS, so skip this file
				warn('\n!!!!!!!!!!!\nInconsistent data input: Goal was found in the passing column:\nEither improve code or clean up data.\n!!!!!!!!!!!')
				overwriteOutput = True
				break
			count = count + 1

	ind = 0
	if targetEvents['Possession'] != []:
		for idx,i in enumerate(targetEvents['Possession']):
			while targetEvents['Passes'][ind][0] >= i[0]: # after the start
				if targetEvents['Passes'][ind][0] <= i[1]: # before the end
					# Passing sequence is indicated by storing the nth possession per pass. This way, the number of consecutive passes can be easily deduced.
					targetEvents['Passes'][ind] = (targetEvents['Passes'][ind][0],targetEvents['Passes'][ind][1],idx)
					# current pass falls within current
					if len(targetEvents['Passes']) > ind+1:
						ind = ind + 1
					else:
						break
				else:
					# pass belongs in next section
					break
	else: # no possession info available
		if not 'Possession' in targetEvents.keys():
			warn('\nERROR: You should first import possession before importing passes (even if possesion is empty).')
		else:
			warn('\nWARNING: No possession information availabe: \nConsidering implementing a module that derives possession information from passes.')

	return targetEvents
#################################################################
#################################################################