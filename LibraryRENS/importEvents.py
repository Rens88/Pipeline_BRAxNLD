# 08-02-2018 Rens Meerhoff
# Script can be generically used to import information from eventsPanda. If the data is not organized similarly, it will simply export empty targetEvents. In the future, we can include LMP-based import of these kinds of data (or perhaps standardize the format earlier in the pipeline).


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
import pandas as pd
# from os.path import isfile, join, isdir
# from os import listdir
# import CSVexcerpt
# import exportCSV

import safetyWarning

if __name__ == '__main__':
	process(eventsPanda,TeamAstring,TeamBstring)

	# 1 --> Time of event
	# 2 --> Reference Team
	# 3 --> varies per event:
	
	goals(eventsPanda,TeamAstring,TeamBstring,targetEvents)
	# no 3

	possession(eventsPanda,TeamAstring,TeamBstring,targetEvents)	
	# 3 --> START time of the possession
	
	passes(eventsPanda,TeamAstring,TeamBstring,possessionCharacteristics,targetEvents)
	# 3 --> nth possession this pass was apart of (to determine number of consqutive passes)

	full(targetEvents)
	# 3 --> START time of the possession


def process(eventsPanda,TeamAstring,TeamBstring):
	targetEvents = {}
	########################################################################################
	####### Import existing events ######################################################
	########################################################################################

	## goals
	targetEvents = goals(eventsPanda,TeamAstring,TeamBstring,targetEvents)
	## possession / turnovers
	targetEvents = possession(eventsPanda,TeamAstring,TeamBstring,targetEvents)
	## Runs
	targetEvents = runs(eventsPanda,TeamAstring,TeamBstring,targetEvents)
	## Pass
	# NB: If available, possession should be computed before passes to incorporate number of consecutive passes
	# Could implement a safer procedure by starting with an empty dictionary and then adding the eventspecific dictionary after it's done the module (exporting an empty dictionary if info not available), then, the possession dictionary HAS to exist before going through
	targetEvents = passes(eventsPanda,TeamAstring,TeamBstring,targetEvents)
	targetEvents = full(eventsPanda,targetEvents)
	return targetEvents

def full(eventsPanda,targetEvents):
	targetEvents = {**targetEvents,'Full':[]}
	TsS = eventsPanda['Ts']
	targetEvents['Full'] = [(float(max(TsS)),None,float(min(TsS)))]
	return targetEvents

def goals(eventsPanda,TeamAstring,TeamBstring,targetEvents):
	targetEvents = {**targetEvents,'Goal':[]}

	if not 'Goal' in eventsPanda.keys():
		# No Goal information, so return immediately
		return targetEvents

	count = 0
	goals = [i for i in eventsPanda['Goal'] if i  != '' ]

	goalsOut = np.ones((len(goals),2),dtype='int')*-1

	for idx,i in enumerate(eventsPanda['Goal']):
		if not i == '' and pd.notnull(i):
			goalsOut[count,1] = eventsPanda['Ts'][idx]

			if TeamAstring in i:
				goalsOut[count,0] = 0
				targetEvents['Goal'].append((float(eventsPanda['Ts'][idx]),TeamAstring))
			elif TeamBstring in i:
				goalsOut[count,0] = 1
				targetEvents['Goal'].append((float(eventsPanda['Ts'][idx]),TeamBstring))
			else:
				warn('\n\nCould not recognize team:\n<<%s>>' %i)
			count = count + 1

	return targetEvents

#################################################################
#################################################################

def runs(eventsPanda,TeamAstring,TeamBstring,targetEvents):
	targetEvents = {**targetEvents,'Run':[]} 

	if not 'Run' in eventsPanda.keys():
		# No Run information, so return immediately
		return targetEvents

	runEvent = [(i,val) for i,val in enumerate(eventsPanda['Run']) if val  != '' and pd.notnull(val)]
	dt = []
	in1 = []
	in2 = []
	for idx,i in enumerate(runEvent):
		curFrame = i[0] # frame
		curTime = eventsPanda['Ts'][i[0]]
		curStatus = i[1]
		# Determine per event who has possession from that frame onward
		if curStatus[:3].lower() == 'run':
			in1 = float(eventsPanda['Ts'][curFrame])

		elif curStatus[:7].lower() == 'end run':
			in2 = float(eventsPanda['Ts'][curFrame])

		if in2 != []:
			if in1 == []:
				# Weird, found an end without a start.
				warn('\nWARNING: Found an end without a start. Ignored it. \nConsider checking the raw data.')
				in2 = []
			else:
				# Found a start and an end
				targetEvents['Run'].append((in1,None,in2))
				in1 = []
				in2 = []

	return targetEvents

def possession(eventsPanda,TeamAstring,TeamBstring,targetEvents):
	targetEvents = {**targetEvents,'Possession':[],'Turnovers':[]} 

	if not 'Possession/Turnover' in eventsPanda.keys():
		# No Possession/Turnover information, so return immediately
		return targetEvents

	possessionEvent = [(i,val) for i,val in enumerate(eventsPanda['Possession/Turnover']) if val  != '' and pd.notnull(val)]
	dt = []

	for idx,i in enumerate(possessionEvent):
		curFrame = i[0] # frame
		curTime = eventsPanda['Ts'][i[0]]
		curStatus = i[1]
		# Determine per event who has possession from that frame onward

		if not idx == len(possessionEvent)-1:
			endPossession = possessionEvent[idx+1][0]-1
		else:
			# CHECK THIS could be incorrect. When in doubt, double check:
			# As there is no record at the end saying that possession ended, I just assumed that the last frame was the end of teh current possession
			endPossession = None#[iq for iq,iv in enumerate(eventsPanda['Entity']['TeamID']) if iv == '' and eventsPanda['Ts'][iq] == max(eventsPanda['Ts'])]


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
			dt.append(float(eventsPanda['Ts'][curFrame+1]-eventsPanda['Ts'][curFrame])) # a cheecky way to read frame rate from data
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
			in1 = float(eventsPanda['Ts'][curFrame])
		if endPossession == None:
			in2 = None
		else:
			in2 = float(eventsPanda['Ts'][endPossession])
		targetEvents['Possession'].append((in2,currentPossession,in1))								

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

def passes(eventsPanda,TeamAstring,TeamBstring,targetEvents):
	targetEvents = {**targetEvents,'Passes':[]}

	if not 'Pass' in eventsPanda.keys():
		# No Pass information, so return immediately
		return targetEvents

	count = 0
	passes = [i for i in eventsPanda['Pass'] if i  != '' ]

	for idx,i in enumerate(eventsPanda['Pass']):
		if not i == '' and pd.notnull(i):
			if TeamAstring in i:
				targetEvents['Passes'].append((float(eventsPanda['Ts'][idx]),TeamAstring,None))				
			elif TeamBstring in i:
				targetEvents['Passes'].append((float(eventsPanda['Ts'][idx]),TeamBstring,None))								
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
			while targetEvents['Passes'][ind][0] >= i[2]: # after the start
				if targetEvents['Passes'][ind][0] <= i[0]: # before the end
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