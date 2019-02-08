# 06-12-2017 Rens Meerhoff
# Script to obtain game/trial-based results of existing events (goals, possession changes and passes)
# NP specific, as there is no event / attribute data for FDP


# TO DO: Exclude these sections with warnings. Data should be based on targetEvents (which is already proofed)

import pdb; #pdb.set_trace()
from warnings import warn
import numpy as np
import logging
from datetime import datetime
from os.path import isfile, join, exists, realpath, abspath, split,dirname, isdir, basename
# from os.path import isfile, join, isdir
# from os import listdir
# import CSVexcerpt
# import exportCSV

import safetyWarning

if __name__ == '__main__':

	goals(window,aggregateEvent,targetEvents,refTeam,othTeam,exportData,exportDataString,exportFullExplanation)
	turnovers(window,aggregateEvent,targetEvents,refTeam,othTeam,exportData,exportDataString,exportFullExplanation)
	possessions(window,aggregateEvent,targetEvents,refTeam,othTeam,exportData,exportDataString,exportFullExplanation)
	passes(window,aggregateEvent,targetEvents,refTeam,othTeam,exportData,exportDataString,exportFullExplanation)
	passes2(rowswithinrange,aggregateEvent,rawDict,attributeDict,refTeam,othTeam,exportData,exportDataString,exportFullExplanation,possessionCharacteristics,targetEvents)

	# reminder (can be deleted):
	# targetEvents = {'Goals':[],'Passes':[],'Turnovers':[],'Possession':[]}
def goals(window,aggregateEvent,targetEvents,refTeam,othTeam,exportData,exportDataString,exportFullExplanation):

	if targetEvents['Goal'] != []:
		goals = []
		for a in targetEvents['Goal']:
			eventInstant,eventID = a[:2]
			if eventInstant > window[0] and eventInstant <= window[1]:
				goals.append(eventID)

		goalCount = float(len(goals))
		goalCountA = float(sum([refTeam in i for i in goals]))
		goalCountB = float(sum([othTeam in i for i in goals]))
		goalsDelta = float(abs(goalCountA - goalCountB))

	else: # no 'goals' information
		# goalCount = None
		# goalCountA = None
		# goalCountB = None
		# goalsDelta = None
		return exportData,exportDataString,exportFullExplanation

	# To export:
	exportDataString.append('goalCount')
	exportData.append(goalCount)
	exportFullExplanation.append('Number of goals per %s.' %aggregateEvent)

	exportDataString.append('goalCount_ref')
	exportData.append(goalCountA)
	exportFullExplanation.append('Number of goals by %s per %s.' %(refTeam,aggregateEvent))

	exportDataString.append('goalCount_oth')
	exportData.append(goalCountB)
	exportFullExplanation.append('Number of goals by %s per %s.' %(othTeam,aggregateEvent))

	exportDataString.append('goalsDelta')
	exportData.append(goalsDelta)
	exportFullExplanation.append('Absolute difference between number of goals scored by %s and %s, per %s.' %(refTeam,othTeam,aggregateEvent))

	safetyWarning.checkLengthExport(exportData,exportDataString,exportFullExplanation)
	return exportData,exportDataString,exportFullExplanation

#################################################################
#################################################################

def turnovers(window,aggregateEvent,targetEvents,refTeam,othTeam,exportData,exportDataString,exportFullExplanation):
	turnoverCharacteristics = []

	if targetEvents['Turnovers'] != []:
		turnoverCharacteristics = []
		# if len(targetEvents['Turnovers'][0]) == 8:
		for a in targetEvents['Turnovers']:
			eventInstant,eventID = a[:2]

			if eventInstant > window[0] and eventInstant <= window[1]:
				turnoverCharacteristics.append(eventID)
		# else:
		# 	for eventInstant,eventID,a in targetEvents['Turnovers']:

		# 		if eventInstant > window[0] and eventInstant <= window[1]:
		# 			turnoverCharacteristics.append(eventID)

		# define outcome variables here
		turnoverCount = len(turnoverCharacteristics)
		turnoverCountA = len([i for i in turnoverCharacteristics if refTeam in i])
		turnoverCountB = len([i for i in turnoverCharacteristics if othTeam in i])
		turnoverDelta = abs(turnoverCountA - turnoverCountB)
	else: # no 'possession' information, outcome should be missing
		# turnoverCount = None
		# turnoverCountA = None
		# turnoverCountB = None
		# turnoverDelta = None
		return exportData,exportDataString,exportFullExplanation


	# # To export:'
	exportDataString.append('turnoverCount')
	exportData.append(turnoverCount)
	exportFullExplanation.append('Number of turnovers per %s.' %aggregateEvent)

	exportDataString.append('turnoverCount_ref')
	exportData.append(turnoverCountA)
	exportFullExplanation.append('Number of turnovers by %s per %s.' %(refTeam,aggregateEvent))

	exportDataString.append('turnoverCount_oth')
	exportData.append(turnoverCountB)
	exportFullExplanation.append('Number of turnovers by %s per %s.' %(othTeam,aggregateEvent))

	exportDataString.append('turnoverDelta')
	exportData.append(turnoverDelta)
	exportFullExplanation.append('Absolute difference between number of turnovers by %s and %s, per %s.' %(refTeam,othTeam,aggregateEvent))

	safetyWarning.checkLengthExport(exportData,exportDataString,exportFullExplanation)
	return exportData,exportDataString,exportFullExplanation

#################################################################
#################################################################

def possessions(window,aggregateEvent,targetEvents,refTeam,othTeam,exportData,exportDataString,exportFullExplanation):
	NoneAsZero = True
	if NoneAsZero:
		exportNone = 0
	else:
		exportNone = None

	possessionCharacteristics = []
	if targetEvents['Possession'] != []:
		possessionCharacteristics = []
		for a in targetEvents['Possession']:
			eventInstantEnd,eventID,eventInstantStart = a[:3]
			if eventInstantStart == None or eventInstantEnd == None:
				# No possession window defined. Skip this event.
				# NB: This might make all the 'None' possession statistics obsolete.
				continue
			if eventInstantStart >= window[0] and eventInstantEnd <= window[1]:
				possDur = eventInstantEnd - eventInstantStart # current possession duration
				possessionCharacteristics.append((possDur,eventID))

		# define outcome variables here
		possessionCount = sum([i[1] != None for i in possessionCharacteristics])
		possessionCountA = sum([i[1] == refTeam for i in possessionCharacteristics])
		possessionCountB = sum([i[1] == othTeam for i in possessionCharacteristics])
		possessionCountDelta = abs(possessionCountA-possessionCountB)
		possessionCountNone = sum([i[1] == None for i in possessionCharacteristics])

		possessionDurationSum = sum(i[0] for i in possessionCharacteristics)
		possessionDurationSumA = sum(i[0] for i in possessionCharacteristics if i[1] == refTeam)
		possessionDurationSumB = sum(i[0] for i in possessionCharacteristics if i[1] == othTeam)
		possessionDurationSumNone = sum(i[0] for i in possessionCharacteristics if i[1] == None)

		if possessionCount != 0:
			possessionDurationAvg = possessionDurationSum / possessionCount
			possessionDurationStd = sum(abs(i[0]-possessionDurationAvg) for i in possessionCharacteristics if i[1] != None) / possessionCount
		else:

			possessionDurationAvg = exportNone
			possessionDurationStd = exportNone

		if possessionCountA != 0:
			possessionDurationAvgA = possessionDurationSumA / possessionCountA
			possessionDurationStdA = sum(abs(i[0]-possessionDurationAvgA) for i in possessionCharacteristics if i[1] == refTeam) / possessionCountA
		else:
			possessionDurationAvgA = exportNone
			possessionDurationStdA = exportNone

		if possessionCountB != 0:
			possessionDurationAvgB = possessionDurationSumB / possessionCountB
			possessionDurationStdB = sum(abs(i[0]-possessionDurationAvgB) for i in possessionCharacteristics if i[1] == othTeam) / possessionCountB
		else:
			possessionDurationAvgB = exportNone
			possessionDurationStdB = exportNone		

		if possessionCountNone != 0:
			possessionDurationAvgNone = possessionDurationSumNone / possessionCountNone
			possessionDurationStdNone = sum(abs(i[0]-possessionDurationAvgNone) for i in possessionCharacteristics if i[1] == None) / possessionCountNone
		else:
			possessionDurationAvgNone = exportNone
			possessionDurationStdNone = exportNone

	else: # no 'possession' information
		# define missing outcome variables here
		# possessionCount = None
		# possessionCountA = None
		# possessionCountB = None
		# possessionCountDelta = None
		# possessionCountNone = None

		# possessionDurationSum = None
		# possessionDurationSumA = None
		# possessionDurationSumB = None
		# possessionDurationSumNone = None

		# possessionDurationAvg = None
		# possessionDurationAvgA = None
		# possessionDurationAvgB = None
		# possessionDurationAvgNone = None

		# possessionDurationStd = None
		# possessionDurationStdA = None
		# possessionDurationStdB = None
		# possessionDurationStdNone = None
		return exportData,exportDataString,exportFullExplanation


	# # To export:'
	# possessionCount in occurence (or frequency) in the whole trial
	exportDataString.append('possessionCount')
	exportData.append(possessionCount)
	exportFullExplanation.append('Number of full possessions per %s. NB: <full> refers to from the start until the end of a possession.' %aggregateEvent)

	exportDataString.append('possessionCount_ref')
	exportData.append(possessionCountA)
	exportFullExplanation.append('Number full possession %s had per %s. NB: <full> refers to from the start until the end of a possession.' %(refTeam,aggregateEvent))

	exportDataString.append('possessionCount_oth')
	exportData.append(possessionCountB)
	exportFullExplanation.append('Number full possession %s had per %s. NB: <full> refers to from the start until the end of a possession.' %(othTeam,aggregateEvent))

	exportDataString.append('possessionCountDelta')	
	exportData.append(possessionCountDelta)
	exportFullExplanation.append('Absolute difference between number of full possessions by %s and %s, per %s. NB: <full> refers to from the start until the end of a possession.' %(refTeam,othTeam,aggregateEvent))

	exportDataString.append('possessionCountNone')
	exportData.append(possessionCountNone)
	exportFullExplanation.append('Number of times no team had possession per %s.' %aggregateEvent)
	
	# possessionDuration (in seconds) --> sum,std,avg,
	exportDataString.append('possessionDurationSum')
	exportData.append(possessionDurationSum)
	exportFullExplanation.append('Total duration (s) of full possessions by %s and %s, per %s. NB: <full> refers to from the start until the end of a possession.' %(refTeam,othTeam,aggregateEvent))

	exportDataString.append('possessionDurationSum_ref')
	exportData.append(possessionDurationSumA)
	exportFullExplanation.append('Total duration (s) of full possessions per %s by %s. NB: <full> refers to from the start until the end of a possession.' %(aggregateEvent,refTeam))

	exportDataString.append('possessionDurationSum_oth')
	exportData.append(possessionDurationSumB)
	exportFullExplanation.append('Total duration (s) of full possessions per %s by %s. NB: <full> refers to from the start until the end of a possession.' %(aggregateEvent,othTeam))

	exportDataString.append('possessionDurationSumNone')
	exportData.append(possessionDurationSumNone)
	exportFullExplanation.append('Total duration (s) of no possession per %s. NB: <full> refers to from the start until the end of a possession.' %aggregateEvent)

	# Average duration of a possession (until turnover / ball loss / end game)
	exportDataString.append('possessionDurationAvg')
	exportData.append(possessionDurationAvg)
	exportFullExplanation.append('Average duration of a full possession per %s by %s and %s. NB: <full> refers to from the start until the end of a possession.' %(aggregateEvent,refTeam,othTeam))

	exportDataString.append('possessionDurationAvg_ref')
	exportData.append(possessionDurationAvgA)
	exportFullExplanation.append('Average duration of a full possession per %s by %s. NB: <full> refers to from the start until the end of a possession.' %(aggregateEvent,refTeam))

	exportDataString.append('possessionDurationAvg_oth')
	exportData.append(possessionDurationAvgB)
	exportFullExplanation.append('Average duration of a full possession per %s by %s. NB: <full> refers to from the start until the end of a possession.' %(aggregateEvent,othTeam))

	exportDataString.append('possessionDurationAvgNone')
	exportData.append(possessionDurationAvgNone)
	exportFullExplanation.append('Average duration (s) of no possession per %s.' %aggregateEvent)

	# Standard deviation of a possession
	exportDataString.append('possessionDurationStd')
	exportData.append(possessionDurationStd)
	exportFullExplanation.append('Standard deviation of the duration of a full possession per %s by %s and %s. NB: <full> refers to from the start until the end of a possession.' %(aggregateEvent,refTeam,othTeam))
	
	exportDataString.append('possessionDurationStd_ref')
	exportData.append(possessionDurationStdA)
	exportFullExplanation.append('Standard deviation of the duration of a full possession per %s by %s. NB: <full> refers to from the start until the end of a possession.' %(aggregateEvent,refTeam))

	exportDataString.append('possessionDurationStd_oth')
	exportData.append(possessionDurationStdB)
	exportFullExplanation.append('Standard deviation of the duration of a full possession per %s by %s. NB: <full> refers to from the start until the end of a possession.' %(aggregateEvent,othTeam))

	exportDataString.append('possessionDurationStdNone')
	exportData.append(possessionDurationStdNone) 
	exportFullExplanation.append('Standard deviation of the duration (s) of no possession per %s.' %aggregateEvent)

	safetyWarning.checkLengthExport(exportData,exportDataString,exportFullExplanation)

	return exportData,exportDataString,exportFullExplanation

#################################################################
#################################################################

def passes(window,aggregateEvent,targetEvents,refTeam,othTeam,exportData,exportDataString,exportFullExplanation):
	NoneAsZero = True
	if NoneAsZero:
		exportNone = 0
	else:
		exportNone = None

	# window = (80,90)
	if targetEvents['Passes'] != []:
		passes = []
		consecutivePasses = []
		# if len(targetEvents['Passes'][0]) == 6:
		for a in targetEvents['Passes']:
			eventInstant,eventID,consecutiveEvents = a[:3]

			if eventInstant > window[0] and eventInstant <= window[1]:
				passes.append(eventID)
				consecutivePasses.append(consecutiveEvents)
		# else:
		# 	for eventInstant,eventID,consecutiveEvents,a in targetEvents['Passes']:
		# 		if eventInstant > window[0] and eventInstant <= window[1]:
		# 			passes.append(eventID)
		# 			consecutivePasses.append(consecutiveEvents)

		passCount = float(len(passes))
		passCountA = float(sum([refTeam in i for i in passes]))
		passCountB = float(sum([othTeam in i for i in passes]))
		passDelta = float(abs(passCountA - passCountB))

	else: # no 'passes' information
		# passCount = None
		# passCountA = None
		# passCountB = None
		# passDelta = None
		return exportData,exportDataString,exportFullExplanation

	if targetEvents['Passes'] != [] and targetEvents['Possession'] != []:
		consecutivePassesPerPossession = []
		consecutivePassesPerPossessionA = []
		consecutivePassesPerPossessionB = []
		tmp = 1
		for idx,val in enumerate(consecutivePasses):
			consecutivePassesPerPossession.append(tmp)
			if passes[idx] == refTeam:
				consecutivePassesPerPossessionA.append(tmp)
			elif passes[idx] == othTeam:
				consecutivePassesPerPossessionB.append(tmp)
			else:
				warn('\nERROR: Could not identify whose possession: <<%s>>' %passes[idx])

			if idx == len(consecutivePasses)-1: # last one
				break
			if val != consecutivePasses[idx+1]: # new possession
				tmp = 1	
			else:
				tmp = tmp + 1

		if consecutivePassesPerPossession != []:
			maxConsecutivePasses = max(consecutivePassesPerPossession)
			avgConsecutivePasses = sum(consecutivePassesPerPossession) / len(consecutivePassesPerPossession)
			stdConsecutivePasses = sum([abs(i - avgConsecutivePasses) for i in consecutivePassesPerPossession]) / len(consecutivePassesPerPossession)
		else:
			maxConsecutivePasses = exportNone
			avgConsecutivePasses = exportNone
			stdConsecutivePasses = exportNone

		if consecutivePassesPerPossessionA != []:
			maxConsecutivePassesA = max(consecutivePassesPerPossessionA)
			avgConsecutivePassesA = sum(consecutivePassesPerPossessionA) / len(consecutivePassesPerPossessionA)
			stdConsecutivePassesA = sum([abs(i - avgConsecutivePassesA) for i in consecutivePassesPerPossessionA]) / len(consecutivePassesPerPossessionA)
		else:
			maxConsecutivePassesA = exportNone
			avgConsecutivePassesA = exportNone
			stdConsecutivePassesA = exportNone

		if consecutivePassesPerPossessionB != []:
			maxConsecutivePassesB = max(consecutivePassesPerPossessionB)
			avgConsecutivePassesB = sum(consecutivePassesPerPossessionB) / len(consecutivePassesPerPossessionB)
			stdConsecutivePassesB = sum([abs(i - avgConsecutivePassesB) for i in consecutivePassesPerPossessionB]) / len(consecutivePassesPerPossessionB)
		else:
			maxConsecutivePassesB = exportNone
			avgConsecutivePassesB = exportNone
			stdConsecutivePassesB = exportNone		
	else:
		maxConsecutivePasses = exportNone
		maxConsecutivePassesA = exportNone
		maxConsecutivePassesB = exportNone
		avgConsecutivePasses = exportNone
		avgConsecutivePassesA = exportNone
		avgConsecutivePassesB = exportNone
		stdConsecutivePasses = exportNone
		stdConsecutivePassesA = exportNone
		stdConsecutivePassesB = exportNone

	# To export:
	# Pass counts
	exportDataString.append('passCount')
	exportData.append(passCount)
	exportFullExplanation.append('Number of passes per %s.' %aggregateEvent)

	exportDataString.append('passCount_ref')
	exportData.append(passCountA)
	exportFullExplanation.append('Number of passes by %s per %s.' %(refTeam,aggregateEvent))

	exportDataString.append('passCount_oth')
	exportData.append(passCountB)
	exportFullExplanation.append('Number of passes by %s per %s.' %(othTeam,aggregateEvent))

	exportDataString.append('passDelta')
	exportData.append(passDelta)
	exportFullExplanation.append('Absolute difference between number of passes by %s and %s.' %(refTeam,othTeam))


	# Consecutive passes
	# Counts
	exportDataString.append('maxConsecutivePasses')
	exportData.append(maxConsecutivePasses)
	exportFullExplanation.append('Maximum number of consecutive passes per %s.' %aggregateEvent)

	exportDataString.append('maxConsecutivePasses_ref')
	exportData.append(maxConsecutivePassesA)
	exportFullExplanation.append('Maximum number of consecutive passes (during a possession) by %s per %s.' %(refTeam,aggregateEvent))

	exportDataString.append('maxConsecutivePasses_oth')
	exportData.append(maxConsecutivePassesB)
	exportFullExplanation.append('Maximum number of consecutive passes (during a possession) by %s per %s.' %(othTeam,aggregateEvent))

	# Average per event
	exportDataString.append('avgConsecutivePasses')
	exportData.append(avgConsecutivePasses)
	exportFullExplanation.append('Average number of consecutive passes (during a possession) per %s.' %aggregateEvent)

	exportDataString.append('avgConsecutivePasses_ref')
	exportData.append(avgConsecutivePassesA)
	exportFullExplanation.append('Average number of consecutive passes (during a possession) by %s per %s.' %(refTeam,aggregateEvent))

	exportDataString.append('avgConsecutivePasses_oth')
	exportData.append(avgConsecutivePassesB)
	exportFullExplanation.append('Average number of consecutive passes (during a possession) by %s per %s.' %(othTeam,aggregateEvent))

	# Std per event
	exportDataString.append('stdConsecutivePasses')
	exportData.append(stdConsecutivePasses)
	exportFullExplanation.append('Standard deviation of number of consecutive passes (during a possession) per %s.' %aggregateEvent)

	exportDataString.append('stdConsecutivePasses_ref')
	exportData.append(stdConsecutivePassesA)
	exportFullExplanation.append('Standard deviation of number of consecutive passes (during a possession) by %s per %s.' %(refTeam,aggregateEvent))

	exportDataString.append('stdConsecutivePasses_oth')
	exportData.append(stdConsecutivePasses)
	exportFullExplanation.append('Standard deviation of number of consecutive passes (during a possession) by %s per %s.' %(othTeam,aggregateEvent))

	safetyWarning.checkLengthExport(exportData,exportDataString,exportFullExplanation)
	return exportData,exportDataString,exportFullExplanation