# 06-12-2017 Rens Meerhoff
# Script to obtain game/trial-based results of existing events (goals, possession changes and passes)
# NP specific, as there is no event / attribute data for FDP


# TO DO: Exclude these sections with warnings. Data should be based on targetEvents (which is already proofed)

import pdb; #pdb.set_trace()
from warnings import warn
import numpy as np
from os.path import isfile, join, isdir
from os import listdir
import CSVexcerpt
import exportCSV

import safetyWarning

if __name__ == '__main__':

	goals(window,aggregateEvent,targetEvents,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation)
	turnovers(window,aggregateEvent,targetEvents,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation)
	possessions(window,aggregateEvent,targetEvents,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation)
	passes(window,aggregateEvent,targetEvents,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation)
	passes2(rowswithinrange,aggregateEvent,rawDict,attributeDict,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation,possessionCharacteristics,targetEvents)

	# reminder (can be deleted):
	# targetEvents = {'Goals':[],'Passes':[],'Turnovers':[],'Possession':[]}
def goals(window,aggregateEvent,targetEvents,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation):

	if targetEvents['Goals'] != []:
		goals = []
		for eventInstant,eventID in targetEvents['Goals']:
			if eventInstant > window[0] and eventInstant <= window[1]:
				goals.append(eventID)

		goalCount = float(len(goals))
		goalCountA = float(sum([TeamAstring in i for i in goals]))
		goalCountB = float(sum([TeamBstring in i for i in goals]))
		goalsDelta = float(abs(goalCountA - goalCountB))

	else: # no 'goals' information
		goalCount = None
		goalCountA = None
		goalCountB = None
		goalsDelta = None
	# To export:
	exportDataString.append('goalCount')
	exportData.append(goalCount)
	exportFullExplanation.append('Number of goals per %s.' %aggregateEvent)

	exportDataString.append('goalCountA')
	exportData.append(goalCountA)
	exportFullExplanation.append('Number of goals by %s per %s.' %(TeamAstring,aggregateEvent))

	exportDataString.append('goalCountB')
	exportData.append(goalCountB)
	exportFullExplanation.append('Number of goals by %s per %s.' %(TeamBstring,aggregateEvent))

	exportDataString.append('goalsDelta')
	exportData.append(goalsDelta)
	exportFullExplanation.append('Absolute difference between number of goals scored by %s and %s, per %s.' %(TeamAstring,TeamBstring,aggregateEvent))

	safetyWarning.checkLengthExport(exportData,exportDataString,exportFullExplanation)
	return exportData,exportDataString,exportFullExplanation

#################################################################
#################################################################

def turnovers(window,aggregateEvent,targetEvents,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation):
	turnoverCharacteristics = []

	if targetEvents['Turnovers'] != []:
		turnoverCharacteristics = []
		for eventInstant,eventID in targetEvents['Turnovers']:

			if eventInstant > window[0] and eventInstant <= window[1]:
				turnoverCharacteristics.append(eventID)

		# define outcome variables here
		turnoverCount = len(turnoverCharacteristics)
		turnoverCountA = len([i for i in turnoverCharacteristics if TeamAstring in i])
		turnoverCountB = len([i for i in turnoverCharacteristics if TeamBstring in i])
		turnoverDelta = abs(turnoverCountA - turnoverCountB)
	else: # no 'possession' information, outcome should be missing
		turnoverCount = None
		turnoverCountA = None
		turnoverCountB = None
		turnoverDelta = None

	# # To export:'
	exportDataString.append('turnoverCount')
	exportData.append(turnoverCount)
	exportFullExplanation.append('Number of turnovers per %s.' %aggregateEvent)

	exportDataString.append('turnoverCountA')
	exportData.append(turnoverCountA)
	exportFullExplanation.append('Number of turnovers by %s per %s.' %(TeamAstring,aggregateEvent))

	exportDataString.append('turnoverCountB')
	exportData.append(turnoverCountB)
	exportFullExplanation.append('Number of turnovers by %s per %s.' %(TeamBstring,aggregateEvent))

	exportDataString.append('turnoverDelta')
	exportData.append(turnoverDelta)
	exportFullExplanation.append('Absolute difference between number of turnovers by %s and %s, per %s.' %(TeamAstring,TeamBstring,aggregateEvent))

	safetyWarning.checkLengthExport(exportData,exportDataString,exportFullExplanation)
	return exportData,exportDataString,exportFullExplanation

#################################################################
#################################################################

def possessions(window,aggregateEvent,targetEvents,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation):
	possessionCharacteristics = []
	if targetEvents['Possession'] != []:
		possessionCharacteristics = []
		for eventInstantStart,eventInstantEnd,eventID in targetEvents['Possession']:
			if eventInstantStart == None or eventInstantEnd == None:
				# No possession window defined. Skip this event.
				# NB: This might make all the 'None' possession statistics obsolete.
				continue
			if eventInstantStart >= window[0] and eventInstantEnd <= window[1]:
				possDur = eventInstantEnd - eventInstantStart # current possession duration
				possessionCharacteristics.append((possDur,eventID))

		# define outcome variables here
		possessionCount = sum([i[1] != None for i in possessionCharacteristics])
		possessionCountA = sum([i[1] == TeamAstring for i in possessionCharacteristics])
		possessionCountB = sum([i[1] == TeamBstring for i in possessionCharacteristics])
		possessionCountDelta = abs(possessionCountA-possessionCountB)
		possessionCountNone = sum([i[1] == None for i in possessionCharacteristics])

		possessionDurationSum = sum(i[0] for i in possessionCharacteristics)
		possessionDurationSumA = sum(i[0] for i in possessionCharacteristics if i[1] == TeamAstring)
		possessionDurationSumB = sum(i[0] for i in possessionCharacteristics if i[1] == TeamBstring)
		possessionDurationSumNone = sum(i[0] for i in possessionCharacteristics if i[1] == None)

		if possessionCount != 0:
			possessionDurationAvg = possessionDurationSum / possessionCount
			possessionDurationStd = sum(abs(i[0]-possessionDurationAvg) for i in possessionCharacteristics if i[1] != None) / possessionCount
		else:
			possessionDurationAvg = None
			possessionDurationStd = None

		if possessionCountA != 0:
			possessionDurationAvgA = possessionDurationSumA / possessionCountA
			possessionDurationStdA = sum(abs(i[0]-possessionDurationAvgA) for i in possessionCharacteristics if i[1] == TeamAstring) / possessionCountA
		else:
			possessionDurationAvgA = None
			possessionDurationStdA = None

		if possessionCountB != 0:
			possessionDurationAvgB = possessionDurationSumB / possessionCountB
			possessionDurationStdB = sum(abs(i[0]-possessionDurationAvgB) for i in possessionCharacteristics if i[1] == TeamBstring) / possessionCountB
		else:
			possessionDurationAvgB = None
			possessionDurationStdB = None		

		if possessionCountNone != 0:
			possessionDurationAvgNone = possessionDurationSumNone / possessionCountNone
			possessionDurationStdNone = sum(abs(i[0]-possessionDurationAvgNone) for i in possessionCharacteristics if i[1] == None) / possessionCountNone
		else:
			possessionDurationAvgNone = None
			possessionDurationStdNone = None

	else: # no 'possession' information
		# define missing outcome variables here
		possessionCount = None
		possessionCountA = None
		possessionCountB = None
		possessionCountDelta = None
		possessionCountNone = None

		possessionDurationSum = None
		possessionDurationSumA = None
		possessionDurationSumB = None
		possessionDurationSumNone = None

		possessionDurationAvg = None
		possessionDurationAvgA = None
		possessionDurationAvgB = None
		possessionDurationAvgNone = None

		possessionDurationStd = None
		possessionDurationStdA = None
		possessionDurationStdB = None
		possessionDurationStdNone = None

	# # To export:'
	# possessionCount in occurence (or frequency) in the whole trial
	exportDataString.append('possessionCount')
	exportData.append(possessionCount)
	exportFullExplanation.append('Number of full possessions per %s. NB: <full> refers to from the start until the end of a possession.' %aggregateEvent)

	exportDataString.append('possessionCountA')
	exportData.append(possessionCountA)
	exportFullExplanation.append('Number full possession %s had per %s. NB: <full> refers to from the start until the end of a possession.' %(TeamAstring,aggregateEvent))

	exportDataString.append('possessionCountB')
	exportData.append(possessionCountB)
	exportFullExplanation.append('Number full possession %s had per %s. NB: <full> refers to from the start until the end of a possession.' %(TeamBstring,aggregateEvent))

	exportDataString.append('possessionCountDelta')	
	exportData.append(possessionCountDelta)
	exportFullExplanation.append('Absolute difference between number of full possessions by %s and %s, per %s. NB: <full> refers to from the start until the end of a possession.' %(TeamAstring,TeamBstring,aggregateEvent))

	exportDataString.append('possessionCountNone')
	exportData.append(possessionCountNone)
	exportFullExplanation.append('Number of times no team had possession per %s.' %aggregateEvent)
	
	# possessionDuration (in seconds) --> sum,std,avg,
	exportDataString.append('possessionDurationSum')
	exportData.append(possessionDurationSum)
	exportFullExplanation.append('Total duration (s) of full possessions by %s and %s, per %s. NB: <full> refers to from the start until the end of a possession.' %(TeamAstring,TeamBstring,aggregateEvent))

	exportDataString.append('possessionDurationSumA')
	exportData.append(possessionDurationSumA)
	exportFullExplanation.append('Total duration (s) of full possessions per %s by %s. NB: <full> refers to from the start until the end of a possession.' %(aggregateEvent,TeamAstring))

	exportDataString.append('possessionDurationSumB')
	exportData.append(possessionDurationSumB)
	exportFullExplanation.append('Total duration (s) of full possessions per %s by %s. NB: <full> refers to from the start until the end of a possession.' %(aggregateEvent,TeamBstring))

	exportDataString.append('possessionDurationSumNone')
	exportData.append(possessionDurationSumNone)
	exportFullExplanation.append('Total duration (s) of no possession per %s. NB: <full> refers to from the start until the end of a possession.' %aggregateEvent)

	# Average duration of a possession (until turnover / ball loss / end game)
	exportDataString.append('possessionDurationAvg')
	exportData.append(possessionDurationAvg)
	exportFullExplanation.append('Average duration of a full possession per %s by %s and %s. NB: <full> refers to from the start until the end of a possession.' %(aggregateEvent,TeamAstring,TeamBstring))

	exportDataString.append('possessionDurationAvgA')
	exportData.append(possessionDurationAvgA)
	exportFullExplanation.append('Average duration of a full possession per %s by %s. NB: <full> refers to from the start until the end of a possession.' %(aggregateEvent,TeamAstring))

	exportDataString.append('possessionDurationAvgB')
	exportData.append(possessionDurationAvgB)
	exportFullExplanation.append('Average duration of a full possession per %s by %s. NB: <full> refers to from the start until the end of a possession.' %(aggregateEvent,TeamBstring))

	exportDataString.append('possessionDurationAvgNone')
	exportData.append(possessionDurationAvgNone)
	exportFullExplanation.append('Average duration (s) of no possession per %s.' %aggregateEvent)

	# Standard deviation of a possession
	exportDataString.append('possessionDurationStd')
	exportData.append(possessionDurationStd)
	exportFullExplanation.append('Standard deviation of the duration of a full possession per %s by %s and %s. NB: <full> refers to from the start until the end of a possession.' %(aggregateEvent,TeamAstring,TeamBstring))
	
	exportDataString.append('possessionDurationStdA')
	exportData.append(possessionDurationStdA)
	exportFullExplanation.append('Standard deviation of the duration of a full possession per %s by %s. NB: <full> refers to from the start until the end of a possession.' %(aggregateEvent,TeamAstring))

	exportDataString.append('possessionDurationStdB')
	exportData.append(possessionDurationStdB)
	exportFullExplanation.append('Standard deviation of the duration of a full possession per %s by %s. NB: <full> refers to from the start until the end of a possession.' %(aggregateEvent,TeamBstring))

	exportDataString.append('possessionDurationStdNone')
	exportData.append(possessionDurationStdNone) 
	exportFullExplanation.append('Standard deviation of the duration (s) of no possession per %s.' %aggregateEvent)

	safetyWarning.checkLengthExport(exportData,exportDataString,exportFullExplanation)

	return exportData,exportDataString,exportFullExplanation

#################################################################
#################################################################

def passes(window,aggregateEvent,targetEvents,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation):

	# window = (80,90)
	if targetEvents['Passes'] != []:
		passes = []
		consecutivePasses = []
		for eventInstant,eventID,consecutiveEvents in targetEvents['Passes']:
			if eventInstant > window[0] and eventInstant <= window[1]:
				passes.append(eventID)
				consecutivePasses.append(consecutiveEvents)

		passCount = float(len(passes))
		passCountA = float(sum([TeamAstring in i for i in passes]))
		passCountB = float(sum([TeamBstring in i for i in passes]))
		passDelta = float(abs(passCountA - passCountB))

	else: # no 'passes' information
		passCount = None
		passCountA = None
		passCountB = None
		passDelta = None

	if targetEvents['Passes'] != [] and targetEvents['Possession'] != []:
		consecutivePassesPerPossession = []
		consecutivePassesPerPossessionA = []
		consecutivePassesPerPossessionB = []
		tmp = 0
		for idx,val in enumerate(consecutivePasses):
			consecutivePassesPerPossession.append(tmp)
			if passes[idx] == TeamAstring:
				consecutivePassesPerPossessionA.append(tmp)
			elif passes[idx] == TeamBstring:
				consecutivePassesPerPossessionB.append(tmp)
			else:
				warn('\nERROR: Could not identify whose possession: <<%s>>' %passes[idx])

			if idx == len(consecutivePasses)-1: # last one
				break
			if val != consecutivePasses[idx+1]: # new possession
				tmp = 0	
			else:
				tmp = tmp + 1

		if consecutivePassesPerPossession != []:
			maxConsecutivePasses = max(consecutivePassesPerPossession)
			avgConsecutivePasses = sum(consecutivePassesPerPossession) / len(consecutivePassesPerPossession)
			stdConsecutivePasses = sum([abs(i - avgConsecutivePasses) for i in consecutivePassesPerPossession]) / len(consecutivePassesPerPossession)
		else:
			maxConsecutivePasses = None
			avgConsecutivePasses = None
			stdConsecutivePasses = None

		if consecutivePassesPerPossessionA != []:
			maxConsecutivePassesA = max(consecutivePassesPerPossessionA)
			avgConsecutivePassesA = sum(consecutivePassesPerPossessionA) / len(consecutivePassesPerPossessionA)
			stdConsecutivePassesA = sum([abs(i - avgConsecutivePassesA) for i in consecutivePassesPerPossessionA]) / len(consecutivePassesPerPossessionA)
		else:
			maxConsecutivePassesA = None
			avgConsecutivePassesA = None
			stdConsecutivePassesA = None

		if consecutivePassesPerPossessionB != []:
			maxConsecutivePassesB = max(consecutivePassesPerPossessionB)
			avgConsecutivePassesB = sum(consecutivePassesPerPossessionB) / len(consecutivePassesPerPossessionB)
			stdConsecutivePassesB = sum([abs(i - avgConsecutivePassesB) for i in consecutivePassesPerPossessionB]) / len(consecutivePassesPerPossessionB)
		else:
			maxConsecutivePassesB = None
			avgConsecutivePassesB = None
			stdConsecutivePassesB = None		
	else:
		maxConsecutivePasses = None
		maxConsecutivePassesA = None
		maxConsecutivePassesB = None
		avgConsecutivePasses = None
		avgConsecutivePassesA = None
		avgConsecutivePassesB = None
		stdConsecutivePasses = None
		stdConsecutivePassesA = None
		stdConsecutivePassesB = None

	# To export:
	# Pass counts
	exportDataString.append('passCount')
	exportData.append(passCount)
	exportFullExplanation.append('Number of passes per %s.' %aggregateEvent)

	exportDataString.append('passCountA')
	exportData.append(passCountA)
	exportFullExplanation.append('Number of passes by %s per %s.' %(TeamAstring,aggregateEvent))

	exportDataString.append('passCountB')
	exportData.append(passCountB)
	exportFullExplanation.append('Number of passes by %s per %s.' %(TeamBstring,aggregateEvent))

	exportDataString.append('passDelta')
	exportData.append(passDelta)
	exportFullExplanation.append('Absolute difference between number of passes by %s and %s.' %(TeamAstring,TeamBstring))


	# Consecutive passes
	# Counts
	exportDataString.append('maxConsecutivePasses')
	exportData.append(maxConsecutivePasses)
	exportFullExplanation.append('Maximum number of consecutive passes per %s.' %aggregateEvent)

	exportDataString.append('maxConsecutivePassesA')
	exportData.append(maxConsecutivePassesA)
	exportFullExplanation.append('Maximum number of consecutive passes (during a possession) by %s per %s.' %(TeamAstring,aggregateEvent))

	exportDataString.append('maxConsecutivePassesB')
	exportData.append(maxConsecutivePassesB)
	exportFullExplanation.append('Maximum number of consecutive passes (during a possession) by %s per %s.' %(TeamBstring,aggregateEvent))

	# Average per event
	exportDataString.append('avgConsecutivePasses')
	exportData.append(avgConsecutivePasses)
	exportFullExplanation.append('Average number of consecutive passes (during a possession) per %s.' %aggregateEvent)

	exportDataString.append('avgConsecutivePassesA')
	exportData.append(avgConsecutivePassesA)
	exportFullExplanation.append('Average number of consecutive passes (during a possession) by %s per %s.' %(TeamAstring,aggregateEvent))

	exportDataString.append('avgConsecutivePassesB')
	exportData.append(avgConsecutivePassesB)
	exportFullExplanation.append('Average number of consecutive passes (during a possession) by %s per %s.' %(TeamBstring,aggregateEvent))

	# Std per event
	exportDataString.append('stdConsecutivePasses')
	exportData.append(stdConsecutivePasses)
	exportFullExplanation.append('Standard deviation of number of consecutive passes (during a possession) per %s.' %aggregateEvent)

	exportDataString.append('stdConsecutivePassesA')
	exportData.append(stdConsecutivePassesA)
	exportFullExplanation.append('Standard deviation of number of consecutive passes (during a possession) by %s per %s.' %(TeamAstring,aggregateEvent))

	exportDataString.append('stdConsecutivePassesB')
	exportData.append(stdConsecutivePasses)
	exportFullExplanation.append('Standard deviation of number of consecutive passes (during a possession) by %s per %s.' %(TeamBstring,aggregateEvent))

	safetyWarning.checkLengthExport(exportData,exportDataString,exportFullExplanation)
	return exportData,exportDataString,exportFullExplanation

def passes2(rowswithinrange,aggregateEvent,possessionCharacteristics,rawDict,attributeDict,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation):
	
	i = [idx for idx,val in enumerate(exportDataString) if val == 'possessionCount']
	possessionCount = exportData[i[0]]
	i = [idx for idx,val in enumerate(exportDataString) if val == 'possessionCountA']
	possessionCountA = exportData[i[0]]
	i = [idx for idx,val in enumerate(exportDataString) if val == 'possessionCountB']
	possessionCountB = exportData[i[0]]


	## TO DO: CHANGE this to not use attributeDict['Pass']
	passInfo = [attributeDict['Pass'][j] for j in rowswithinrange]
	passes = [i for i in passInfo if i != '']

	passOut = np.ones((len(passes),2),dtype='int')*-1
	count = 0
	for idx,i in enumerate(passInfo):
		if not i == '':
			passOut[count,1] = idx
			if TeamAstring in i:
				passOut[count,0] = 0
			elif TeamBstring in i:
				passOut[count,0] = 1
			else:
				warn('\n\nCould not recognize team:\n<<%s>>' %i)
			if 'oal' in i:
				# ITS A GOAL NOT A PASS, so skip this file
				warn('\n!!!!!!!!!!!\nInconsistent data input: Goal was found in the passing column:\nEither improve code or clean up data.\n!!!!!!!!!!!')
				overwriteOutput = True
				break
			count = count + 1

	# To export:
	exportDataString.append('passCount')
	exportData.append(float(len(passes)))
	passCountA = sum([TeamAstring in i for i in passes])
	exportFullExplanation.append('Number of passes per %s.' %aggregateEvent)

	exportDataString.append('passCountA')
	exportData.append(float(passCountA))
	passCountB = sum([TeamBstring in i for i in passes])
	exportFullExplanation.append('Number of passes by %s per %s.' %(TeamAstring,aggregateEvent))
	
	exportDataString.append('passCountB')
	exportData.append(float(passCountB))
	exportFullExplanation.append('Number of passes by %s per %s.' %(TeamBstring,aggregateEvent))
	
	exportDataString.append('passDelta')
	exportData.append(float(abs(passCountA - passCountB)))
	exportFullExplanation.append('Absolute difference between number of passes scored by %s and %s.' %(TeamAstring,TeamBstring))

	ind = 0
	ConsecutivePasses = []
	PassessPossession = []

	for idx,i in enumerate(possessionCharacteristics):
		tmp = 0		
		if len(passOut) != 0: # Skip this if there were no passess during the current possession
			while passOut[ind][1] >= i[0]: # after the start
				if passOut[ind][1] <= i[4]: # before the end
					tmp = tmp + 1
					# current pass falls within current
					if len(passOut) > ind+1:
						ind = ind + 1
					else:
						break
				else:
					# pass belongs in next section
					break
		ConsecutivePasses.append(tmp)
		PassessPossession.append(i[2])

	if ConsecutivePasses == [] or ConsecutivePasses == [0] or possessionCount == 0: # no passes detected, overwrite output
		ConsecutivePassesAvg = None
		overwriteOutput = True # might be obsolete
		PassessPossession = [TeamAstring,TeamBstring] # might be obsolete
	else:
		ConsecutivePassesAvg = float(sum(ConsecutivePasses) / possessionCount)

	exportDataString.append('ConsecutivePassesMax')
	exportData.append(float(max(ConsecutivePasses)))
	exportFullExplanation.append('Maximum number of consecutive passes during one possession per %s.' %aggregateEvent)

	exportDataString.append('ConsecutivePassesMaxA')
	consPassesA = [val for idx,val in enumerate(ConsecutivePasses) if PassessPossession[idx] == TeamAstring]
	if consPassesA == [] or possessionCountA == 0:
		consPassesA = [0]
		ConsecutivePassesAvgA = None
	else:
		ConsecutivePassesAvgA = float(sum([val for idx,val in enumerate(ConsecutivePasses) if PassessPossession[idx] == TeamAstring]) / possessionCountA)
	exportData.append(float(max(consPassesA)))
	exportFullExplanation.append('Maximum number of consecutive passes during one possession by %s per %s.' %(TeamAstring,aggregateEvent))

	exportDataString.append('ConsecutivePassesMaxB')
	consPassesB = [val for idx,val in enumerate(ConsecutivePasses) if PassessPossession[idx] == TeamBstring]
	if consPassesB == [] or possessionCountB == 0:
		consPassesB = [0]
		ConsecutivePassesAvgB = None
	else:
		ConsecutivePassesAvgB = float(sum([val for idx,val in enumerate(ConsecutivePasses) if PassessPossession[idx] == TeamBstring]) / possessionCountB)
	exportData.append(float(max(consPassesB)))
	exportFullExplanation.append('Maximum number of consecutive passes during one possession by %s per %s.' %(TeamBstring,aggregateEvent))

	# Average per possession
	exportDataString.append('ConsecutivePassesAvg')
	exportData.append(ConsecutivePassesAvg)
	exportFullExplanation.append('Average number of consecutive passes during one possession by %s per %s.' %(TeamAstring,aggregateEvent))

	exportDataString.append('ConsecutivePassesAvgA')
	exportData.append(ConsecutivePassesAvgA)
	exportFullExplanation.append('Average number of consecutive passes during one possession by %s per %s.' %(TeamAstring,aggregateEvent))

	exportDataString.append('ConsecutivePassesAvgB')
	exportData.append(ConsecutivePassesAvgB)
	exportFullExplanation.append('Average number of consecutive passes during one possession by %s per %s.' %(TeamBstring,aggregateEvent))

	safetyWarning.checkLengthExport(exportData,exportDataString,exportFullExplanation)

	return exportData,exportDataString,exportFullExplanation
#################################################################
#################################################################