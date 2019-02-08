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
import logging
from datetime import datetime
from os.path import isfile, join, exists, realpath, abspath, split,dirname, isdir, basename

import safetyWarning

if __name__ == '__main__':
	process(rawDict,attributeDict,TeamAstring,TeamBstring,filename,folder,exportData,exportDataString,exportFullExplanation)

	goals(rawDict,attributeDict,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation,targetEvents)
	possession(rawDict,attributeDict,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation,targetEvents)	
	passes(rawDict,attributeDict,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation,possessionCharacteristics,targetEvents)

def process(rawDict,attributeDict,TeamAstring,TeamBstring,filename,folder,exportData,exportDataString,exportFullExplanation):
	existingAttributesIssues = False
		# Experimental group label
	# exportData = [str(filename[9:13]), filename[14:17]]# Always 3 letters? Always same format?
	# exportDataString = ['expGroupID','expTest']
	# exportFullExplanation = ['Identifier of the experimental group','Name of the type of trial (pre = pre-test, pos = post-test, tra = transfer test, ret = retention test)']

	targetEvents = {'Goals':[],'Passes':[],'Turnovers':[],'Possession':[]}

	########################################################################################
	####### Count existing attributes ######################################################
	########################################################################################

	## goals
	exportData,exportDataString,exportFullExplanation,targetEvents = goals(rawDict,attributeDict,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation,targetEvents)
	## possession / turnovers
	exportData,exportDataString,exportFullExplanation,possessionCharacteristics,targetEvents,tmp1 = possession(rawDict,attributeDict,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation,targetEvents)
	## Pass
	exportData,exportDataString,exportFullExplanation,targetEvents,tmp2 = passes(rawDict,attributeDict,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation,possessionCharacteristics,targetEvents)
	if tmp1 or tmp2:
		existingAttributesIssues = True

	# ############## EXPORT THIS BEAUTY TO CSV
	# outputFilename = folder + 'output.csv'
	# exportCSV.newOrAdd(outputFilename,exportDataString,exportData)	
	# outputFilename = folder + 'outputDescription.txt'
	# exportCSV.varDescription(outputFilename,exportDataString,exportFullExplanation)

	return exportData,exportDataString,exportFullExplanation,targetEvents,existingAttributesIssues

def goals(rawDict,attributeDict,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation,targetEvents):
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

	# To export:
	exportDataString.append('goalCount')
	exportData.append(float(len(goals)))
	exportFullExplanation.append('Number of goals per match.')

	exportDataString.append('goalCountA')
	goalCountA = sum([TeamAstring in i for i in goals])
	exportData.append(float(goalCountA))
	exportFullExplanation.append('Number of goals by %s per match.' %TeamAstring)

	exportDataString.append('goalCountB')
	goalCountB = sum([TeamBstring in i for i in goals])
	exportData.append(float(goalCountB))
	exportFullExplanation.append('Number of goals by %s per match.' %TeamBstring)

	exportDataString.append('goalsDelta')
	exportData.append(float(abs(goalCountA - goalCountB)))
	exportFullExplanation.append('Absolute difference between number of goals scored by %s and %s.' %(TeamAstring,TeamBstring))

	safetyWarning.checkLengthExport(exportData,exportDataString,exportFullExplanation)
	return exportData,exportDataString,exportFullExplanation,targetEvents

#################################################################
#################################################################

def possession(rawDict,attributeDict,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation,targetEvents):
	overwriteOutput = False
	initLength = len(exportData)

	possessionEvent = [(i,val) for i,val in enumerate(attributeDict['Possession/Turnover']) if val  != '' ]
	possessionCharacteristics = []
	dt = []

	for idx,i in enumerate(possessionEvent):
		curFrame = i[0] # frame
		curTime = rawDict['Time']['TsS'][i[0]]
		curStatus = i[1]
		# Determine per event who has possession from that frame onward

		if not idx == len(possessionEvent)-1:
			currentPossessionDuration = rawDict['Time']['TsS'][possessionEvent[idx+1][0]-1] - curTime
			endPossession = possessionEvent[idx+1][0]-1
		else:
			currentPossessionDuration = None
			endPossession = None

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
			currentPossessionDuration = None
			currentPossession = None

		elif 'Turnover' in curStatus:		
			dt.append(float(rawDict['Time']['TsS'][curFrame+1]-rawDict['Time']['TsS'][curFrame])) # a cheecky way to read frame rate from data
			if currentPossession == TeamAstring:
				currentPossession = TeamBstring
				targetEvents['Turnovers'].append((float(rawDict['Time']['TsS'][idx]),TeamAstring))								
			elif currentPossession == TeamBstring:
				currentPossession = TeamAstring
				targetEvents['Turnovers'].append((float(rawDict['Time']['TsS'][idx]),TeamBstring))								
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

		possessionCharacteristics.append([curFrame,curStatus,currentPossession,currentPossessionDuration,endPossession])
		if curFrame == None:
			in1 = None
		else:
			in1 = float(rawDict['Time']['TsS'][curFrame])
		if endPossession == None:
			in2 = None
		else:
			in2 = float(rawDict['Time']['TsS'][endPossession])
		targetEvents['Possession'].append((in1,in2,currentPossession))								

	# print(rawDict['Time']['TsS'])
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

	rawDict['Time']['TsS']
	attributeDict['currentPossession'] = [''] * len(rawDict['Time']['TsS'])
	# Allocate possession to timeseries attributes
	for i in range(len(possessionCharacteristics)-1):
		for q in range(possessionCharacteristics[i][0],possessionCharacteristics[i+1][0]):
			attributeDict['currentPossession'][q] = possessionCharacteristics[i][2]

	# # To export:'
	exportDataString.append('turnoverCount')
	exportData.append(float(sum(['Turnover' in i[1] for i in possessionCharacteristics])))
	exportFullExplanation.append('Number of turnovers per match.')

	exportDataString.append('turnoverCountA')
	turnoverCountA = sum(['Turnover' in i[1] for i in possessionCharacteristics if i[2] == TeamAstring])
	exportData.append(float(turnoverCountA))
	exportFullExplanation.append('Number of turnovers by %s per match.' %TeamAstring)

	exportDataString.append('turnoverCountB')
	turnoverCountB = sum(['Turnover' in i[1] for i in possessionCharacteristics if i[2] == TeamBstring])
	exportData.append(float(turnoverCountB))
	exportFullExplanation.append('Number of turnovers by %s per match.' %TeamBstring)

	exportDataString.append('turnoverDelta')
	exportData.append(float(abs(turnoverCountA - turnoverCountB)))
	exportFullExplanation.append('Absolute difference between number of turnovers scored by %s and %s.' %(TeamAstring,TeamBstring))

	# possessionCount in occurence (or frequency) in the whole trial
	exportDataString.append('possessionCount')
	possessionCount = sum([i[2] != None for i in possessionCharacteristics])
	exportData.append(float(possessionCount))
	exportFullExplanation.append('Number of times a different team had possession per match.')

	exportDataString.append('possessionCountA')
	possessionCountA = sum([i[2] == TeamAstring for i in possessionCharacteristics])
	exportData.append(float(possessionCountA))
	exportFullExplanation.append('Number of times %s had possession per match.' %TeamAstring)

	exportDataString.append('possessionCountB')
	possessionCountB = sum([i[2] == TeamBstring for i in possessionCharacteristics])
	exportData.append(float(possessionCountB))
	exportFullExplanation.append('Number of times %s had possession per match.' %TeamBstring)

	exportDataString.append('possessionCountDelta')	
	exportData.append(float(abs(possessionCountA - possessionCountB)))
	exportFullExplanation.append('Absolute difference between number of possessions scored by %s and %s.' %(TeamAstring,TeamBstring))

	exportDataString.append('possessionCountNone')
	possessionCountNone = len(possessionCharacteristics) - possessionCount
	exportData.append(float(possessionCountNone))
	exportFullExplanation.append('Number of times no team had possession per match.')

	# possessionDuration (in seconds) --> sum,std,avg,
	# Sum of the whole trial
	exportDataString.append('possessionDurationSum')
	possessionDurationSum = sum([i[3] for i in possessionCharacteristics if i[3] != None])
	exportData.append(float(possessionDurationSum))
	exportFullExplanation.append('Total duration (s) of possession per match by %s and %s.' %(TeamAstring,TeamBstring))

	exportDataString.append('possessionDurationSumA')
	possessionDurationSumA = sum([i[3] for i in possessionCharacteristics if i[3] != None and i[2] == TeamAstring])
	exportData.append(float(possessionDurationSumA))
	exportFullExplanation.append('Total duration (s) of possession per match by %s.' %TeamAstring)

	exportDataString.append('possessionDurationSumB')
	possessionDurationSumB = sum([i[3] for i in possessionCharacteristics if i[3] != None and i[2] == TeamBstring])
	exportData.append(float(possessionDurationSumB))
	exportFullExplanation.append('Total duration (s) of possession per match by %s.' %TeamBstring)

	exportDataString.append('possessionDurationSumNone')
	possessionDurationSumNone = sum([None == i for i in attributeDict['currentPossession']])*frameTime
	exportData.append(float(possessionDurationSumNone))
	exportFullExplanation.append('Total duration (s) of no possession per match.')

	# Average duration of a possession (until turnover / ball loss / end game)
	if possessionCount == 0 and overwriteOutput:
		possessionCount = 1
		possessionCountA = 1		
		possessionCountB = 1
		possessionCountNone = 1		

	exportDataString.append('possessionDurationAvg')
	possessionDurationAvg = possessionDurationSum / possessionCount
	exportData.append(float(possessionDurationAvg))
	exportFullExplanation.append('Average duration of a possession per match by %s and %s.' %(TeamAstring,TeamBstring))

	exportDataString.append('possessionDurationAvgA')
	possessionDurationAvgA = possessionDurationSumA / possessionCountA
	exportData.append(float(possessionDurationAvgA))
	exportFullExplanation.append('Average duration of a possession per match by %s.' %TeamAstring)

	exportDataString.append('possessionDurationAvgB')
	possessionDurationAvgB = possessionDurationSumB / possessionCountB
	exportData.append(float(possessionDurationAvgB))
	exportFullExplanation.append('Average duration of a possession per match by %s.' %TeamBstring)

	exportDataString.append('possessionDurationAvgNone')
	exportData.append(float(possessionDurationSumNone / possessionCountNone))
	exportFullExplanation.append('Average duration (s) of no possession per match.')

	# Standard deviation of a possession
	exportDataString.append('possessionDurationStd')
	exportData.append(float(sum([abs(i[3]-possessionDurationAvg) for i in possessionCharacteristics if i[3] != None])/ possessionCount))
	exportFullExplanation.append('Standard deviation of the duration of a possession per match by %s and %s.' %(TeamAstring,TeamBstring))
	
	exportDataString.append('possessionDurationStdA')
	exportData.append(float(sum([abs(i[3]-possessionDurationAvgA) for i in possessionCharacteristics if i[3] != None and i[2] == TeamAstring])/ possessionCountA))
	exportFullExplanation.append('Standard deviation of the duration of a possession per match by %s.' %TeamAstring)

	exportDataString.append('possessionDurationStdB')
	exportData.append(float(sum([abs(i[3]-possessionDurationAvgB) for i in possessionCharacteristics if i[3] != None and i[2] == TeamBstring])/ possessionCountB))
	exportFullExplanation.append('Standard deviation of the duration of a possession per match by %s.' %TeamBstring)

	exportDataString.append('possessionDurationSumNone')
	exportData.append(None) # incvonient to compute. No possessionCharacteristics[3] in for loop)
	exportFullExplanation.append('Standard deviation of the duration (s) of no possession per match. NOTE: I did not (yet) compute this variable as it was somewhat annoying to do, and I dont think it is so interesting. Rens.')

	safetyWarning.checkLengthExport(exportData,exportDataString,exportFullExplanation)

	if overwriteOutput:
		warn('\nOutput replaced with 999')
		for idx,val in enumerate(exportData):
			if idx >= initLength:
				exportData[idx] = 999

	return exportData,exportDataString,exportFullExplanation,possessionCharacteristics,targetEvents,overwriteOutput

#################################################################
#################################################################

def passes(rawDict,attributeDict,TeamAstring,TeamBstring,exportData,exportDataString,exportFullExplanation,possessionCharacteristics,targetEvents):
	overwriteOutput = False
	initLength = len(exportData)
	i = [idx for idx,val in enumerate(exportDataString) if val == 'possessionCount']

	possessionCount = exportData[i[0]]
	i = [idx for idx,val in enumerate(exportDataString) if val == 'possessionCountA']
	possessionCountA = exportData[i[0]]
	i = [idx for idx,val in enumerate(exportDataString) if val == 'possessionCountB']
	possessionCountB = exportData[i[0]]

	count = 0
	passes = [i for i in attributeDict['Pass'] if i  != '' ]

	passOut = np.ones((len(passes),2),dtype='int')*-1

	for idx,i in enumerate(attributeDict['Pass']):
		if not i == '':
			passOut[count,1] = idx#rawDict['Time']['TsS'][idx]
			if TeamAstring in i:
				passOut[count,0] = 0
				targetEvents['Passes'].append((float(rawDict['Time']['TsS'][idx]),TeamAstring))				
			elif TeamBstring in i:
				passOut[count,0] = 1
				targetEvents['Passes'].append((float(rawDict['Time']['TsS'][idx]),TeamBstring))								
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
	exportFullExplanation.append('Number of passes per match.')

	exportDataString.append('passCountA')
	exportData.append(float(passCountA))
	passCountB = sum([TeamBstring in i for i in passes])
	exportFullExplanation.append('Number of passes by %s per match.' %TeamAstring)
	
	exportDataString.append('passCountB')
	exportData.append(float(passCountB))
	exportFullExplanation.append('Number of passes by %s per match.' %TeamBstring)
	
	exportDataString.append('passDelta')
	exportData.append(float(abs(passCountA - passCountB)))
	exportFullExplanation.append('Absolute difference between number of passes scored by %s and %s.' %(TeamAstring,TeamBstring))

	ind = 0
	ConsecutivePasses = []
	PassessPossession = []

	for idx,i in enumerate(possessionCharacteristics):
		tmp = 0		
		while passOut[ind][1] >= i[0]: # after the start
			if passOut[ind][1] <= i[4]: # before the end
				# print('found one',passOut[ind][1])
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

	if ConsecutivePasses == []: # no passes detected, overwrite output
		overwriteOutput = True
		ConsecutivePasses = [0,0]
		PassessPossession = [TeamAstring,TeamBstring]

	exportDataString.append('ConsecutivePassesMax')
	exportData.append(float(max(ConsecutivePasses)))
	exportFullExplanation.append('Maximum number of consecutive passes during one possession per match.')

	exportDataString.append('ConsecutivePassesMaxA')
	exportData.append(float(max([val for idx,val in enumerate(ConsecutivePasses) if PassessPossession[idx] == TeamAstring])))
	exportFullExplanation.append('Maximum number of consecutive passes during one possession by %s per match.' %TeamAstring)

	exportDataString.append('ConsecutivePassesMaxB')
	exportData.append(float(max([val for idx,val in enumerate(ConsecutivePasses) if PassessPossession[idx] == TeamBstring])))
	exportFullExplanation.append('Maximum number of consecutive passes during one possession by %s per match.' %TeamBstring)

	# Average per possession
	exportDataString.append('ConsecutivePassesAvg')
	exportData.append(float(sum(ConsecutivePasses) / possessionCount))
	exportFullExplanation.append('Average number of consecutive passes during one possession by %s per match.' %TeamAstring)

	exportDataString.append('ConsecutivePassesAvgA')
	exportData.append(float(sum([val for idx,val in enumerate(ConsecutivePasses) if PassessPossession[idx] == TeamAstring]) / possessionCountA))
	exportFullExplanation.append('Average number of consecutive passes during one possession by %s per match.' %TeamAstring)

	exportDataString.append('ConsecutivePassesAvgB')
	exportData.append(float(sum([val for idx,val in enumerate(ConsecutivePasses) if PassessPossession[idx] == TeamBstring]) / possessionCountB))
	exportFullExplanation.append('Average number of consecutive passes during one possession by %s per match.' %TeamBstring)

	safetyWarning.checkLengthExport(exportData,exportDataString,exportFullExplanation)
	if overwriteOutput:
		warn('\nOutput replaced with 999')
		for idx,val in enumerate(exportData):
			if idx >= initLength:
				exportData[idx] = 999

	return exportData,exportDataString,exportFullExplanation,targetEvents,overwriteOutput
#################################################################
#################################################################