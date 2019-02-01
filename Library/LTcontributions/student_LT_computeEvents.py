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
import datetime

## Here, you can clarify which functions exist in this module.
if __name__ == '__main__': 

	process(targetEvents,aggregateLevel,rawPanda,attrPanda,eventsPanda,TeamAstring,TeamBstring)

	# This is an example that can be used to see how you compute an event
	addRandomEvents(rawPanda,targetEvents,TeamAstring,TeamBstring)
	
## Here, you specifiy what each function does
def process(targetEvents,aggregateLevel,rawPanda,attrPanda,eventsPanda,TeamAstring,TeamBstring,eventClassified):
	# targetEvents = addRandomEvents(rawPanda,targetEvents,TeamAstring,TeamBstring)
	# print(targetEvents['shotOnTarget'])
	targetEvents, eventClassified = attackEvents(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring,eventClassified)

	allDict = playerSecondsAttack(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring)
	# targetEvents = attackLabels(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring)
	# print('################')
	# print(targetEvents['shotOnTarget'])
	# print(len(targetEvents['attack']))
	# print(attrPanda)
	# pdb.set_trace()
	
	return targetEvents,eventClassified,allDict

def attackLabels(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring,eventClassified):
	#labels for event result
	noShotLabel = 0
	shotNotOnTargetLabel = 1
	shotOnTargetLabel = 2
	goalLabel = 3
	attackEvents = targetEvents['attack']
	maxTimeDiff = 5

	if 'shotNotOnTarget' in targetEvents:
		for idx, i in enumerate(targetEvents['shotNotOnTarget']):
			# print('shotnot',i)
			timeDiff = 999999
			attackIdx = -1
			for jdx, j in enumerate(attackEvents):
				#event is during the attack
				if i[0] <= j[0] and i[0] >= j[2]:
					attackIdx = jdx
					break
				#determine timedifference between targetEvents and attackEvents with absolute treshold of 5 seconds
				elif abs(i[0] - j[0]) < timeDiff and abs(i[0] - j[0]) <= maxTimeDiff:
					timeDiff = abs(i[0] - j[0])
					attackIdx = jdx

			#put label on attack
			if attackIdx != -1:
				attackList = list(targetEvents['attack'][attackIdx])
				attackList[3] = shotNotOnTargetLabel
				attackTuple = tuple(attackList)
				attackEvents[attackIdx] = attackTuple
				# print('-shotNotOnTargetLabel',attackTuple)
			else:
				warn('\nShot Not on Target is not labeled to an attack: ' + str(i))

	if 'shotOnTarget' in targetEvents:
		for idx, i in enumerate(targetEvents['shotOnTarget']):
			# print('shot',i)
			timeDiff = 999999
			attackIdx = -1
			for jdx, j in enumerate(attackEvents):
				#event is during the attack
				if i[0] <= j[0] and i[0] >= j[2]:
					attackIdx = jdx
					break
				#determine timedifference between targetEvents and attackEvents with absolute treshold of 5 seconds
				elif abs(i[0] - j[0]) < timeDiff and abs(i[0] - j[0]) <= maxTimeDiff:
					timeDiff = abs(i[0] - j[0])
					attackIdx = jdx

			if attackIdx != -1:
				attackList = list(targetEvents['attack'][attackIdx])
				attackList[3] = shotOnTargetLabel
				attackTuple = tuple(attackList)
				attackEvents[attackIdx] = attackTuple
				# print('-shotOnTargetLabel',attackTuple)
			else:
				warn('\nShot on Target is not labeled to an attack: ' + str(i))

	if 'goal' in targetEvents:
		for idx, i in enumerate(targetEvents['goal']):
			timeDiff = 999999
			attackIdx = -1
			for jdx, j in enumerate(attackEvents):
				#event is during the attack
				if i[0] <= j[0] and i[0] >= j[2]:
					attackIdx = jdx
					break
				#determine timedifference between targetEvents and attackEvents with absolute treshold of 5 seconds
				elif abs(i[0] - j[0]) < timeDiff and abs(i[0] - j[0]) <= maxTimeDiff:
					timeDiff = abs(i[0] - j[0])
					attackIdx = jdx

			if attackIdx != -1:
				attackList = list(targetEvents['attack'][attackIdx])
				attackList[3] = goalLabel
				attackTuple = tuple(attackList)
				attackEvents[attackIdx] = attackTuple
				# print('-Goal',attackTuple)
			else:
				warn('\nGoal is not labeled to an attack: ' + str(i))

	for idx, i in enumerate(attackEvents):
		if i[3] == None:
			attackList = list(targetEvents['attack'][idx])
			attackList[3] = noShotLabel
			attackTuple = tuple(attackList)
			attackEvents[idx] = attackTuple
			# print('-No Shot',attackTuple)

	eventClassified = True

	return targetEvents, eventClassified


def attackEvents(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring,eventClassified):
	if 'shotNotOnTarget' not in targetEvents or 'shotOnTarget' not in targetEvents or 'goal' not in targetEvents:
		warn('\nWARNING: Shots and goals are not labeled. Check importEvents.\n')

	consecutiveTs = 5 #consecutive timestamps to call it an attack.
	endTs = 5.0 #5 seconds not the ball
	timeCount = 0
	timeFrequenty = 0.1 #in seconds
	beginTime = 0
	endTime = 0
	attackStart = False
	attackEvents = []
	previousTime = 0
	zone = 0
	max_X = 52.5
	max_Y = 34
	outTime = 0.5
	ballSpeedTreshold = 1
	playerField = 'PlayerID'
	teamField = 'TeamID'

	allDict = pd.concat([rawPanda, attrPanda], axis=1)
	allDict = allDict.loc[:,~allDict.columns.duplicated()]
	# sLength = len(allDict[teamField])
	# allDict = allDict.assign(attack=pd.Series())
	#LT: idee:
	#1. kolom met attack toevoegen en die vullen als de speler aan de bal is tijdens een aanval.
	#2. aantal seconden dat een speler aan de bal is tussen de begin en eindtijd van een attack. Dus pas bepalen nadat de aanvallen bepaald zijn.
	# uniquePlayers = allDict[playerField][(allDict[playerField] != 'ball') & (allDict[playerField] != 'groupRow')].unique()
	# dfPlayers = pd.DataFrame(data=uniquePlayers)
	# dfPlayers.columns = [playerField]
	# dfPlayers = dfPlayers.assign(TeamID=allDict[teamField][allDict[playerField] == dfPlayers])
	# dfPlayers = dfPlayers.assign(seconds=0.0)
	
	ball = allDict[allDict[playerField] == 'ball']

	inBallPos = allDict[allDict['InBallPos'] == 1]
	inBallPos = inBallPos.sort_values('Ts')

	# print(ball)
	# pdb.set_trace()

	for idx,i in enumerate(pd.unique(inBallPos['Ts'])):
		curTime = round(i,1)
		try: #try if ball exists
			curBallX = ball['X'][ball['Ts'] == i].values[0]
			curBallY = ball['Y'][ball['Ts'] == i].values[0]
		except: #continue if there is no ball
			continue
		try:
			curBallXPast = ball['X'][ball['Ts'] == (i-outTime)].values[0]
			curBallYPast = ball['Y'][ball['Ts'] == (i-outTime)].values[0]
		except:
			curBallXPast = 0
			curBallYPast = 0
		
		# if not attackStart:
		# 	print(curTime, round(curTime - previousTime,1) == timeFrequenty, all(inBallPos['inZone'][inBallPos['Ts'] == i] == 1), abs(curBallX) <= max_X, abs(curBallY) <= max_Y)

		#determine beginTime of attack. #Attack starts if:		
		#1. player with ball is for 0.5 seconds in final third
		#2. if the ball is on the field
		#3. if the ball is moving 
		#4. and the previous attack has ended
		if round(curTime - previousTime,1) == timeFrequenty and all(inBallPos['inZone'][inBallPos['Ts'] == i] == 1) and abs(curBallX) <= max_X and abs(curBallY) <= max_Y and ball['Speed'][(ball['Ts'] > i) & (ball['Ts'] < (i+endTs))].mean() > ballSpeedTreshold and not attackStart:
			timeCount = timeCount + 1
		else:
			timeCount = 0

		if timeCount == consecutiveTs:
			beginTime = round(curTime - consecutiveTs * timeFrequenty,1)
			attackStart = True
			#current attacking team
			curAttTeam = inBallPos[teamField][inBallPos['Ts'] == i].values[0]
			curX = inBallPos['X'][inBallPos['Ts'] == i].values[0]
			curPlayer = inBallPos[playerField][inBallPos['Ts'] == i].values[0]
			# dfPlayers.loc[dfPlayers[playerField] == curPlayer,'seconds'] += (consecutiveTs-1) * timeFrequenty
			# allDict.loc[(allDict[playerField] == curPlayer) & (allDict[teamField] == curAttTeam),'attack'] = 1 #LT: pas na 5 ts
			# allDict[playerField][(allDict[playerField] != 'ball') & (allDict[playerField] != 'groupRow')]

			#determine where the final third is. curX is the player with ball in final third
			if curX > 0:
				rightSide = True
			else:
				rightSide = False

		#determine endTime of attack.
		if attackStart:
			#attack ends if the team has no possession on opponents half for 5 seconds
			if (curTime - previousTime) >= endTs:
				endTime = previousTime
				attackStart = False
				attackEvents.append((endTime,curAttTeam,beginTime,None))
				# print('--5sec:', str(datetime.timedelta(seconds=endTime)), curAttTeam, str(datetime.timedelta(seconds=beginTime)), endTime, beginTime)
			#if the ball passes the center line
			elif (curBallX > 0 and not rightSide) or (curBallX < 0 and rightSide):
				endTime = curTime
				attackStart = False
				attackEvents.append((endTime,curAttTeam,beginTime,None))
				# print('--middle:', str(datetime.timedelta(seconds=endTime)), curAttTeam, str(datetime.timedelta(seconds=beginTime)), endTime, beginTime)
			#if the ball passed the side or backline for 0.5 seconds
			elif (abs(curBallX) >= max_X and abs(curBallXPast) >= max_X) or (abs(curBallY) >= max_Y and abs(curBallYPast) >= max_Y):
				endTime = curTime - outTime
				attackStart = False
				attackEvents.append((endTime,curAttTeam,beginTime,None))
				# print('--out:', str(datetime.timedelta(seconds=endTime)), curAttTeam, str(datetime.timedelta(seconds=beginTime)), endTime, beginTime)
			#if defending team shoots the ball out of the final third
			elif lastTeam != curAttTeam and ((curBallX > -18.5 and not rightSide) or (curBallX < 18.5 and rightSide)):
				endTime = previousTime
				attackStart = False
				attackEvents.append((endTime,curAttTeam,beginTime,None))
				# print('--def:', str(datetime.timedelta(seconds=endTime)), curAttTeam, str(datetime.timedelta(seconds=beginTime)), endTime, beginTime)
			#if ball is almost not moving for 5 seconds. The referee may have stopped the game (for a free kick for example).
			elif ball['Speed'][(ball['Ts'] > i) & (ball['Ts'] < (i+endTs))].mean() < ballSpeedTreshold:
				endTime = previousTime
				attackStart = False
				attackEvents.append((endTime,curAttTeam,beginTime,None))
				# print('--ball:', str(datetime.timedelta(seconds=endTime)), curAttTeam, str(datetime.timedelta(seconds=beginTime)), endTime, beginTime)
			#if attack ends during half/full time
			elif curTime == max(inBallPos['Ts']):
				endTime = curTime
				attackStart = False
				attackEvents.append((endTime,curAttTeam,beginTime,None))
				# print('--half/full time:', str(datetime.timedelta(seconds=endTime)), curAttTeam, str(datetime.timedelta(seconds=beginTime)), endTime, beginTime)
			# dfPlayers.loc[dfPlayers[playerField] == curPlayer,'seconds'] += timeFrequenty
			# allDict.loc[(allDict[playerField] == curPlayer) & (allDict[teamField] == curAttTeam),'attack'] = 1


		if all(inBallPos['opponentsHalf'][inBallPos['Ts'] == i] == 1):
			# print('previousTime', curTime)
			previousTime = curTime
		# endTime = 0

		lastTeam = inBallPos['TeamID'][inBallPos['Ts'] == i].values[0]

	targetEvents = {**targetEvents,'attack': attackEvents}

	# secondsInFinalThirdPlayer = allDict.groupby([teamField,playerField])['attack'].sum()/10
	# print(secondsInFinalThirdPlayer)
	# return
	#LT: voor programma van KNVB niet nodig
	# targetEvents,eventClassified = attackLabels(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring,eventClassified)

	return targetEvents,eventClassified

def playerSecondsAttack(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring):
	allDict = pd.concat([rawPanda, attrPanda], axis=1)
	allDict = allDict.loc[:,~allDict.columns.duplicated()]
	dfDanger = allDict[allDict['dangerousity'] > 0]#LT: toevoegen. Dangerousity score groter dan 0.
	dfDanger = dfDanger.sort_values('Ts')
	attackEvents = targetEvents['attack']
	playerField = 'PlayerID'
	teamField = 'TeamID'
	timeFrequenty = 0.1 #in seconds

	# uniquePlayers = allDict[playerField][(allDict[playerField] != 'ball') & (allDict[playerField] != 'groupRow')].unique()
	# dfPlayers = pd.DataFrame(data=uniquePlayers)
	# dfPlayers.columns = [playerField]
	# # dfPlayers = dfPlayers.assign(TeamID=allDict[teamField][allDict[playerField] == dfPlayers])
	# dfPlayers = dfPlayers.assign(seconds=0.0)
	allDict = allDict.assign(inAttack=pd.Series())
	# print(attackEvents)
	# print(dfPlayers)

	if 'attack' in targetEvents:
		for idx, i in enumerate(attackEvents):
			for jdx,j in enumerate(pd.unique(dfDanger['Ts'])):
				#event is during the attack
				# curTeam = dfDanger[teamField][dfDanger['Ts'] == j].values[0]
				if i[0] >= j and i[2] <= j:
					curPlayer = dfDanger[playerField][dfDanger['Ts'] == j].values[0]
					# print(i,j,curPlayer)
					# dfPlayers.loc[dfPlayers[playerField] == curPlayer,'seconds'] += timeFrequenty
					allDict.loc[(allDict[playerField] == curPlayer) & (allDict['Ts'] == j),'inAttack'] = 1
					#LT: hier al voor elk kwartier scores bepalen?

	return allDict

#LT: afronden nodig?
def attackEventsOLD(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring):
	if 'shotNotOnTarget' not in targetEvents or 'shotOnTarget' not in targetEvents or 'goal' not in targetEvents:
		warn('\nWARNING: Shots and goals are not labeled. Check importEvents.\n')

	consecutiveTs = 5 #consecutive timestamps to call it an attack.
	endTs = 20 #2 seconds not the ball
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

	# allDict = rawPanda.join(attrPanda, lsuffix='_raw', rsuffix='_attr')

	opponentsHalf = attrPanda[attrPanda['opponentsHalf'] == 1]
	opponentsHalf = opponentsHalf.sort_values('Ts')
	# print(opponentsHalf)

	attackEvents = []

	previousTime = 0
	for idx,i in enumerate(pd.unique(opponentsHalf['Ts'])):
		curTime = round(i,1)
		# print(curTime)
		curTeamOpponentsHalf = opponentsHalf['TeamID'][opponentsHalf['Ts'] == i].values[0]
		eventSet = False

		#determine beginTime of attack. Attack starts if player with ball is for 0.5 seconds in final third
		if round(curTime - previousTime,1) == timeFrequenty and all(opponentsHalf['inZone'][opponentsHalf['Ts'] == i] == 1) and not attackStart:
			timeCount = timeCount + 1
		else:
			timeCount = 0

		if timeCount == (consecutiveTs - 1):
			beginTime = round(curTime - (consecutiveTs - 1) * timeFrequenty,1)
			attackStart = True

		#determine endTime of attack. attack ends if ball is for 0.5 seconds on own half or opponent obtains possession, or match ends during attack.
		if ((curTime - previousTime) > (endTs - 1) * timeFrequenty or curTime == max(opponentsHalf['Ts'])) and attackStart:
			endTime = previousTime
			attackStart = False

		if endTime > 0:
			#first search for goal, because a goal is also a shot on target
			if 'goal' in targetEvents:
				if not eventSet:
					for idx, i in enumerate(targetEvents['goal']):
						if i[0] >= beginTime and i[0] < endTime:
							attackEvents.append((endTime,curTeamOpponentsHalf,beginTime,goalLabel))
							print(endTime,curTeamOpponentsHalf,beginTime,goalLabel)
							eventSet = True
							break

			if 'shotNotOnTarget' in targetEvents and eventSet == False:
				for idx, i in enumerate(targetEvents['shotNotOnTarget']):
					if i[0] >= beginTime and i[0] < endTime:
						attackEvents.append((endTime,curTeamOpponentsHalf,beginTime,shotNotOnTargetLabel))
						print(endTime,curTeamOpponentsHalf,beginTime,shotNotOnTargetLabel)
						eventSet = True
						break

			if 'shotOnTarget' in targetEvents and eventSet == False:
				if not eventSet:
					for idx, i in enumerate(targetEvents['shotOnTarget']):
						if i[0] >= beginTime and i[0] < endTime:
							attackEvents.append((endTime,curTeamOpponentsHalf,beginTime,shotOnTargetLabel))
							print(endTime,curTeamOpponentsHalf,beginTime,shotOnTargetLabel)
							eventSet = True
							break

			if not eventSet:
				attackEvents.append((endTime,curTeamOpponentsHalf,beginTime,noShotLabel))
				print(endTime,curTeamOpponentsHalf,beginTime,noShotLabel)

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