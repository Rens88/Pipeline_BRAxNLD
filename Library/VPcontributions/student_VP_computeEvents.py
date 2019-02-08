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
	passList = [ i[0] for i in targetEvents['pass']]
	tmp = pd.DataFrame(passList)
	tmp.columns = ['PassTimes']
	passDF = tmp.sort_values('PassTimes', ascending=True)
	passDF.columns = ['PassTimes']

	targetEvents, eventClassified = buildUpEvents(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring,eventClassified,passDF)
	# targetEvents = attackLabels(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring)
	# print('################')
	# print(targetEvents['shotOnTarget'])
	# print(len(targetEvents['attack']))
	# print(attrPanda)
	# pdb.set_trace()
	allDict = pd.concat([rawPanda, attrPanda], axis=1)
	allDict = allDict.loc[:,~allDict.columns.duplicated()]

	return targetEvents,eventClassified,allDict

def buildUpLabels(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring,eventClassified,passDF):
	#labels for event result
	shotNotOnTargetLabel = 1
	shotOnTargetLabel = 2
	goalLabel = 3
	finalThirdLabel = 4
	noFinalThirdLabel = 5
	attackEvents = targetEvents['buildUp']
	maxTimeDiff = 5

	allDict = pd.concat([rawPanda, attrPanda], axis=1)
	allDict = allDict.loc[:,~allDict.columns.duplicated()]

	ball = allDict[allDict['PlayerID'] == 'ball']

	inBallPos = allDict[allDict['InBallPos'] == 1]
	inBallPos = inBallPos.sort_values('Ts')

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
				attackList = list(targetEvents['buildUp'][attackIdx])
				attackList[3] = shotNotOnTargetLabel
				attackTuple = tuple(attackList)
				attackEvents[attackIdx] = attackTuple
				# print('-shotNotOnTargetLabel',attackTuple)
			else:
				logging.critical(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' - ' + basename(__file__) + '\nShot Not on Target is not labeled to an attack')
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
				attackList = list(targetEvents['buildUp'][attackIdx])
				attackList[3] = shotOnTargetLabel
				attackTuple = tuple(attackList)
				attackEvents[attackIdx] = attackTuple
				# print('-shotOnTargetLabel',attackTuple)
			else:
				logging.critical(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' - ' + basename(__file__) + '\nShot on Target is not labeled to an attack')
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
				attackList = list(targetEvents['buildUp'][attackIdx])
				attackList[3] = goalLabel
				attackTuple = tuple(attackList)
				attackEvents[attackIdx] = attackTuple
				# print('-Goal',attackTuple)
			else:
				logging.critical(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' - ' + basename(__file__) + '\nGoal is not labeled to an attack')
				warn('\nGoal is not labeled to an attack: ' + str(i))

	for idx, i in enumerate(attackEvents):
		if (i[3] == None):
			curInPoss = inBallPos[inBallPos['Ts'] == i[0]]
			curInPoss = curInPoss[curInPoss['inZone'] == 1]
			if(len(curInPoss) >= 1):
				attackList = list(targetEvents['buildUp'][idx])
				attackList[3] = finalThirdLabel
				attackTuple = tuple(attackList)
				attackEvents[idx] = attackTuple
			else:
				attackList = list(targetEvents['buildUp'][idx])
				attackList[3] = noFinalThirdLabel
				attackTuple = tuple(attackList)
				attackEvents[idx] = attackTuple
			# print('-No Shot',attackTuple)

	eventClassified = True

	#targetEvents,eventClassified = buildUpLabels(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring,eventClassified)

	return targetEvents, eventClassified

def distanceToBall(rawDict,attributeDict):
	def distance(X_1,Y_1,X_2,Y_2):
		return np.sqrt((X_1 - X_2)**2 + (Y_1 - Y_2)**2)

	#All the players on the field
	tmp = rawDict[rawDict['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']

	allDict = pd.concat([rawDict, attributeDict], axis=1)
	allDict = allDict.loc[:,~allDict.columns.duplicated()]

	#The ball
	ball = allDict[allDict['PlayerID'] == 'ball']

	inBallPos = allDict[allDict['InBallPos'] == 1]
	inBallPos = inBallPos.sort_values('Ts')

	#Create new attribute distanceToBall
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distanceToBall'])
	newAttributes.loc[players.index,'distanceToBall'] = np.nan

	for idx,i in enumerate(pd.unique(inBallPos['Ts'])):
		curTime = i
		if (len(ball['X'][ball['Ts'] == curTime]) != 1 or len(ball['Y'][ball['Ts'] == curTime]) != 1):
			continue
		curBallX = ball['X'][ball['Ts'] == curTime].item()
		curBallY = ball['Y'][ball['Ts'] == curTime].item()
		curPlayersX = players['X'][players['Ts'] == curTime]
		curPlayersY = players['Y'][players['Ts'] == curTime]
		curPlayer_distToBall = distance(curPlayersX, curPlayersY, curBallX, curBallY)
		newAttributes['distanceToBall'][curPlayer_distToBall.index] = curPlayer_distToBall[curPlayer_distToBall.index]

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	tmpdistanceToBall = 'Distance from a player to the ball'

	return attributeDict#,attributeLabel

def buildUpEvents(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring,eventClassified,passDF):
	if 'shotNotOnTarget' not in targetEvents or 'shotOnTarget' not in targetEvents or 'goal' not in targetEvents:
		warn('\nWARNING: Shots and goals are not labeled. Check importEvents.\n')

	endTs = 5.0 #3 seconds not on the ball
	beginTime = 0
	endTime = 0
	buildUpStart = False
	buildUpEvents = []
	previousTime = 0
	zone = 0
	max_X = 52.5
	max_Y = 34
	outTime = 0.5
	ballSpeedTreshold = 1
	firstIteration = True

	allDict = pd.concat([rawPanda, attrPanda], axis=1)
	allDict = allDict.loc[:,~allDict.columns.duplicated()]

	tmp = allDict[allDict['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']

	ball = allDict[allDict['PlayerID'] == 'ball']

	inBallPos = allDict[allDict['InBallPos'] == 1]
	inBallPos = inBallPos.sort_values('Ts')

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

		#determine begin of buildUp. BuildUp starts if:
		#1. The ball is on the field
		#2. The speed of the ball is not 0
		#3. The previous buildUp has ended
		if abs(curBallX) <= max_X and abs(curBallY) <= max_Y and ball['Speed'][(ball['Ts'] > i) & (ball['Ts'] < (i+endTs))].mean() > ballSpeedTreshold and not buildUpStart:
			lastTeam = inBallPos['TeamID'][inBallPos['Ts'] == i].values[0]
			beginTime = round(curTime)
			buildUpStart = True
			#current attacking team
			curAttTeam = inBallPos['TeamID'][inBallPos['Ts'] == i].values[0]

		#determine endTime of buildUp.
		if buildUpStart:
			curInPossession = inBallPos[inBallPos['Ts'] == i]
			#If the opponent has intercepted the ball
			if (curAttTeam != lastTeam and firstIteration == False):
				curInPossession = inBallPos[inBallPos['Ts'] == i]
				#if (len(curInPossession['distanceToBall']) > 0):
					#if (curInPossession['distanceToBall'].item() >= 0.2):
						#continue
				endTime = previousTime
				buildUpStart = False
				inPossession = inBallPos[(inBallPos['Ts'] >= beginTime) & (inBallPos['Ts'] <= endTime)]
				playersInPossession = pd.unique(inPossession['PlayerID'])
				if (len(playersInPossession) >= 2):
					passCount = passDF[passDF['PassTimes'] >= beginTime]
					passCount = passCount[passCount['PassTimes'] <= endTime]
					if(len(passCount) == 1):
						passTime = passCount['PassTimes'].item()
						passedTo = players[players['ballPassedTo'] == 1]
						passedTo = passedTo[passedTo['Ts'] == passTime]
						if(len(passedTo['curPositioningRank']) == 1):
							buildUpEvents.append((endTime,curAttTeam,beginTime,None))
					elif(len(passCount) > 0):
						buildUpEvents.append((endTime,curAttTeam,beginTime,None))
			#buildUp ends if the team has no possession for 5 seconds
			elif ((curTime - previousTime) >= endTs):
				endTime = previousTime
				buildUpStart = False
				inPossession = inBallPos[(inBallPos['Ts'] >= beginTime) & (inBallPos['Ts'] <= endTime)]
				playersInPossession = pd.unique(inPossession['PlayerID'])
				if (len(playersInPossession) >= 2):
					passCount = passDF[passDF['PassTimes'] >= beginTime]
					passCount = passCount[passCount['PassTimes'] <= endTime]
					if(len(passCount) == 1):
						passTime = passCount['PassTimes'].item()
						passedTo = players[players['ballPassedTo'] == 1]
						passedTo = passedTo[passedTo['Ts'] == passTime]
						if(len(passedTo['curPositioningRank']) == 1):
							buildUpEvents.append((endTime,curAttTeam,beginTime,None))
					elif(len(passCount) > 0):
						buildUpEvents.append((endTime,curAttTeam,beginTime,None))
			#if the ball passed the side or backline for 0.5 seconds
			elif (abs(curBallX) >= max_X and abs(curBallXPast) >= max_X) or (abs(curBallY) >= max_Y and abs(curBallYPast) >= max_Y):
				endTime = curTime - outTime
				buildUpStart = False
				inPossession = inBallPos[(inBallPos['Ts'] >= beginTime) & (inBallPos['Ts'] <= endTime)]
				playersInPossession = pd.unique(inPossession['PlayerID'])
				if (len(playersInPossession) >= 2):
					passCount = passDF[passDF['PassTimes'] >= beginTime]
					passCount = passCount[passCount['PassTimes'] <= endTime]
					if(len(passCount) == 1):
						passTime = passCount['PassTimes'].item()
						passedTo = players[players['ballPassedTo'] == 1]
						passedTo = passedTo[passedTo['Ts'] == passTime]
						if(len(passedTo['curPositioningRank']) == 1):
							buildUpEvents.append((endTime,curAttTeam,beginTime,None))
					elif(len(passCount) > 0):
						buildUpEvents.append((endTime,curAttTeam,beginTime,None))
			#if ball is almost not moving for 5 seconds. The referee may have stopped the game (for a free kick for example).
			elif (ball['Speed'][(ball['Ts'] > i) & (ball['Ts'] < (i+endTs))].mean() < ballSpeedTreshold):
				endTime = previousTime
				buildUpStart = False
				inPossession = inBallPos[(inBallPos['Ts'] >= beginTime) & (inBallPos['Ts'] <= endTime)]
				playersInPossession = pd.unique(inPossession['PlayerID'])
				if (len(playersInPossession) >= 2):
					passCount = passDF[passDF['PassTimes'] >= beginTime]
					passCount = passCount[passCount['PassTimes'] <= endTime]
					if(len(passCount) == 1):
						passTime = passCount['PassTimes'].item()
						passedTo = players[players['ballPassedTo'] == 1]
						passedTo = passedTo[passedTo['Ts'] == passTime]
						if(len(passedTo['curPositioningRank']) == 1):
							buildUpEvents.append((endTime,curAttTeam,beginTime,None))
					elif(len(passCount) > 0):
						buildUpEvents.append((endTime,curAttTeam,beginTime,None))
			#if attack ends during half/full time
			elif curTime == max(inBallPos['Ts']):
				endTime = curTime
				buildUpStart = False
				inPossession = inBallPos[(inBallPos['Ts'] >= beginTime) & (inBallPos['Ts'] <= endTime)]
				playersInPossession = pd.unique(inPossession['PlayerID'])
				if (len(playersInPossession) >= 2):
					passCount = passDF[passDF['PassTimes'] >= beginTime]
					passCount = passCount[passCount['PassTimes'] <= endTime]
					if(len(passCount) == 1):
						passTime = passCount['PassTimes'].item()
						passedTo = players[players['ballPassedTo'] == 1]
						passedTo = passedTo[passedTo['Ts'] == passTime]
						if(len(passedTo['curPositioningRank']) == 1):
							buildUpEvents.append((endTime,curAttTeam,beginTime,None))
					elif(len(passCount) > 0):
						buildUpEvents.append((endTime,curAttTeam,beginTime,None))
		if (inBallPos['TeamID'][inBallPos['Ts'] == i].values[0] == lastTeam):
			previousTime = curTime

		lastTeam = inBallPos['TeamID'][inBallPos['Ts'] == i].values[0]
		firstIteration = False

	targetEvents = {**targetEvents,'buildUp': buildUpEvents}

	targetEvents,eventClassified = buildUpLabels(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring,eventClassified,passDF)

	#targetEvents,eventClassified = attackLabels(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring,eventClassified)
	return targetEvents,eventClassified

def buildUpEvents2(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring,eventClassified):
	if 'shotNotOnTarget' not in targetEvents or 'shotOnTarget' not in targetEvents or 'goal' not in targetEvents:
		warn('\nWARNING: Shots and goals are not labeled. Check importEvents.\n')

	consecutiveTs = 5 #consecutive timestamps to call it a buildUp
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

	allDict = pd.concat([rawPanda, attrPanda], axis=1)
	allDict = allDict.loc[:,~allDict.columns.duplicated()]

	ball = allDict[allDict['PlayerID'] == 'ball']

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
		if round(curTime - previousTime,1) == timeFrequenty and abs(curBallX) <= max_X and abs(curBallY) <= max_Y and ball['Speed'][(ball['Ts'] > i) & (ball['Ts'] < (i+endTs))].mean() > ballSpeedTreshold and not attackStart:
			timeCount = timeCount + 1
		else:
			timeCount = 0

		if timeCount == consecutiveTs:
			beginTime = round(curTime - consecutiveTs * timeFrequenty,1)
			attackStart = True
			#current attacking team
			curAttTeam = inBallPos['TeamID'][inBallPos['Ts'] == i].values[0]
			curX = inBallPos['X'][inBallPos['Ts'] == i].values[0]
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


		if all(inBallPos['opponentsHalf'][inBallPos['Ts'] == i] == 1):
			# print('previousTime', curTime)
			previousTime = curTime
		# endTime = 0

		lastTeam = inBallPos['TeamID'][inBallPos['Ts'] == i].values[0]

	targetEvents = {**targetEvents,'attack': attackEvents}

	targetEvents,eventClassified = buildUpLabels(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring,eventClassified)

	return targetEvents,eventClassified


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

	allDict = pd.concat([rawPanda, attrPanda], axis=1)
	allDict = allDict.loc[:,~allDict.columns.duplicated()]

	ball = allDict[allDict['PlayerID'] == 'ball']

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
			curAttTeam = inBallPos['TeamID'][inBallPos['Ts'] == i].values[0]
			curX = inBallPos['X'][inBallPos['Ts'] == i].values[0]
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


		if all(inBallPos['opponentsHalf'][inBallPos['Ts'] == i] == 1):
			# print('previousTime', curTime)
			previousTime = curTime
		# endTime = 0

		lastTeam = inBallPos['TeamID'][inBallPos['Ts'] == i].values[0]

	targetEvents = {**targetEvents,'attack': attackEvents}

	targetEvents,eventClassified = attackLabels(rawPanda,attrPanda,targetEvents,TeamAstring,TeamBstring,eventClassified)

	return targetEvents,eventClassified

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
