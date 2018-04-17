# A student's template.
# To make your module function with the pipeline:
# 0) copy the whole example folder, and replace XX with your initials
# 1) edit the name of the student's function in spatialAggregation.py (around line 58)
# 2) edit the student function that's imported at the top of spatialAggregation.py
# 	(where it now says "import student_XX_spatialAggregation")
# 3) change the string content of <studentFolder> in the 'process_Template' (NB: you can make a copy of process_Template and call it process_StudentXX)

import numpy as np
import math
import pandas as pd
import os
import time
import matplotlib.pyplot as plt
from warnings import warn
import pdb;

#standard KNVB settings
fieldLength = 105
fieldWidth = 68

## Here, you can clarify which functions exist in this module.
if __name__ == '__main__': 

	# -- rawDict  -- 
	# A panda that contains the raw data with the keys:
	# - 'Ts' for the time in seconds
	# - 'X' for the x position in meters
	# - 'Y' for the y position in meters
	# - 'PlayerID' for the identifier of a player. *
	# - 'TeamID' for the string that identifies a team.

	# * Note that the 'ball' and 'groupRow' don't refer to a player, 
	# but to a row corresponding to the ball and a row with grouped 
	# information only (e.g., Team A, defenders, all players)

	# -- attributeDict --
	# A panda that contains all the features that are based on the positional data.

	# -- attributeLabel -- 
	# A dictionary that has the same keys as attributeDict. The contents
	# of each key provide a label for each of the computed features,
	# for example: 'Distance to the goal (m)'.
	# When you compute a new feature, please add a corresponding label
	# to the attributeLabel dictionary. Include unit of measurment.

	# -- TeamAstring
	# The string input that corresponds to the team that plays at home.
	# This string is used in rawDict['TeamID'] to indicate the team that
	# a player belongs to.

	# -- TeamBstring
	# The string input that corresponds to the team that plays away.
	# This string is used in rawDict['TeamID'] to indicate the team that
	# a player belongs to.

	# The process function is called to process all subfunctions
	process(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	# This is an example that can be used to see how you compute a group level variable.
	teamCentroid_panda(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)
	# This is an example that can be used to see how to compute an individual level variable.
	distanceToCentroid(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

## Here, you specifiy what each function does
def process(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg):
	# Use this is an example for a GROUP level aggregate
	# attributeDict_EXAMPLE,attributeLabel_EXAMPLE = \
	# teamCentroid_panda(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	# # Use this is an example for a PLAYER level aggregate
	# attributeDict_EXAMPLE,attributeLabel_EXAMPLE = \
	# distanceToCentroid(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	# attributeDict,attributeLabel = \
	# heatMap(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	attributeDict,attributeLabel = \
	ballPossession(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	# attributeDict,attributeLabel = \
	# zone(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	# attributeDict,attributeLabel = \
	# control(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	# attributeDict,attributeLabel = \
	# pressure(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	attributeDict,attributeLabel = \
	density(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	# NB: Centroid and distance to centroid are stored in example variables that are not exported
	# when 'process' is finished, because these features are already embedded in the main pipeline.
	# Make sure that the name of the output variables that you create corresponds to the variables
	# that this function returns (i.e., 'attributeDict' and 'attributeLabel').
	
	return attributeDict,attributeLabel

def timing(f):
    def wrap(*args):
        time1 = time.time()
        ret = f(*args)
        time2 = time.time()
        print('%s function took %0.3f s' % (f.__name__, (time2-time1)))
        return ret
    return wrap

def heatMap(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	# Generate some test data
	x = rawDict['X'][rawDict['TeamID'] == TeamAstring]
	y = rawDict['Y'][rawDict['TeamID'] == TeamAstring]

	heatmap, xedges, yedges = np.histogram2d(x, y, bins=(68,105))
	extent = [xedges[0], xedges[-1], yedges[0], yedges[-1]]

	plt.clf()
	plt.imshow(heatmap.T, extent=extent, origin='lower')
	plt.show()

	pdb.set_trace()

@timing
def ballPossession(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	#ball
	ballComplete = rawDict[rawDict['PlayerID'] == 'ball']

	#only players
	tmp = rawDict[rawDict['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']

	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distToBall', 'inPossession'])
	newAttributes['inPossession'] = 0
	
	# For every ball
	for idx,i in enumerate(ballComplete['Ts']):
		curTime = ballComplete['Ts'][idx]
		
		curBallX = ballComplete['X'][idx]
		curBallY = ballComplete['Y'][idx]

		# Take all corresponding Ts (for PlayerID != 'groupRow' and 'ball')
		curPlayersX = players['X'][players['Ts'] == curTime]
		curPlayersY = players['Y'][players['Ts'] == curTime]

		curPlayersID = players['PlayerID'][players['Ts'] == curTime]

		curPlayer_distToBall = np.sqrt((curPlayersX - curBallX)**2 + (curPlayersY - curBallY)**2)
		newAttributes['distToBall'][curPlayer_distToBall.index] = curPlayer_distToBall
		idxPossession = curPlayer_distToBall[curPlayer_distToBall == min(curPlayer_distToBall)].index
		newAttributes['inPossession'][idxPossession] = 1
		# IDEA??
		# Duration threshold?

		# absolute threshold (<3m?)

		# velocity (same direction?)

		# prioritization?

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	##### THE STRINGS #####	
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	tmpDistToBallString = 'Distance from player to ball.'
	tmpInPossession = 'Boolean for player in possession of the ball.'
	attributeLabel_tmp = {'distToBall':tmpDistToBallString,'inPossession':tmpInPossession}
	attributeLabel.update(attributeLabel_tmp)
	
	return attributeDict,attributeLabel

def determineSide(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	##############   DETERMINE SIDE   ###############
	team_A = rawDict[rawDict['TeamID'] == TeamAstring]
	team_B = rawDict[rawDict['TeamID'] == TeamBstring]

	#only first timestamp
	team_A_Begin = team_A[team_A['Ts'] == min(team_A['Ts'])]
	team_B_Begin = team_B[team_B['Ts'] == min(team_B['Ts'])]

	#sum of all X positions of the players per team at timestamp 0
	team_A_Begin_X = sum(team_A_Begin['X'])
	team_B_Begin_X = sum(team_B_Begin['X'])

	#return team that is on the left side of the field, and location of the goals
	if(team_A_Begin_X < 0 and team_B_Begin_X > 0):
		goal_A_X = (fieldLength / 2) * -1
		goal_B_X = (fieldLength / 2)
		goal_Y = 0
		return TeamAstring, goal_A_X, goal_B_X, goal_Y
	elif(team_A_Begin_X > 0 and team_B_Begin_X < 0):
		goal_A_X = (fieldLength / 2)
		goal_B_X = (fieldLength / 2) * -1
		goal_Y = 0
		return TeamBstring, goal_A_X, goal_B_X, goal_Y
	else:
		warn('\nWARNING: Cannot determine the side, because the players of the teams are not on the same side at the first timestamp.\n')
		return 'Err','Err','Err','Err'

@timing
def zone(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	beginZone = 34
	zoneMin_X = fieldLength / 2 - beginZone

	##############   READ ZONE CSV   ###############
	dirPath = os.path.dirname(os.path.realpath(__file__))
	fileName = dirPath + '\\Zone.csv'
	zoneMatrix = np.loadtxt(open(fileName, 'r'),delimiter=os.pathsep)

	##############   DETERMINE SIDE   ###############
	leftSide, goal_A_X, goal_B_X, goal_Y = determineSide(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	#LT: switch sides at half time? When is it half time?
	#determine zone X of both teams
	if(leftSide == TeamAstring):
		zoneA = -1
		zoneB = 1
		zoneMin_A_X = goal_B_X - beginZone
		zoneMax_A_X = goal_B_X
		zoneMin_B_X = goal_A_X
		zoneMax_B_X = goal_A_X + beginZone
	elif(leftSide == TeamBstring):
		zoneA = 1
		zoneB = -1
		zoneMin_A_X = goal_B_X
		zoneMax_A_X = goal_B_X + beginZone
		zoneMin_B_X = goal_A_X - beginZone
		zoneMax_B_X = goal_A_X
	else:
		#Error: see determineSide()
		return attributeDict,attributeLabel

	#determine zone Y, For both teams the same
	zoneMin_Y = (fieldWidth / 2) * -1
	zoneMax_Y = (fieldWidth / 2)

	##############   CREATE ATTRIBUTES   ###############
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['zone','inZone'])
	newAttributes['inZone'] = 0
	newAttributes['zone'] = 0

	inPossession = rawDict[attributeDict['inPossession'] == 1]

	##############   DETERMINE ZONE VALUE IF PLAYER IS IN ZONE   ###############
	for idx,i in enumerate(pd.unique(rawDict['Ts'])):
		curTime = rawDict['Ts'][idx]
		curPlayerX = inPossession['X'][inPossession['Ts'] == curTime]
		curPlayerY = inPossession['Y'][inPossession['Ts'] == curTime]
		curPlayerTeam = inPossession['TeamID'][inPossession['Ts'] == curTime]

		#skip if NaN
		if all(np.isnan(curPlayerX)) or all(np.isnan(curPlayerY)):
		 	continue #LT: or break?

		#TEAM A
		if all(curPlayerTeam == TeamAstring) and all(curPlayerX >= zoneMin_A_X) and all(curPlayerX <= zoneMax_A_X) and all(curPlayerY >= zoneMin_Y) and all(curPlayerY <= zoneMax_Y):
			zone_X = int(round(abs(curPlayerX + (zoneA * zoneMin_X))))
			zone_Y = int(round(abs(curPlayerY + zoneMax_Y)))
			zoneValue = zoneMatrix[zone_X][zone_Y]

			newAttributes['zone'][curPlayerTeam.index] = zoneValue
			newAttributes['inZone'][curPlayerTeam.index] = 1
		#TEAM B
		elif all(curPlayerTeam == TeamBstring) and all(curPlayerX >= zoneMin_B_X) and all(curPlayerX <= zoneMax_B_X) and all(curPlayerY >= zoneMin_Y) and all(curPlayerY <= zoneMax_Y):
			zone_X = int(round(abs(curPlayerX + (zoneB * zoneMin_X))))
			zone_Y = int(round(abs(curPlayerY + zoneMax_Y)))
			zoneValue = zoneMatrix[zone_X][zone_Y]

			newAttributes['zone'][curPlayerTeam.index] = zoneValue
			newAttributes['inZone'][curPlayerTeam.index] = 1
		else:
			continue

	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	##### THE STRINGS #####
	tmpZone = 'Value of player with ball in the zone (last' + str(beginZone) + 'meters).'
	tmpInZone = 'Is player with ball in the zone?'

	attributeLabel_tmp = {'zone': tmpZone, 'inZone': tmpInZone}
	attributeLabel.update(attributeLabel_tmp)

	# altogether = pd.concat([rawDict,attributeDict], axis=1)
	# altogether.to_csv('D:\\KNVB\\test.csv')

	return attributeDict,attributeLabel

@timing
def control(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	#Only players in possession
	inPossession = attributeDict[attributeDict['inPossession'] == 1]

	#ball
	ballComplete = attributeDict[rawDict['PlayerID'] == 'ball']
	
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['avgRelSpeedPlayerBall','control'])
	newAttributes['avgRelSpeedPlayerBall'] = 0
	newAttributes['control'] = 0

	#LT: how to determine this constant?
	constant = 1
	
	# For every timestamp in ball
	for idx,i in enumerate(ballComplete['Ts']):
		curTime = ballComplete['Ts'][idx]

		curBallSpeed = ballComplete['Snelheid'][idx]
		curPlayerSpeed = inPossession['Snelheid'][inPossession['Ts'] == curTime]
		curPlayerSpeedDiffBall = abs((curPlayerSpeed - curBallSpeed)/curBallSpeed)
		control = 1 - constant * curPlayerSpeedDiffBall**2

		newAttributes['avgRelSpeedPlayerBall'][curPlayerSpeedDiffBall.index] = curPlayerSpeedDiffBall
		newAttributes['control'][curPlayerSpeedDiffBall.index] = control
		
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	tmpAvgRelSpeedPlayerBall = 'Average relative speed of player and ball.'
	tmpControl = 'Ball control of player with ball.'

	attributeLabel_tmp = {'avgRelSpeedPlayerBall': tmpAvgRelSpeedPlayerBall, 'control': tmpControl}
	attributeLabel.update(attributeLabel_tmp)

	# altogether = pd.concat([rawDict,attributeDict], axis=1)
	# altogether.to_csv('D:\\KNVB\\test.csv')
	
	return attributeDict,attributeLabel

@timing
def pressure(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	def distance(X_1,Y_1,X_2,Y_2):
		return np.sqrt((X_1 - X_2)**2 + (Y_1 - Y_2)**2)

	#distances
	highPressDist = 1
	headOnDist = 4
	lateralDist = 3
	hindDist = 2 

	#angles to both sides, so max angle is 180
	highPressAngle = 180 #LT: not necessary --> everybody
	headOnAngle = 90 #both sides 90 degrees
	lateralAngle = 45
	hindAngle = 45

	#values
	highPressValue = 10
	headOnValue = 7
	lateralValue = 5
	hindValue = 2

	#LT: how to determine this constant?
	constant = 1

	leftSide, goal_A_X, goal_B_X, goal_Y = determineSide(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['pressureOnPlayerWithBall','pressureFromDefender','pressureZone','angleInPossDefGoal','distToPlayerWithBall'])

	#players in possession
	inPossession = rawDict[attributeDict['inPossession'] == 1]

	#all players
	players = rawDict[(rawDict['PlayerID'] != 'ball') & (rawDict['PlayerID'] != 'groupRow')]

	newAttributes['pressureFromDefender'] = 0

	for idx,i in enumerate(pd.unique(rawDict['Ts'])):
		curTime = rawDict['Ts'][idx]
		curPlayer = players[players['Ts'] == curTime]
		curInPossession = inPossession[inPossession['Ts'] == curTime]
		curTeamInPossession = inPossession['TeamID'][inPossession['Ts'] == curTime]
		curInPossessionX = inPossession['X'][inPossession['Ts'] == curTime]
		curInPossessionY = inPossession['Y'][inPossession['Ts'] == curTime]

		if all(np.isnan(curInPossessionX)) or all(np.isnan(curInPossessionY)):
		 	continue #LT: or break?
		else:
			curInPossessionX = float(curInPossessionX)
			curInPossessionY = float(curInPossessionY)

		#calculate distances between player with ball, defender and goal
		if all(curTeamInPossession == TeamAstring):
			playersOpponent = curPlayer[curPlayer['TeamID'] == TeamBstring]

			distInPossessDefender = distance(curInPossessionX,curInPossessionY,playersOpponent['X'],playersOpponent['Y'])
			distInPossessGoal = distance(curInPossessionX,curInPossessionY,goal_B_X,goal_Y)
			distDefenderGoal = distance(playersOpponent['X'],playersOpponent['Y'],goal_B_X,goal_Y)

		elif all(curTeamInPossession == TeamBstring):
			playersOpponent = curPlayer[curPlayer['TeamID'] == TeamAstring]

			distInPossessDefender = distance(curInPossessionX,curInPossessionY,playersOpponent['X'],playersOpponent['Y'])
			distInPossessGoal = distance(curInPossessionX,curInPossessionY,goal_A_X,goal_Y)
			distDefenderGoal = distance(playersOpponent['X'],playersOpponent['Y'],goal_A_X,goal_Y)

		else:#nobody in possession
			continue

		newAttributes['distToPlayerWithBall'][playersOpponent.index] = distInPossessDefender
		#angle between player with ball, defender and goal, see https://stackoverflow.com/questions/1211212/how-to-calculate-an-angle-from-three-points
		angleInPossDefGoal = np.degrees(np.arccos((distInPossessDefender**2 + distInPossessGoal**2 - distDefenderGoal**2) / (2 * distInPossessDefender * distInPossessGoal)))

		#HIGH PRESSURE ZONE
		defender = playersOpponent[(distInPossessDefender < highPressDist) & (angleInPossDefGoal < highPressAngle)]
		newAttributes['angleInPossDefGoal'][defender.index] = angleInPossDefGoal[defender.index]
		newAttributes['pressureFromDefender'][defender.index] = 1 - (distInPossessDefender[defender.index] / highPressValue)
		newAttributes['pressureZone'][defender.index] = 'HIGH PRESSURE'

		#HEAD-ON ZONE
		defender = playersOpponent[(distInPossessDefender >= highPressDist) & (distInPossessDefender < headOnDist) & (angleInPossDefGoal < headOnAngle)]
		newAttributes['angleInPossDefGoal'][defender.index] = angleInPossDefGoal[defender.index]
		newAttributes['pressureFromDefender'][defender.index] = 1 - (distInPossessDefender[defender.index] / headOnValue)
		newAttributes['pressureZone'][defender.index] = 'HEAD-ON'

		#LATERAL ZONE
		defender = playersOpponent[(distInPossessDefender >= highPressDist) & (distInPossessDefender < lateralDist) & (angleInPossDefGoal >= headOnAngle) & (angleInPossDefGoal < (headOnAngle + lateralAngle))]
		newAttributes['angleInPossDefGoal'][defender.index] = angleInPossDefGoal[defender.index]
		newAttributes['pressureFromDefender'][defender.index] = 1 - (distInPossessDefender[defender.index] / lateralValue)
		newAttributes['pressureZone'][defender.index] = 'LATERAL'

		#HIND ZONE
		defender = playersOpponent[(distInPossessDefender >= highPressDist) & (distInPossessDefender < hindDist) & (angleInPossDefGoal >= headOnAngle + lateralAngle)]
		newAttributes['angleInPossDefGoal'][defender.index] = angleInPossDefGoal[defender.index]
		newAttributes['pressureFromDefender'][defender.index] = 1 - (distInPossessDefender[defender.index] / hindValue)
		newAttributes['pressureZone'][defender.index] = 'HIND'

		newAttributes['pressureOnPlayerWithBall'][curInPossession.index] = 1 - math.exp(-1 * constant * sum(newAttributes['pressureFromDefender'][playersOpponent.index]))

	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	##### THE STRINGS #####
	tmpPressureOnPlayerWithBall = 'Pressure on player with ball.'
	tmpPressureFromDefender = 'Pressure from defender.'
	tmpPressureZone = 'Pressure Zone for defender.'
	tmpAngleInPossDefGoal = 'Angle for player with ball between defender and goal.'
	tmpDistToPlayerWithBall = 'Distance to player with ball, only for defenders.'

	attributeLabel_tmp = {'pressureOnPlayerWithBall': tmpPressureOnPlayerWithBall, 'pressureFromDefender': tmpPressureFromDefender, 'pressureZone': tmpPressureZone, 'angleInPossDefGoal': tmpAngleInPossDefGoal, 'distToPlayerWithBall': tmpDistToPlayerWithBall}
	attributeLabel.update(attributeLabel_tmp)
	altogether = pd.concat([rawDict,attributeDict], axis=1)
	altogether.to_csv('D:\\KNVB\\test.csv')

	return attributeDict,attributeLabel

def determineZone(pointA,pointB,pointC,pointD,pointE,players):
	lineAB = [pointA,pointB]
	lineBC = [pointB,pointC]
	lineCD = [pointC,pointD]
	if(pointE == ''):
		lineDA = [pointD,pointA]
	else:
		lineDE = [pointD,pointE]
		lineEA = [pointE,pointA]

	X_coords_AB, Y_coords_AB = zip(*lineAB)
	X_coords_BC, Y_coords_BC = zip(*lineBC)
	X_coords_CD, Y_coords_CD = zip(*lineCD)
	if(pointE == ''):
		X_coords_DA, Y_coords_DA = zip(*lineDA)
	else:
		X_coords_DE, Y_coords_DE = zip(*lineDE)
		X_coords_EA, Y_coords_EA = zip(*lineEA)

	coefficientsAB = np.polyfit(X_coords_AB, Y_coords_AB, 1)
	coefficientsBC = np.polyfit(X_coords_BC, Y_coords_BC, 1)
	coefficientsCD = np.polyfit(X_coords_CD, Y_coords_CD, 1)
	if(pointE != ''):
		coefficientsDE = np.polyfit(X_coords_DE,Y_coords_DE, 1)

	Y_AB = (coefficientsAB[0] * players['X']) + coefficientsAB[1]
	X_BC = (players['Y'] - coefficientsBC[1]) / coefficientsBC[0]
	Y_CD = (coefficientsCD[0] * players['X']) + coefficientsCD[1]
	if(pointE == ''):
		X_DA = pointA[0]#goal of opponnent
	else:
		Y_DE = (coefficientsDE[0] * players['X']) + coefficientsDE[1]
		X_EA = pointA[0]#goal of opponnent
		# print(players)#,Y_AB,X_BC,Y_CD,Y_DE,X_EA)
		# print(pointA[0])

	if(pointA[0] < 0 and pointE == ''):
		playerInZone = players[(players['Y'] < Y_AB) & (players['X'] < X_BC) & (players['Y'] > Y_CD) & (players['X'] > X_DA)]
	elif(pointA[0] > 0 and pointE == ''):
		playerInZone = players[(players['Y'] > Y_AB) & (players['X'] > X_BC) & (players['Y'] < Y_CD) & (players['X'] < X_DA)] #LT:nog controleren
	elif(pointA[0] < 0 and pointA[1] < 0 and pointE != ''):#gecontroleerd
		playerInZone = players[(players['Y'] > Y_AB) & (players['X'] < X_BC) & (players['Y'] < Y_CD) & (players['Y'] < Y_DE) & (players['X'] > X_EA)]
	elif(pointA[0] < 0 and pointA[1] > 0 and pointE != ''):#gecontroleerd
		playerInZone = players[(players['Y'] < Y_AB) & (players['X'] < X_BC) & (players['Y'] > Y_CD) & (players['Y'] > Y_DE) & (players['X'] > X_EA)]
	elif(pointA[0] > 0 and pointA[1] < 0 and pointE != ''):#gecontroleerd
		playerInZone = players[(players['Y'] > Y_AB) & (players['X'] > X_BC) & (players['Y'] < Y_CD) & (players['Y'] < Y_DE) & (players['X'] < X_EA)]
	elif(pointA[0] > 0 and pointA[1] > 0 and pointE != ''):#gecontroleerd
		playerInZone = players[(players['Y'] < Y_AB) & (players['X'] > X_BC) & (players['Y'] > Y_CD) & (players['Y'] > Y_DE) & (players['X'] < X_EA)]
	else:
		warn('Something went wrong in determineZone.')
		return 'Err'

	return playerInZone

def plotZone(pointA,pointB,pointC,pointD,pointE,playersOpponent,playersOwnTeam):
	if(pointE == ''):
		data = [pointA,pointB,pointC,pointD]
	else:
		data = [pointA,pointB,pointC,pointD,pointE]

	x_val = [x[0] for x in data]
	y_val = [y[1] for y in data]

	plt.plot(x_val,y_val, color='blue')
	plt.scatter(playersOpponent['X'],playersOpponent['Y'], color='red')
	# if all(playersOwnTeam != ''):
	# 	plt.scatter(playersOwnTeam['X'],playersOwnTeam['Y'], color='green')
	plt.show()
	# pdb.set_trace()

@timing
def density(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	def distance(X_1,Y_1,X_2,Y_2):
		return np.sqrt((X_1 - X_2)**2 + (Y_1 - Y_2)**2)

	#determining centrality
	leftSide, goal_A_X, goal_B_X, goal_Y = determineSide(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)
	# goal_BZ_Y_Left = goal_Y + 10 #2.7 meters next to the goal
	# goal_BZ_Y_Right = goal_Y - 10 #2.7 meters next to the goal
	goalBZWidth = 10
	distInPossessBZ = 2 #distance from player with ball for blocking zone in both sides
	angleInPossessBZ = 90 #angle for the sides of distInPossessBZ
	IZWidth = 11 #width of the Interception Zone
	goalIZNearestWidth = 12
	goalIZFurthestWidth = 17

	#LT:how to determine this constant?
	constantSD = 1
	constantPD = 1

	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['shotDensityForPlayerWithBall','shotDensityFromDefender','passDensityForPlayerWithBall','angleToGoal','playerInIZ','Majority'])

	#players in possession
	inPossession = rawDict[attributeDict['inPossession'] == 1]

	#all players
	players = rawDict[(rawDict['PlayerID'] != 'ball') & (rawDict['PlayerID'] != 'groupRow')]

	newAttributes['shotDensityFromDefender'] = 0
	newAttributes['passDensityFromDefender'] = 0

	for idx,i in enumerate(pd.unique(rawDict['Ts'])):
		curTime = rawDict['Ts'][idx]
		# curTime = 0.5
		curPlayer = players[players['Ts'] == curTime]
		curInPossession = inPossession[inPossession['Ts'] == curTime]
		curInPossessionX = inPossession['X'][inPossession['Ts'] == curTime]
		curInPossessionY = inPossession['Y'][inPossession['Ts'] == curTime]
		curTeamInPossession = inPossession['TeamID'][inPossession['Ts'] == curTime]

		if all(np.isnan(curInPossessionX)) or all(np.isnan(curInPossessionY)):
		 	continue #LT: or break?
		else:
			curInPossessionX = float(curInPossessionX)
			curInPossessionY = float(curInPossessionY)

		#determine centrality
		centrality = 1 - abs(curInPossessionY) / (fieldWidth/2)
		# print(inPossession[inPossession['Ts'] == curTime],centrality)

		if all(curTeamInPossession == TeamAstring):
			playersOpponent = curPlayer[curPlayer['TeamID'] == TeamBstring]
			playersOwnTeam = curPlayer[(curPlayer['TeamID'] == TeamAstring) & (attributeDict['inPossession'] != 1)]

			#calculate angle to goal for player with ball
			radiansToGoal = math.atan2(abs(curInPossessionY-goal_Y), abs(curInPossessionX-goal_B_X))
			degreesToGoal = math.degrees(radiansToGoal)
			newAttributes['angleToGoal'][curInPossession.index] = degreesToGoal

			############################# SHOT DENSITY ######################################

			#https://math.stackexchange.com/questions/143932/calculate-point-given-x-y-angle-and-distance
			if(goal_B_X < 0):
				leftBZ_X = curInPossessionX + distInPossessBZ * np.cos(np.radians(degreesToGoal - angleInPossessBZ))
				leftBZ_Y = curInPossessionY + distInPossessBZ * np.sin(np.radians(degreesToGoal - angleInPossessBZ))
				rightBZ_X = curInPossessionX + distInPossessBZ * np.cos(np.radians(degreesToGoal + angleInPossessBZ))
				rightBZ_Y = curInPossessionY + distInPossessBZ * np.sin(np.radians(degreesToGoal + angleInPossessBZ))
				goal_BZ_Y_Left = goal_Y + goalBZWidth
				goal_BZ_Y_Right = goal_Y - goalBZWidth
			else:
				leftBZ_X = curInPossessionX + distInPossessBZ * np.cos(np.radians(degreesToGoal + angleInPossessBZ))
				leftBZ_Y = curInPossessionY + distInPossessBZ * np.sin(np.radians(degreesToGoal + angleInPossessBZ))
				rightBZ_X = curInPossessionX + distInPossessBZ * np.cos(np.radians(degreesToGoal - angleInPossessBZ))
				rightBZ_Y = curInPossessionY + distInPossessBZ * np.sin(np.radians(degreesToGoal - angleInPossessBZ))
				goal_BZ_Y_Left = goal_Y - goalBZWidth
				goal_BZ_Y_Right = goal_Y + goalBZWidth

			#coordinates of Blocking Zone (BZ)
			leftGoalCoor = (goal_B_X, goal_BZ_Y_Left)
			rightGoalCoor = (goal_B_X, goal_BZ_Y_Right)
			leftBZCoor = (leftBZ_X, leftBZ_Y)
			rightBZCoor = (rightBZ_X, rightBZ_Y)

			defenderInBZ = determineZone(leftGoalCoor, rightBZCoor, leftBZCoor, rightGoalCoor, '', playersOpponent)

			# plotZone(leftGoalCoor, rightBZCoor, leftBZCoor, rightGoalCoor, '', playersOpponent, '')
			# print(defenderInBZ)

			distInPossessDefender = distance(curInPossessionX,curInPossessionY,defenderInBZ['X'],defenderInBZ['Y'])
			distInPossessGoal = distance(curInPossessionX,curInPossessionY,goal_B_X,goal_Y)
			distDefenderGoal = distance(defenderInBZ['X'],defenderInBZ['Y'],goal_B_X,goal_Y)

			if(goal_B_X < 0 and curInPossessionY < goal_Y):
				goalIZ_Y_Left = goal_Y + goalIZFurthestWidth
				goalIZ_Y_Right = goal_Y - goalIZNearestWidth
				middleIZCoor = (goal_B_X + IZWidth, goalIZ_Y_Left)
			elif(goal_B_X < 0 and curInPossessionY > goal_Y):
				goalIZ_Y_Left = goal_Y + goalIZNearestWidth
				goalIZ_Y_Right = goal_Y - goalIZFurthestWidth
				middleIZCoor = (goal_B_X + IZWidth, goalIZ_Y_Right)
			elif(goal_B_X > 0 and curInPossessionY < goal_Y):
				goalIZ_Y_Left = goal_Y - goalIZNearestWidth
				goalIZ_Y_Right = goal_Y + goalIZFurthestWidth
				middleIZCoor = (goal_B_X - IZWidth, goalIZ_Y_Right)
			elif(goal_B_X > 0 and curInPossessionY > goal_Y):
				goalIZ_Y_Left = goal_Y - goalIZFurthestWidth
				goalIZ_Y_Right = goal_Y + goalIZNearestWidth
				middleIZCoor = (goal_B_X - IZWidth, goalIZ_Y_Left)

			#coordinates of Interception Zone (IZ)
			leftGoalCoor = (goal_B_X, goalIZ_Y_Left)
			rightGoalCoor = (goal_B_X, goalIZ_Y_Right)
			leftIZCoor = leftBZCoor
			rightIZCoor = rightBZCoor

			if(goal_B_X < 0 and curInPossessionY < goal_Y):
				# print('AAAAAAAAAAAAa')
				defenderInIZ = determineZone(rightGoalCoor, leftIZCoor, rightIZCoor, middleIZCoor, leftGoalCoor, playersOpponent)
				attackerInIZ = determineZone(rightGoalCoor, leftIZCoor, rightIZCoor, middleIZCoor, leftGoalCoor, playersOwnTeam)
				# plotZone(rightGoalCoor, leftIZCoor, rightIZCoor, middleIZCoor, leftGoalCoor, playersOpponent, playersOwnTeam)
			elif(goal_B_X < 0 and curInPossessionY > goal_Y):
				# print('BBBBBBBBBBBBBBBBBB')
				defenderInIZ = determineZone(leftGoalCoor, rightIZCoor, leftIZCoor, middleIZCoor, rightGoalCoor, playersOpponent)
				attackerInIZ = determineZone(leftGoalCoor, rightIZCoor, leftIZCoor, middleIZCoor, rightGoalCoor, playersOwnTeam)
				# plotZone(leftGoalCoor, rightIZCoor, leftIZCoor, middleIZCoor, rightGoalCoor, playersOpponent, playersOwnTeam)
			elif(goal_B_X > 0 and curInPossessionY < goal_Y):
				# print('CCCCCCCCCCCCCCCCCC')
				defenderInIZ = determineZone(leftGoalCoor, rightIZCoor, leftIZCoor, middleIZCoor, rightGoalCoor, playersOpponent)
				attackerInIZ = determineZone(leftGoalCoor, rightIZCoor, leftIZCoor, middleIZCoor, rightGoalCoor, playersOwnTeam)
				# plotZone(leftGoalCoor, rightIZCoor, leftIZCoor, middleIZCoor, rightGoalCoor, playersOpponent, playersOwnTeam)
			elif(goal_B_X > 0 and curInPossessionY > goal_Y):
				# print('DDDDDDDDDDDDDD')
				defenderInIZ = determineZone(rightGoalCoor, leftIZCoor, rightIZCoor, middleIZCoor, leftGoalCoor, playersOpponent)
				attackerInIZ = determineZone(rightGoalCoor, leftIZCoor, rightIZCoor, middleIZCoor, leftGoalCoor, playersOwnTeam)
				# plotZone(rightGoalCoor, leftIZCoor, rightIZCoor, middleIZCoor, leftGoalCoor, playersOpponent, playersOwnTeam)

			# print("DEFENDER: ", defenderInIZ)
			# print("ATTACKER: ", attackerInIZ)

		elif all(curTeamInPossession == TeamBstring):
			playersOpponent = curPlayer[curPlayer['TeamID'] == TeamAstring]
			playersOwnTeam = curPlayer[(curPlayer['TeamID'] == TeamBstring) & (attributeDict['inPossession'] != 1)]

			#calculate angle to goal for player with ball
			radiansToGoal = math.atan2(abs(curInPossessionY-goal_Y), abs(curInPossessionX-goal_A_X))
			degreesToGoal = math.degrees(radiansToGoal)
			newAttributes['angleToGoal'][curInPossession.index] = degreesToGoal

			############################# SHOT DENSITY ######################################

			#https://math.stackexchange.com/questions/143932/calculate-point-given-x-y-angle-and-distance
			if(goal_A_X < 0):
				leftBZ_X = curInPossessionX + distInPossessBZ * np.cos(np.radians(degreesToGoal - angleInPossessBZ))
				leftBZ_Y = curInPossessionY + distInPossessBZ * np.sin(np.radians(degreesToGoal - angleInPossessBZ))
				rightBZ_X = curInPossessionX + distInPossessBZ * np.cos(np.radians(degreesToGoal + angleInPossessBZ))
				rightBZ_Y = curInPossessionY + distInPossessBZ * np.sin(np.radians(degreesToGoal + angleInPossessBZ))
				goal_BZ_Y_Left = goal_Y + goalBZWidth
				goal_BZ_Y_Right = goal_Y - goalBZWidth
			else:
				leftBZ_X = curInPossessionX + distInPossessBZ * np.cos(np.radians(degreesToGoal + angleInPossessBZ))
				leftBZ_Y = curInPossessionY + distInPossessBZ * np.sin(np.radians(degreesToGoal + angleInPossessBZ))
				rightBZ_X = curInPossessionX + distInPossessBZ * np.cos(np.radians(degreesToGoal - angleInPossessBZ))
				rightBZ_Y = curInPossessionY + distInPossessBZ * np.sin(np.radians(degreesToGoal - angleInPossessBZ))
				goal_BZ_Y_Left = goal_Y - goalBZWidth
				goal_BZ_Y_Right = goal_Y + goalBZWidth

			#coordinates of Blocking Zone (BZ)
			leftGoalCoor = (goal_A_X, goal_BZ_Y_Left)
			rightGoalCoor = (goal_A_X, goal_BZ_Y_Right)
			leftBZCoor = (leftBZ_X, leftBZ_Y)
			rightBZCoor = (rightBZ_X, rightBZ_Y)

			defenderInBZ = determineZone(leftGoalCoor, rightBZCoor, leftBZCoor, rightGoalCoor, '', playersOpponent)

			# plotZone(leftGoalCoor, rightBZCoor, leftBZCoor, rightGoalCoor, '', playersOpponent, '')
			# print(defenderInBZ)

			distInPossessDefender = distance(curInPossessionX,curInPossessionY,defenderInBZ['X'],defenderInBZ['Y'])
			distInPossessGoal = distance(curInPossessionX,curInPossessionY,goal_A_X,goal_Y)
			distDefenderGoal = distance(defenderInBZ['X'],defenderInBZ['Y'],goal_A_X,goal_Y)

			############################# PASS DENSITY ######################################
			if(goal_A_X < 0 and curInPossessionY < goal_Y):
				goalIZ_Y_Left = goal_Y + goalIZFurthestWidth
				goalIZ_Y_Right = goal_Y - goalIZNearestWidth
				middleIZCoor = (goal_A_X + IZWidth, goalIZ_Y_Left)
			elif(goal_A_X < 0 and curInPossessionY > goal_Y):
				goalIZ_Y_Left = goal_Y + goalIZNearestWidth
				goalIZ_Y_Right = goal_Y - goalIZFurthestWidth
				middleIZCoor = (goal_A_X + IZWidth, goalIZ_Y_Right)
			elif(goal_A_X > 0 and curInPossessionY < goal_Y):
				goalIZ_Y_Left = goal_Y - goalIZNearestWidth
				goalIZ_Y_Right = goal_Y + goalIZFurthestWidth
				middleIZCoor = (goal_A_X - IZWidth, goalIZ_Y_Right)
			elif(goal_A_X > 0 and curInPossessionY > goal_Y):
				goalIZ_Y_Left = goal_Y - goalIZFurthestWidth
				goalIZ_Y_Right = goal_Y + goalIZNearestWidth
				middleIZCoor = (goal_A_X - IZWidth, goalIZ_Y_Left)

			#coordinates of Interception Zone (IZ)
			leftGoalCoor = (goal_A_X, goalIZ_Y_Left)
			rightGoalCoor = (goal_A_X, goalIZ_Y_Right)
			leftIZCoor = leftBZCoor
			rightIZCoor = rightBZCoor

			if(goal_A_X < 0 and curInPossessionY < goal_Y):
				# print('AAAAAAAAAAAAa')
				defenderInIZ = determineZone(rightGoalCoor, leftIZCoor, rightIZCoor, middleIZCoor, leftGoalCoor, playersOpponent)
				attackerInIZ = determineZone(rightGoalCoor, leftIZCoor, rightIZCoor, middleIZCoor, leftGoalCoor, playersOwnTeam)
				# plotZone(rightGoalCoor, leftIZCoor, rightIZCoor, middleIZCoor, leftGoalCoor, playersOpponent, playersOwnTeam)
			elif(goal_A_X < 0 and curInPossessionY > goal_Y):
				# print('BBBBBBBBBBBBBBBBBB')
				defenderInIZ = determineZone(leftGoalCoor, rightIZCoor, leftIZCoor, middleIZCoor, rightGoalCoor, playersOpponent)
				attackerInIZ = determineZone(leftGoalCoor, rightIZCoor, leftIZCoor, middleIZCoor, rightGoalCoor, playersOwnTeam)
				# plotZone(leftGoalCoor, rightIZCoor, leftIZCoor, middleIZCoor, rightGoalCoor, playersOpponent, playersOwnTeam)
			elif(goal_A_X > 0 and curInPossessionY < goal_Y):
				# print('CCCCCCCCCCCCCCCCCC')
				defenderInIZ = determineZone(leftGoalCoor, rightIZCoor, leftIZCoor, middleIZCoor, rightGoalCoor, playersOpponent)
				attackerInIZ = determineZone(leftGoalCoor, rightIZCoor, leftIZCoor, middleIZCoor, rightGoalCoor, playersOwnTeam)
				# plotZone(leftGoalCoor, rightIZCoor, leftIZCoor, middleIZCoor, rightGoalCoor, playersOpponent, playersOwnTeam)
			elif(goal_A_X > 0 and curInPossessionY > goal_Y):
				# print('DDDDDDDDDDDDDD')
				defenderInIZ = determineZone(rightGoalCoor, leftIZCoor, rightIZCoor, middleIZCoor, leftGoalCoor, playersOpponent)
				attackerInIZ = determineZone(rightGoalCoor, leftIZCoor, rightIZCoor, middleIZCoor, leftGoalCoor, playersOwnTeam)
				# plotZone(rightGoalCoor, leftIZCoor, rightIZCoor, middleIZCoor, leftGoalCoor, playersOpponent, playersOwnTeam)

			# print("DEFENDER: ", defenderInIZ)
			# print("ATTACKER: ", attackerInIZ)

		else:#nobody in possession
			continue

		shotDensity = 1 - (distInPossessDefender / distInPossessGoal)
		newAttributes['shotDensityFromDefender'][defenderInBZ.index] = shotDensity[defenderInBZ.index]
		newAttributes['shotDensityForPlayerWithBall'][curInPossession.index] = 1 - math.exp(-1 * constantSD * sum(shotDensity)) #newAttributes['shotDensityFromDefender'][defenderInBZ.index]))

		newAttributes['playerInIZ'][defenderInIZ.index] = 1
		newAttributes['playerInIZ'][attackerInIZ.index] = 1
		majority = defenderInIZ['PlayerID'].count() - attackerInIZ['PlayerID'].count()
		newAttributes['Majority'][curInPossession.index] = majority
		newAttributes['passDensityForPlayerWithBall'][curInPossession.index] = 0.5 + (math.atan(constantPD * majority) / math.pi)
		# print(defenderInIZ['PlayerID'].count(),attackerInIZ['PlayerID'].count(),defenderInIZ['PlayerID'].count()-attackerInIZ['PlayerID'].count())

		# pdb.set_trace()

	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	##### THE STRINGS #####
	tmpShotDensityForPlayerWithBall = 'Shot Density for player with ball.'
	tmpShotDensityFromDefender = 'Shot Density from defender in the Blocking Zone on player with ball.'
	tmpPassDensityForPlayerWithBall = 'Pass Density for player with ball.'
	tmpAngleToGoal = 'Angle to goal for player with ball.'
	tmpPlayerInIZ = 'Player in the Interception Zone.'
	tmpMajority = 'Difference between the number of defenders and attackers within the Interception Zone.'

	attributeLabel_tmp = {'shotDensityForPlayerWithBall': tmpShotDensityForPlayerWithBall, 'shotDensityFromDefender': tmpShotDensityFromDefender, 'passDensityForPlayerWithBall': tmpPassDensityForPlayerWithBall, 'angleToGoal': tmpAngleToGoal, 'playerInIZ': tmpPlayerInIZ, 'Majority': tmpMajority}
	attributeLabel.update(attributeLabel_tmp)
	altogether = pd.concat([rawDict,attributeDict], axis=1)
	altogether.to_csv('D:\\KNVB\\test.csv')

	# pdb.set_trace()

	return attributeDict,attributeLabel

## Of course, you can also create new modules (seperate files), to avoid having a very long file.
## If you do, don't forget to import the module at the top of this file using <import newModule>.

#####################################################################
def teamCentroid_panda(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	###############
	# Use this as an example to compute a GROUP level variable. Pay attention to the indexing. Let me know if you have an easier way.
	###############
	
	##### THE DATA #####
	# Prepare the indices of the groupRows. This will be used to store the computed values in the index corresponding to attributeDict.
	ind_groupRows = attributeDict[rawDict['PlayerID'] == 'groupRow'].index
	ind_groupRowsA = ind_groupRows
	ind_groupRowsB = ind_groupRows
	# Separate the raw values per team
	dfA = rawDict[rawDict['TeamID'] == TeamAstring]
	dfB = rawDict[rawDict['TeamID'] == TeamBstring]
	# Pivot X and Y dataframes for Team A
	Team_A_X = dfA.pivot(columns='Ts', values='X')
	Team_A_Y = dfA.pivot(columns='Ts', values='Y')
	# Pivot X and Y dataframes for Team B
	Team_B_X = dfB.pivot(columns='Ts', values='X')
	Team_B_Y = dfB.pivot(columns='Ts', values='Y')   

	# The warnings below were included to debug problems with different lengths per team and/or group row
	if len(ind_groupRows) != Team_A_X.shape[1]:
		warn('\nWARNING: (potentially fatal) The number of groupRows does not correspond to the number of entries identified for <%s>.\nMOST LIKELY CAUSE: There are less ''groupRows'' than there are unique timeStamps\nAlternative reasons: This is either due to incorrect identification of groupRows (and subsequent allocation of the string <groupRow> in its TeamID).\nOr, this could be due to issues with identifying <%s>.\nUPDATE: Should now be cleaned in cleanupData.py in verifyGroupRows().\n' %(TeamAstring,TeamAstring))
	if len(ind_groupRows) != Team_B_X.shape[1]:
		warn('\nWARNING: (potentially fatal) The number of groupRows does not correspond to the number of entries identified for <%s>.\nMOST LIKELY CAUSE: There are less ''groupRows'' than there are unique timeStamps\nAlternative reasons: This is either due to incorrect identification of groupRows (and subsequent allocation of the string <groupRow> in its TeamID).\nOr, this could be due to issues with identifying <%s>.\nUPDATE: Should now be cleaned in cleanupData.py in verifyGroupRows().\n' %(TeamBstring,TeamBstring))
	if Team_A_X.shape[1] != Team_B_X.shape[1]:
		warn('\nWARNING: (potentially fatal) \n\
	The number of rows (i.e., unique timestamps) identified for <%s> (n = %s frames) != <%s> (n = %s frames).\n\
	In other words. For some period of time, only players from one team were registered.\n\
	HERE I solved it by making the computation for each team separately.\n\
	Alternatively, we could: \n\
		1) clean up the data to include missing timestamps (or exclude timestamps that only occur for one team)\n\
		2) Write the analysis to only compute the spatial aggregate when there is data from both teams.\n\
	NB: As long as the difference in n is small (e.g., less than a second), the impact is minimal.\n\
#######################################################################################################'\
 %(TeamAstring,Team_A_X.shape[1],TeamBstring,Team_B_X.shape[1]))

		# Index of the groupRows for every unique timestamp that exists for each team separately
		uniqueTs_TeamA_Rows = rawDict['Ts'][rawDict['TeamID'] == TeamAstring].unique()
		ind_groupRowsA = [i for i in ind_groupRows if np.isin(rawDict['Ts'][i],uniqueTs_TeamA_Rows)]
		
		uniqueTs_TeamB_Rows = rawDict['Ts'][rawDict['TeamID'] == TeamBstring].unique()
		ind_groupRowsB = [i for i in ind_groupRows if np.isin(rawDict['Ts'][i],uniqueTs_TeamB_Rows)]

	# Create empty DataFrame to store results, NB: columns need to be assigend beforehand.
	# newAttributes = pd.DataFrame(index = ind_groupRows,columns = ['TeamCentXA', 'TeamCentYA', 'LengthA', 'WidthA', 'TeamCentXB', 'TeamCentYB', 'LengthB', 'WidthB'])
	newAttributesA = pd.DataFrame(index = ind_groupRowsA,columns = ['TeamCentXA', 'TeamCentYA', 'LengthA', 'WidthA'])	
	newAttributesB = pd.DataFrame(index = ind_groupRowsB,columns = ['TeamCentXB', 'TeamCentYB', 'LengthB', 'WidthB'])

	# Compute the new attributes and store them with the index that corresponds to attributeDict
	pd.options.mode.chained_assignment = None  # default='warn' # NB: The code below gives a warning because it may be uncertain whether the right ind_groupRows are called. If you know a work-around, let me know.
		
	# For team A
	newAttributesA['TeamCentXA'][ind_groupRowsA] = Team_A_X.mean(axis=0, skipna=True)
	newAttributesA['TeamCentYA'][ind_groupRowsA] = Team_A_Y.mean(axis=0, skipna=True)
	newAttributesA['LengthA'][ind_groupRowsA] = Team_A_X.max(axis=0, skipna=True) - Team_A_X.min(axis=0, skipna=True)
	newAttributesA['WidthA'][ind_groupRowsA] = Team_A_Y.max(axis=0, skipna=True) - Team_A_Y.min(axis=0, skipna=True)
	# And for team B
	newAttributesB['TeamCentXB'][ind_groupRowsB] = Team_B_X.mean(axis=0, skipna=True)
	newAttributesB['TeamCentYB'][ind_groupRowsB] = Team_B_Y.mean(axis=0, skipna=True)
	newAttributesB['LengthB'][ind_groupRowsB] = Team_B_X.max(axis=0, skipna=True) - Team_B_X.min(axis=0, skipna=True)
	newAttributesB['WidthB'][ind_groupRowsB] = Team_B_Y.max(axis=0, skipna=True) - Team_B_Y.min(axis=0, skipna=True)
	pd.options.mode.chained_assignment = 'warn'  # default='warn'

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributesA, newAttributesB], axis=1)
	
	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	tmpXAString = 'X-position of %s (m)' %TeamAstring
	tmpYAString = 'Y-position of %s (m)' %TeamAstring
	tmpLengthAString = 'Distance along Y-axis %s (m)' %TeamAstring
	tmpWidthAString = 'Distance along X-axis %s (m)' %TeamAstring
	
	tmpXBString = 'X-position of %s (m)' %TeamBstring
	tmpYBString = 'Y-position of %s (m)' %TeamBstring			
	tmpLengthBString = 'Distance along Y-axis %s (m)' %TeamBstring
	tmpWidthBString = 'Distance along X-axis %s (m)' %TeamBstring
	
	attributeLabel_tmp = {'TeamCentXA': tmpXAString, 'TeamCentYA': tmpYAString, 'LengthA': tmpLengthAString,'WidthA': tmpWidthAString,\
	'TeamCentXB': tmpXBString,'TeamCentYB': tmpYBString,'LengthB': tmpLengthBString,'WidthB': tmpWidthBString}
	attributeLabel.update(attributeLabel_tmp)

	return attributeDict,attributeLabel


############################################################################

def distanceToCentroid(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	###############
	# Use this as an example to compute a PLAYER level variable. Pay attention to the indexing. Let me know if you have an easier way.
	###############
	
	##### THE DATA #####
	# In this case, the new attribute will be computed based on a group (i.e., team) value
	TeamVals = attributeDict[rawDict['PlayerID'] == 'groupRow'].set_index('Ts')
	# Create empty DataFrame to store results, NB: columns need to be assigend beforehand.
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distToCent'])
	
	# For every player in the dataFrame
	for idx,i in enumerate(pd.unique(rawDict['PlayerID'])):
		curPlayer = rawDict[rawDict['PlayerID'] == i]
		curPlayerDict = curPlayer.set_index('Ts')

		if all(curPlayer['PlayerID'] == 'groupRow'):
			# It's actually not a player, but a group, so skip it.
			continue # do nothing
		elif all(curPlayer['PlayerID'] == 'ball'):
			# It's actually not a player, but the ball, so skip it.
			continue # do nothing
		elif all(curPlayer['TeamID'] == TeamAstring):
			# Compute the distance to the centroid, NB: team specific!!
			curPlayer_distToCent = np.sqrt((curPlayerDict['X'] - TeamVals['TeamCentXA'])**2 + (curPlayerDict['Y'] - TeamVals['TeamCentYA'])**2)
		elif all(curPlayer['TeamID'] == TeamBstring):
			# Compute the distance to the centroid, NB: team specific!!
			curPlayer_distToCent = np.sqrt((curPlayerDict['X'] - TeamVals['TeamCentXB'])**2 + (curPlayerDict['Y'] - TeamVals['TeamCentYB'])**2)

		# Put compute values in the right place in the dataFrame
		newAttributes['distToCent'][curPlayer.index] = curPlayer_distToCent[curPlayerDict.index]

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	tmpDistToCentString = 'Player\'s distance to its team\'s centroid (m)'
	attributeLabel.update({'distToCent':tmpDistToCentString})
	
	return attributeDict,attributeLabel