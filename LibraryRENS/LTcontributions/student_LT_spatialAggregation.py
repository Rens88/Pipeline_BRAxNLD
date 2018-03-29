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
from warnings import warn
import pdb;

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

	attributeDict,attributeLabel = \
	ballPossession(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	attributeDict,attributeLabel = \
	zone(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	attributeDict,attributeLabel = \
	control(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	# attributeDict,attributeLabel = \
	# pressure(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	# NB: Centroid and distance to centroid are stored in example variables that are not exported
	# when 'process' is finished, because these features are already embedded in the main pipeline.
	# Make sure that the name of the output variables that you create corresponds to the variables
	# that this function returns (i.e., 'attributeDict' and 'attributeLabel').

	return attributeDict,attributeLabel

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

def zone(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	#standard KNVB settings
	length = 105
	width = 68
	beginZone = 34
	zoneMin_X = length / 2 - beginZone

	##############   READ ZONE CSV   ###############
	dirPath = os.path.dirname(os.path.realpath(__file__))
	fileName = dirPath + '\\Zone.csv'
	zoneMatrix = np.loadtxt(open(fileName, 'r'),delimiter=os.pathsep)

	##############   DETERMINE SIDE   ###############
	team_A = rawDict[rawDict['TeamID'] == TeamAstring]
	team_B = rawDict[rawDict['TeamID'] == TeamBstring]

	#only timestamp 0
	team_A_Begin = team_A[team_A['Ts'] == 0]
	team_B_Begin = team_B[team_B['Ts'] == 0]

	#sum of all X positions of the players per team at timestamp 0
	team_A_Begin_X = sum(team_A_Begin['X'])
	team_B_Begin_X = sum(team_B_Begin['X'])

	#LT: switch sides at half time? When is it half time?
	#determine zone X of both teams
	if(team_A_Begin_X < 0 and team_B_Begin_X > 0):
		zoneA = -1
		zoneB = 1
		zoneMin_A_X = (length / 2) - beginZone
		zoneMax_A_X = (length / 2)
		zoneMin_B_X = (length / 2) * -1
		zoneMax_B_X = (length / 2) *-1 + beginZone
	elif(team_A_Begin_X > 0 and team_B_Begin_X < 0):
		zoneA = 1
		zoneB = -1
		zoneMin_A_X = (length / 2) * -1
		zoneMax_A_X = (length / 2) *-1 + beginZone
		zoneMin_B_X = (length / 2) - beginZone
		zoneMax_B_X = (length / 2)
	else:
		warn('\nWARNING: Cannot determine the zone, because the players of the teams are not on the same side at timestamp 0, or there is no timestamp 0.\n')
		return attributeDict,attributeLabel

	#determine zone Y, For both teams the same
	zoneMin_Y = (width / 2) * -1
	zoneMax_Y = (width / 2)

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

	altogether = pd.concat([rawDict,attributeDict], axis=1)
	altogether.to_csv('D:\\KNVB\\test.csv')

	return attributeDict,attributeLabel

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
	tmpControl = 'Control of the player with ball.'

	attributeLabel_tmp = {'avgRelSpeedPlayerBall': tmpAvgRelSpeedPlayerBall, 'control': tmpControl}
	attributeLabel.update(attributeLabel_tmp)

	# altogether = pd.concat([rawDict,attributeDict], axis=1)
	# altogether.to_csv('D:\\KNVB\\test.csv')
	
	return attributeDict,attributeLabel

def pressure(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	#Only players in possession
	inPossession = rawDict[attributeDict['inPossession'] == 1]
	notInPossession = rawDict[attributeDict['inPossession'] == 0]

	for idx,i in enumerate(pd.unique(rawDict['Ts'])):
		curTime = rawDict['Ts'][idx]
		inPossessionX = inPossession['X'][inPossession['Ts'] == curTime]
		inPossessionY = inPossession['Y'][inPossession['Ts'] == curTime]
		notInPossessionX = notInPossession['X'][notInPossession['Ts'] == curTime]
		notInPossessionY = notInPossession['Y'][notInPossession['Ts'] == curTime]

		print(curTime,inPossession['TeamID'][inPossession['Ts'] == curTime])

		pdb.set_trace()

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