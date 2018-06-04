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
from collections import Counter
import re

#Standard KNVB settings
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
	process(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg)

	# This is an example that can be used to see how you compute a group level variable.
	teamCentroid_panda(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)
	# This is an example that can be used to see how to compute an individual level variable.
	distanceToCentroid(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

def timing(f):
    def wrap(*args):
        time1 = time.time()
        ret = f(*args)
        time2 = time.time()
        print('%s function took %0.3f s' % (f.__name__, (time2-time1)))
        return ret
    return wrap

## Here, you specifiy what each function does
def process(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg,targetEvents):
	# Use this is an example for a GROUP level aggregate
	# attributeDict_EXAMPLE,attributeLabel_EXAMPLE = \
	# teamCentroid_panda(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	# # Use this is an example for a PLAYER level aggregate
	# attributeDict_EXAMPLE,attributeLabel_EXAMPLE = \
	# distanceToCentroid(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	# attributeDict,attributeLabel = \
	# heatMap(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	#DataFrame aanmaken met alle tijdstippen waarop er wordt gepasst
	passList = [ i[0] for i in targetEvents['pass']]
	tmp = pd.DataFrame(passList)
	tmp.columns = ['PassTimes']
	passDF = tmp.sort_values('PassTimes', ascending=True)
	passDF.columns = ['PassTimes']

	pd.options.mode.chained_assignment = None

	#print("Start ballPossession")
	#attributeDict,attributeLabel = \
	#ballPossession(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	#print("Start distanceToOpponent")
	#attributeDict,attributeLabel = \
	#distanceToOpponent(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	print("Start distanceToGoal")
	attributeDict,attributeLabel = \
	distanceToGoal(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF)

	#print("Start angleOpponentToPassline")
	#attributeDict,attributeLabel = \
	#angleOpponentToPassline(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF)

	print("Start distanceToInPossession")
	attributeDict,attributeLabel = \
	distanceToInPossession(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF)

	print("Start ratePlayersPerFeature")
	attributeDict,attributeLabel = \
	ratePlayersPerFeature(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF)

	print("Start ratePlayers")
	attributeDict,attributeLabel = \
	ratePlayers(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF)


	# NB: Centroid and distance to centroid are stored in example variables that are not exported
	# when 'process' is finished, because these features are already embedded in the main pipeline.
	# Make sure that the name of the output variables that you create corresponds to the variables
	# that this function returns (i.e., 'attributeDict' and 'attributeLabel').

	return attributeDict,attributeLabel

#####################################################################
def teamCentroid_panda(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg):
	###############
	# Use this as an example to compute a GROUP level variable. Pay attention to the indexing. Let me know if you have an easier way.
	###############

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

	if skipSpatAgg:
		return attributeDict,attributeLabel
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
	pd.options.mode.chained_assignment = 'None'  # default='warn' # NB: The code below gives a warning because it may be uncertain whether the right ind_groupRows are called. If you know a work-around, let me know.

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
	pd.options.mode.chained_assignment = 'None'  # default='warn'

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

def determineGoalKeepers(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF):
	#Player closest to own goal at TS 0.0 should be the goalkeeper
	dictionary = pd.concat([rawDict, attributeDict], axis=1)
	dictionary = dictionary.loc[:,~dictionary.columns.duplicated()]
	tmp = dictionary[dictionary['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']
	players = players[players['Ts'] == 0.0]
	playersTeamA = players[players['TeamID'] == TeamAstring]
	playersTeamB = players[players['TeamID'] == TeamBstring]

	goalkeeperA = playersTeamA[playersTeamA['distanceToOwnGoal'] == min(playersTeamA['distanceToOwnGoal'])]
	goalkeeperB = playersTeamB[playersTeamB['distanceToOwnGoal'] == min(playersTeamB['distanceToOwnGoal'])]

	return goalkeeperA['PlayerID'].item(),goalkeeperB['PlayerID'].item()

def distanceToInPossessionOLD(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF):
	def distance(X_1,Y_1,X_2,Y_2):
		return np.sqrt((X_1 - X_2)**2 + (Y_1 - Y_2)**2)

	goalkeeperA,goalkeeperB = determineGoalKeepers(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF)
	#All the players on the field
	tmp = rawDict[rawDict['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']
	players = players[players['PlayerID'] != goalkeeperA]
	players = players[players['PlayerID'] != goalkeeperB]

	#Create new attribute distanceToGoal
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distanceToInPossession'])

	#Create list with players who are in possession
	inPossession = rawDict[attributeDict['InBallPos'] == 1]

def distanceToInPossession(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF):
	def distance(X_1,Y_1,X_2,Y_2):
		return np.sqrt((X_1 - X_2)**2 + (Y_1 - Y_2)**2)

	goalkeeperA,goalkeeperB = determineGoalKeepers(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF)
	#All the players on the field
	tmp = rawDict[rawDict['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']
	players = players[players['PlayerID'] != goalkeeperA]
	players = players[players['PlayerID'] != goalkeeperB]

	#Create new attribute distanceToGoal
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distanceToInPossession'])

	#Create list with players who are in possession
	inPossession = rawDict[attributeDict['InBallPos'] == 1]

	for idx,i in enumerate(pd.unique(players['PlayerID'])):
		curPlayer = players[players['PlayerID'] == i]
		curPlayerDict = curPlayer.set_index('Ts')
		inPossessionDict = inPossession.set_index('Ts')

		if all(curPlayer['PlayerID'] == 'groupRow'):
			# It's actually not a player, but a group, so skip it.
			continue # do nothing
		elif all(curPlayer['PlayerID'] == 'ball'):
			# It's actually not a player, but the ball, so skip it.
			continue # do nothing
		else:
			curPlayer_distToInPossession = distance(curPlayerDict['X'], curPlayerDict['Y'], inPossessionDict['X'], inPossessionDict['Y'])

		# Put compute values in the right place in the dataFrame
		newAttributes['distanceToInPossession'][curPlayer.index] = curPlayer_distToInPossession[curPlayerDict.index]

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	tmpdistanceToInPossession = 'Distance to inPossession'

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'distanceToInPossession': tmpdistanceToInPossession}
	attributeLabel.update(attributeLabel_tmp)
	altogether = pd.concat([rawDict,attributeDict], axis=1)
	altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

	return attributeDict,attributeLabel


############################################################################
def ballPossession(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	def distance(X_1,Y_1,X_2,Y_2):
		return np.sqrt((X_1 - X_2)**2 + (Y_1 - Y_2)**2)

	#ball
	ballComplete = rawDict[rawDict['PlayerID'] == 'ball']

	#only players
	tmp = rawDict[rawDict['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']

	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distToBall', 'inPossession'])
	newAttributes['inPossession'] = 0

	# For every ball
	for idx,i in enumerate(ballComplete['Ts']):
		curTime = i

		curBallX = ballComplete['X'][ballComplete['Ts'] == curTime]
		curBallY = ballComplete['Y'][ballComplete['Ts'] == curTime]

		# Take all corresponding Ts (for PlayerID != 'groupRow' and 'ball')
		curPlayersX = players['X'][players['Ts'] == curTime]
		curPlayersY = players['Y'][players['Ts'] == curTime]

		curPlayersID = players['PlayerID'][players['Ts'] == curTime]

		curPlayer_distToBall = distance(players['X'][players['Ts'] == curTime], players['Y'][players['Ts'] == curTime], float(curBallX), float(curBallY))
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
	newAttributes.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')
	return attributeDict,attributeLabel

def halfTime(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	players = rawDict[(rawDict['PlayerID'] != 'ball') & (rawDict['PlayerID'] != 'groupRow')]
	noPlayers = 0

	for idx,i in enumerate(pd.unique(rawDict['Ts'])):
		curTime = i
		curPlayer = players[players['Ts'] == curTime]
		if(len(curPlayer.index) == 0):
			noPlayers = noPlayers + 1
		else:
			noPlayers = 0

		if (noPlayers == 600):
			return curTime

	return -1

def angleOpponentToPassline(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF):
	def distance(X_1,Y_1,X_2,Y_2):
		return np.sqrt((X_1 - X_2)**2 + (Y_1 - Y_2)**2)


	goalkeeperA,goalkeeperB = determineGoalKeepers(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF)

	#Create new attribute distanceToGoal
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['angleOpponentToPassline'])

	inPossession = rawDict.ix[attributeDict['InBallPos'] == 1]

	players = rawDict[(rawDict['PlayerID'] != 'ball') & (rawDict['PlayerID'] != 'groupRow')]
	players = players[players['PlayerID'] != goalkeeperA]
	players = players[players['PlayerID'] != goalkeeperB]

	for idx,i in enumerate(pd.unique(passDF['PassTimes'])):
		curTime = i
		curPlayer = players[players['Ts'] == curTime]
		curInPossession = inPossession[inPossession['Ts'] == curTime]
		curTeamInPossession = inPossession['TeamID'][inPossession['Ts'] == curTime]
		curInPossessionX = inPossession['X'][inPossession['Ts'] == curTime]
		curInPossessionY = inPossession['Y'][inPossession['Ts'] == curTime]
		curTeamA = curPlayer[curPlayer['TeamID'] == TeamAstring]
		curTeamB = curPlayer[curPlayer['TeamID'] == TeamBstring]

		if all(np.isnan(curInPossessionX)) or all(np.isnan(curInPossessionY)):
			continue; #No x- or y-coordinate for a player in possession found

		else:
			curInPossessionX = float(curInPossessionX)
			curInPossessionY = float(curInPossessionY)

		if all(curTeamInPossession == TeamAstring):
			for idx,j in enumerate(pd.unique(curTeamA['PlayerID'])):
				angleToPassline = 180.00
				currentPlayerA = curTeamA[curTeamA['PlayerID'] == j]
				if(str(currentPlayerA['PlayerID']) == str(curInPossession['PlayerID'])):
					continue
				currentPlayerA_X = float(currentPlayerA['X'])
				currentPlayerA_Y = float(currentPlayerA['Y'])
				lengthPassline = distance(curInPossessionX, curInPossessionY, currentPlayerA_X, currentPlayerA_Y)
				for idx,k in enumerate(pd.unique(curTeamB['PlayerID'])):
					currentPlayerB = curTeamB[curTeamB['PlayerID'] == k]
					currentPlayerB_X = float(currentPlayerB['X'])
					currentPlayerB_Y = float(currentPlayerB['Y'])
					distToInPossession = distance(curInPossessionX, curInPossessionY, currentPlayerB_X, currentPlayerB_Y)
					if (float(distToInPossession > lengthPassline)):
						continue
					tmp_AngleToPassline = np.degrees(np.arccos(distToInPossession / lengthPassline))
					if (tmp_AngleToPassline < angleToPassline):
						angleToPassline = tmp_AngleToPassline
				newAttributes['angleOpponentToPassline'][currentPlayerA.index] = angleToPassline

		elif all(curTeamInPossession == TeamBstring):
			for idx,j in enumerate(pd.unique(curTeamB['PlayerID'])):
				angleToPassline = 180.00
				currentPlayerB = curTeamB[curTeamB['PlayerID'] == j]
				if(str(currentPlayerB['PlayerID']) == str(curInPossession['PlayerID'])):
					continue
				currentPlayerB_X = float(currentPlayerB['X'])
				currentPlayerB_Y = float(currentPlayerB['Y'])
				lengthPassline = distance(curInPossessionX, curInPossessionY, currentPlayerB_X, currentPlayerB_Y)
				for idx,k in enumerate(pd.unique(curTeamA['PlayerID'])):
					currentPlayerA = curTeamA[curTeamA['PlayerID'] == k]
					currentPlayerA_X = float(currentPlayerA['X'])
					currentPlayerA_Y = float(currentPlayerA['Y'])
					distToInPossession = distance(curInPossessionX, curInPossessionY, currentPlayerA_X, currentPlayerA_Y)
					if (float(distToInPossession > lengthPassline)):
						continue
					tmp_AngleToPassline = np.degrees(np.arccos(distToInPossession / lengthPassline))
					if (tmp_AngleToPassline < angleToPassline):
						angleToPassline = tmp_AngleToPassline
				newAttributes['angleOpponentToPassline'][currentPlayerB.index] = angleToPassline
		else:
			continue

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	angleToPassline = 'Angle from opponent to the passline'

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'angleOpponentToPassline': angleToPassline}
	attributeLabel.update(attributeLabel_tmp)
	altogether = pd.concat([rawDict,attributeDict], axis=1)
	altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

	return attributeDict,attributeLabel

def distanceToOpponent(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	def distance(X_1,Y_1,X_2,Y_2):
		return np.sqrt((X_1 - X_2)**2 + (Y_1 - Y_2)**2)

	#All the players on the field
	tmp = rawDict[rawDict['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']

	#Create new attribute distanceToGoal
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distanceToOpponent'])

	for idx,i in enumerate(pd.unique(rawDict['PlayerID'])):
		curPlayer = rawDict[rawDict['PlayerID'] == i]
		curPlayerDict = curPlayer.set_index('Ts')
		playersTeamA = players[players['TeamID'] == TeamAstring]
		playersTeamADict = playersTeamA.set_index('Ts')
		playersTeamB = players[players['TeamID'] == TeamBstring]
		playersTeamBDict = playersTeamB.set_index('Ts')

		if all(curPlayer['PlayerID'] == 'groupRow'):
			# It's actually not a player, but a group, so skip it.
			continue # do nothing
		elif all(curPlayer['PlayerID'] == 'ball'):
			# It's actually not a player, but the ball, so skip it.
			continue # do nothing
		elif all(curPlayer['TeamID'] == TeamAstring):
			for index,j in enumerate(pd.unique(rawDict['Ts'])):
				curTime = j
				currentPlayer = curPlayer[curPlayer['Ts'] == curTime]
				currentPlayerDict = currentPlayer.set_index('Ts')
				currentPlayersTeamB = playersTeamB[playersTeamB['Ts'] == curTime]
				currentPlayersTeamBDict = currentPlayersTeamB.set_index('Ts')
				curPlayer_distToOpponent = min(distance(currentPlayerDict['X'], currentPlayerDict['Y'], currentPlayersTeamBDict['X'], currentPlayersTeamBDict['Y']))
				newAttributes['distanceToOpponent'][currentPlayer.index] = curPlayer_distToOpponent
		elif all(curPlayer['TeamID'] == TeamBstring):
			for index,j in enumerate(pd.unique(rawDict['Ts'])):
				curTime = j
				currentPlayer = curPlayer[curPlayer['Ts'] == curTime]
				currentPlayerDict = currentPlayer.set_index('Ts')
				currentPlayersTeamA = playersTeamA[playersTeamA['Ts'] == curTime]
				currentPlayersTeamADict = currentPlayersTeamA.set_index('Ts')
				curPlayer_distToOpponent = min(distance(currentPlayerDict['X'], currentPlayerDict['Y'], currentPlayersTeamADict['X'], currentPlayersTeamADict['Y']))
				newAttributes['distanceToOpponent'][currentPlayer.index] = curPlayer_distToOpponent

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	distanceToOpponent = 'Distance to nearest opponent'

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'distanceToOpponent': newAttributes['distanceToOpponent']}
	attributeLabel.update(attributeLabel_tmp)
	altogether = pd.concat([rawDict,attributeDict], axis=1)
	altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

	return attributeDict,attributeLabel

def distanceInPossessionToPlayers(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF):
	def distance(X_1,Y_1,X_2,Y_2):
		return np.sqrt((X_1 - X_2)**2 + (Y_1 - Y_2)**2)

	#All the players on the field
	tmp = rawDict[rawDict['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']

	#Create new attribute distanceToGoal
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distanceToInPossession'])

	#Players in possession
	inPossession = rawDict[attributeDict['inPossession'] == 1]

	for idx,i in enumerate(passDF['PassTimes']):
		curPlayer = rawDict[rawDict['Ts'] == i]
		curPlayerDict = curPlayer.set_index('Ts')

		if all(curPlayer['PlayerID'] == 'groupRow'):
			# It's actually not a player, but a group, so skip it.
			continue # do nothing
		elif all(curPlayer['PlayerID'] == 'ball'):
			# It's actually not a player, but the ball, so skip it.
			continue # do nothing
		else:
			# Compute the distance to the centroid, NB: team specific!!
			curTime = i

			curPossessionX = inPossession['X'][idx]
			curPossessionY = inPossession['Y'][idx]

			# Take all corresponding Ts (for PlayerID != 'groupRow' and 'ball')
			curPlayersX = players['X'][players['Ts'] == curTime]
			curPlayersY = players['Y'][players['Ts'] == curTime]

			curPlayersID = players['PlayerID'][players['Ts'] == curTime]

			curPlayer_distToPossession = distance(curPlayersX, curPlayersY, curPossessionX, curPossessionY)
			newAttributes['distanceToInPossession'][curPlayer_distToPossession.index] = curPlayer_distToPossession[curPlayer_distToPossession.index]

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	tmpdistanceToOpponentGoal = 'Distance to player in possession'

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'distanceToOpponentGoal': tmpdistanceToOpponentGoal}
	attributeLabel.update(attributeLabel_tmp)
	altogether = pd.concat([rawDict,attributeDict], axis=1)
	altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

	return attributeDict,attributeLabel

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

def distanceToGoal(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF):
	def distance(X_1,Y_1,X_2,Y_2):
		return np.sqrt((X_1 - X_2)**2 + (Y_1 - Y_2)**2)

	#All the players on the field
	tmp = rawDict[rawDict['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']

	#Create new attribute distanceToGoal
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distanceToOpponentGoal', 'distanceToOwnGoal'])

	#players in possession
	inPossession = rawDict[attributeDict['InBallPos'] == 1]

	#Set variables to appropriate values
	leftSide, goal_A_X, goal_B_X, goal_Y = determineSide(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

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
			curPlayer_distOwnGoal = distance(curPlayerDict['X'], curPlayerDict['Y'], goal_A_X, goal_Y)
			curPlayer_distOppGoal = distance(curPlayerDict['X'], curPlayerDict['Y'], goal_B_X, goal_Y)
		elif all(curPlayer['TeamID'] == TeamBstring):
			# Compute the distance to the centroid, NB: team specific!!
			curPlayer_distOwnGoal = distance(curPlayerDict['X'], curPlayerDict['Y'], goal_B_X, goal_Y)
			curPlayer_distOppGoal = distance(curPlayerDict['X'], curPlayerDict['Y'], goal_A_X, goal_Y)
		# Put compute values in the right place in the dataFrame
		newAttributes['distanceToOpponentGoal'][curPlayer.index] = curPlayer_distOppGoal[curPlayerDict.index]
		newAttributes['distanceToOwnGoal'][curPlayer.index] = curPlayer_distOwnGoal[curPlayerDict.index]

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	tmpdistanceToOpponentGoal = 'Distance to opponent\'s goal'
	tmpdistanceToOwnGoal = 'Distance to own goal'
		##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'distanceToOpponentGoal': tmpdistanceToOpponentGoal, 'distanceToOwnGoal': tmpdistanceToOwnGoal}
	attributeLabel.update(attributeLabel_tmp)
	altogether = pd.concat([rawDict,attributeDict], axis=1)
	altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

	return attributeDict,attributeLabel

def second_smallest(numbers):
    m1, m2 = float('inf'), float('inf')
    for x in numbers:
        if x <= m1:
            m1, m2 = x, m1
        elif x < m2:
            m2 = x
    return m2

def ratePlayersPerFeature(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF):
	def distance(X_1,Y_1,X_2,Y_2):
		return np.sqrt((X_1 - X_2)**2 + (Y_1 - Y_2)**2)

	#Create new attribute for the positioning positioningRating
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distanceToOpponentRating', 'angleToPasslineRating', 'distanceToPossessionRating', 'distanceToOpponentGoalRating'])

	dictionary = pd.concat([rawDict, attributeDict], axis=1)
	#dictionary = pd.read_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv',low_memory=False)
	dictionary = dictionary.loc[:,~dictionary.columns.duplicated()]

	goalkeeperA,goalkeeperB = determineGoalKeepers(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF)
	#All the players on the field
	tmp = dictionary[dictionary['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']
	players = players[players['PlayerID'] != goalkeeperA]
	players = players[players['PlayerID'] != goalkeeperB]

	inPossession = dictionary[dictionary['InBallPos'] == 1]

	for idx,i in enumerate(passDF['PassTimes']):
		curTime = i
		curPlayer = players[players['Ts'] == curTime]
		curInPossession = inPossession[inPossession['Ts'] == curTime]
		curTeamInPossession = inPossession['TeamID'][inPossession['Ts'] == curTime]
		curInPossessionX = inPossession['X'][inPossession['Ts'] == curTime]
		curInPossessionY = inPossession['Y'][inPossession['Ts'] == curTime]
		curInPossessionID = inPossession['PlayerID'][inPossession['Ts'] == curTime]
		curTeamA = curPlayer[curPlayer['TeamID'] == TeamAstring]
		curTeamA = curTeamA[curTeamA.InBallPos != 1]
		curTeamADict = curTeamA.set_index('Ts')
		curTeamB = curPlayer[curPlayer['TeamID'] == TeamBstring]
		curTeamB = curTeamB[curTeamB.InBallPos != 1]
		curTeamBDict = curTeamB.set_index('Ts')

		if all(np.isnan(curInPossessionX)) or all(np.isnan(curInPossessionY)):
			continue; #No x- or y-coordinate for a player in possession found

		else:
			curInPossessionX = float(curInPossessionX)
			curInPossessionY = float(curInPossessionY)

		if all(curTeamInPossession == TeamAstring):
			#Rate distance to nearest opponent
			minDistance = min(curTeamA['dist to closest visitor'])
			maxDistance = max(curTeamA['dist to closest visitor'])
			distanceToOpponentRating = (curTeamA['dist to closest visitor'] - minDistance) / (maxDistance - minDistance) * 9 + 1

			#Rate angle to Opponent
			minAngle = min(curTeamA['angleOpponentToPassline'])
			maxAngle = max(curTeamA['angleOpponentToPassline'])
			angleRating = (curTeamA['angleOpponentToPassline'] - minAngle) / (maxAngle - minAngle) * 9 + 1

			#Rate distance to player in possession
			minDistanceToPossession = min(curTeamA['distanceToInPossession'])
			maxDistanceToPossession = max(curTeamA['distanceToInPossession'])
			distanceToPossessionRating = (curTeamA['distanceToInPossession'] - minDistanceToPossession) / (maxDistanceToPossession - minDistanceToPossession) * 9 + 1

			#Rate decrease in distance to goal
			distanceToGoal = float(curInPossession['distanceToOpponentGoal']) - curTeamA['distanceToOpponentGoal']
			maxDistanceToGoal = max(distanceToGoal)
			distanceToGoal2 = distanceToGoal.copy()
			distanceToGoal2[distanceToGoal2 < 0] = maxDistanceToGoal
			minDistanceToGoal = min(distanceToGoal2)
			if (maxDistanceToGoal - minDistanceToGoal == 0.0):
				distanceToGoalRating = distanceToGoal - minDistanceToGoal - 1000000
			else:
				distanceToGoalRating = (distanceToGoal - minDistanceToGoal) / (maxDistanceToGoal - minDistanceToGoal) * 9 + 1
			distanceToGoalRating[distanceToGoalRating < 0] = 0

			newAttributes['distanceToOpponentRating'][curTeamA.index] = distanceToOpponentRating
			newAttributes['angleToPasslineRating'][curTeamA.index] = angleRating
			newAttributes['distanceToPossessionRating'][curTeamA.index] = distanceToPossessionRating
			newAttributes['distanceToOpponentGoalRating'][curTeamA.index] = distanceToGoalRating

		elif all(curTeamInPossession == TeamBstring):
			#Rate distance to nearest opponent
			minDistance = min(curTeamB['dist to closest visitor'])
			maxDistance = max(curTeamB['dist to closest visitor'])
			distanceToOpponentRating = (curTeamB['dist to closest visitor'] - minDistance) / (maxDistance - minDistance) * 9 + 1

			#Rate angle to Opponent
			minAngle = min(curTeamB['angleOpponentToPassline'])
			maxAngle = max(curTeamB['angleOpponentToPassline'])
			angleRating = (curTeamB['angleOpponentToPassline'] - minAngle) / (maxAngle - minAngle) * 9 + 1

			#Rate distance to player in possession
			minDistanceToPossession = min(curTeamB['distanceToInPossession'])
			maxDistanceToPossession = max(curTeamB['distanceToInPossession'])
			distanceToPossessionRating = (curTeamB['distanceToInPossession'] - minDistanceToPossession) / (maxDistanceToPossession - minDistanceToPossession) * 9 + 1

			#Rate decrease in distance to goal
			distanceToGoal = float(curInPossession['distanceToOpponentGoal']) - curTeamB['distanceToOpponentGoal']
			maxDistanceToGoal = max(distanceToGoal)
			distanceToGoal2 = distanceToGoal.copy()
			distanceToGoal2[distanceToGoal2 < 0] = maxDistanceToGoal
			minDistanceToGoal = min(distanceToGoal2)
			if (maxDistanceToGoal - minDistanceToGoal == 0.0):
				distanceToGoalRating = distanceToGoal - minDistanceToGoal - 1000000
			else:
				distanceToGoalRating = (distanceToGoal - minDistanceToGoal) / (maxDistanceToGoal - minDistanceToGoal) * 9 + 1
			distanceToGoalRating[distanceToGoalRating < 0] = 0

			newAttributes['distanceToOpponentRating'][curTeamB.index] = distanceToOpponentRating
			newAttributes['angleToPasslineRating'][curTeamB.index] = angleRating
			newAttributes['distanceToPossessionRating'][curTeamB.index] = distanceToPossessionRating
			newAttributes['distanceToOpponentGoalRating'][curTeamB.index] = distanceToGoalRating

		else:
			continue

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	tmp_distanceToOpponentRating = 'Rating (1-10) for the distance a player has to his opponent'
	tmp_angleToPasslineRating = 'Rating (1-10) for the angle an opponent has to the passline'
	tmp_distanceToPossessionRating = 'Rating (1-10) for the distance a player has to his teammate in possession'
	tmp_distanceToOpponentGoalRating = 'Rating (01-10) for distance that a player is closter to the goal to than the player in possession'

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'distanceToOpponentRating': tmp_distanceToOpponentRating, 'angleToPasslineRating': tmp_angleToPasslineRating, 'tmp_distanceToPossessionRating': tmp_distanceToPossessionRating, 'tmp_distanceToOpponentGoalRating': tmp_distanceToOpponentGoalRating}
	attributeLabel.update(attributeLabel_tmp)
	altogether = pd.concat([rawDict,attributeDict], axis=1)
	altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

	return attributeDict,attributeLabel

def ratePlayers(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF):
	#Create new attribute for the positioning positioningRating
	newAttributes = pd.DataFrame(columns = ['PlayerID', 'distToOpRating', 'angleToPasslineRating','distToPossRating','distToOpGoalRating','positionRating'])

	dictionary = pd.concat([rawDict, attributeDict], axis=1)
	dictionary = dictionary.loc[:,~dictionary.columns.duplicated()]

	goalkeeperA,goalkeeperB = determineGoalKeepers(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF)
	#All the players on the field
	tmp = dictionary[dictionary['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']
	players = players[players['PlayerID'] != goalkeeperA]
	players = players[players['PlayerID'] != goalkeeperB]

	for idx,i in enumerate(pd.unique(players['PlayerID'])):
		curPlayer = dictionary[dictionary['PlayerID'] == i]
		curPlayerDict = curPlayer.set_index('Ts')
		if all(curPlayer['PlayerID'] == 'groupRow'):
			# It's actually not a player, but a group, so skip it.
			continue # do nothing
		elif all(curPlayer['PlayerID'] == 'ball'):
			# It's actually not a player, but the ball, so skip it.
			continue # do nothing
		else:
			distToOp = curPlayer['distanceToOpponentRating'].dropna()
			distToOpRating = distToOp.sum() / distToOp.count()
			angleToPassline = curPlayer['angleToPasslineRating'].dropna()
			angleToPasslineRating = angleToPassline.sum() / distToOp.count()
			distToPoss = curPlayer['distanceToPossessionRating'].dropna()
			distToPossRating = distToPoss.sum() / distToPoss.count()
			distToOpGoal = curPlayer['distanceToOpponentGoalRating'].dropna()
			distToOpGoalRating = distToOpGoal.sum() / distToOpGoal.count()
			positionRating = (distToOpRating + angleToPasslineRating + distToPossRating + distToOpGoalRating) / 4
			newAttributes.loc[len(newAttributes)]=[str(pd.unique(curPlayer['PlayerID'])),positionRating]

	newAttributes.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/playerRatings.csv')

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	tmp_positioningRating = 'Rating (1-10) for the positioning of a player over the course of a match'

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'positioningRating': tmp_positioningRating}
	attributeLabel.update(attributeLabel_tmp)
	altogether = pd.concat([rawDict,attributeDict], axis=1)
	altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

	return attributeDict,attributeLabel
