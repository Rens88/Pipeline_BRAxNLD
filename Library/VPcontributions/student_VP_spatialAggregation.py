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

#Op True zetten als een testbestand wordt gebruikt
korteWedstrijd = False

KNVB = True #Geeft aan of de analyse voor de KNVB of voor de Universiteit is

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

	print("Start distanceToGoal")
	attributeDict,attributeLabel = \
	distanceToGoal(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF,skipSpatAgg)

	print("Start distanceToInPossession")
	attributeDict,attributeLabel = \
	distanceToInPossession(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF,skipSpatAgg)

	print("Start angleOpponentToPassline")
	attributeDict,attributeLabel = \
	angleOpponentToPassline(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF,skipSpatAgg)

	print("Start populationBasedRankings")
	attributeDict,attributeLabel = \
	populationBasedRankings(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF,skipSpatAgg)

	print("Start distanceToBall")
	attributeDict,attributeLabel = \
	distanceToBall(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF,skipSpatAgg)

	print("Start zone")
	attributeDict,attributeLabel = \
	zone(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg)

	print("Start ratePlayersPerFeature")
	attributeDict,attributeLabel = \
	rankPlayersPerFeature(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF,skipSpatAgg)

	print("Start Player Passed To")
	attributeDict,attributeLabel = \
	playerPassedToRank(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF,skipSpatAgg)

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

	if skipSpatAgg: # Return eary if spatial aggregation is being skipped
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
	try:
	# 	# Pivotting method:
		# Team_A_X_pivotted = dfA.pivot(columns='Ts', values='X')

		# Groupby method:
		tmp = dfA[['Ts','X','PlayerID']]
		Team_A_X = tmp.groupby(['Ts'])

	except:
		Team_A_X = np.nanmean(dfA['X'])
		print(Team_A_X.shape)
		print(dfA['X'].shape)
		print(len(ind_groupRows))
		warn('Tried to pivot Team_A_X. but it didnt work. Find a different way, because dataset is too large for pivoting')
		pdb.set_trace()

	try:
		##Team_A_Y = dfA.pivot(columns='Ts', values='Y')
		# Groupby method:
		tmp = dfA[['Ts','Y']]
		Team_A_Y = tmp.groupby(['Ts'])

	except:
		Team_A_Y = np.nanmean(dfA['Y'])
		print(dfA.shape)
		warn('Tried to pivot Team_A_Y. but it didnt work. Find a different way, because dataset is too large for pivoting')
		pdb.set_trace()

	# Pivot X and Y dataframes for Team B
	Team_B_X = dfB.pivot(columns='Ts', values='X')
	Team_B_Y = dfB.pivot(columns='Ts', values='Y')

	# The warnings below were included to debug problems with different lengths per team and/or group row
	####### if len(ind_groupRows) != Team_A_X.shape[1]:
	if len(ind_groupRows) != len(Team_A_X):
		warn('\nWARNING: (potentially fatal) The number of groupRows does not correspond to the number of entries identified for <%s>.\nMOST LIKELY CAUSE: There are less ''groupRows'' than there are unique timeStamps\nAlternative reasons: This is either due to incorrect identification of groupRows (and subsequent allocation of the string <groupRow> in its TeamID).\nOr, this could be due to issues with identifying <%s>.\nUPDATE: Should now be cleaned in cleanupData.py in verifyGroupRows().\nALTERNATIVELY: As a result of a non-chronological dataset, the interpolation in cleanup doesnt work well.\n' %(TeamAstring,TeamAstring))
	####### if len(ind_groupRows) != Team_B_X.shape[1]:
	if len(ind_groupRows) != len(Team_B_X):
		warn('\nWARNING: (potentially fatal) The number of groupRows does not correspond to the number of entries identified for <%s>.\nMOST LIKELY CAUSE: There are less ''groupRows'' than there are unique timeStamps\nAlternative reasons: This is either due to incorrect identification of groupRows (and subsequent allocation of the string <groupRow> in its TeamID).\nOr, this could be due to issues with identifying <%s>.\nUPDATE: Should now be cleaned in cleanupData.py in verifyGroupRows().\nALTERNATIVELY: As a result of a non-chronological dataset, the interpolation in cleanup doesnt work well.\n' %(TeamBstring,TeamBstring))
	##############if Team_A_X.shape[1] != Team_B_X.shape[1]:
	if len(Team_A_X) != len(Team_B_X):
		warn('\nWARNING: (potentially fatal) \n\
	The number of rows (i.e., unique timestamps) identified for <%s> (n = %s frames) != <%s> (n = %s frames).\n\
	In other words. For some period of time, only players from one team were registered.\n\
	HERE I solved it by making the computation for each team separately.\n\
	Alternatively, we could: \n\
		1) clean up the data to include missing timestamps (or exclude timestamps that only occur for one team)\n\
		2) Write the analysis to only compute the spatial aggregate when there is data from both teams.\n\
	NB: As long as the difference in n is small (e.g., less than a second), the impact is minimal.\n\
#######################################################################################################'\
 %(TeamAstring,len(Team_A_X),TeamBstring,len(Team_B_X)))
 ############# %(TeamAstring,Team_A_X.shape[1],TeamBstring,Team_B_X.shape[1]))

		# Index of the groupRows for every unique timestamp that exists for each team separately
		uniqueTs_TeamA_Rows = rawDict['Ts'][rawDict['TeamID'] == TeamAstring].unique()
		ind_groupRowsA = [i for i in ind_groupRows if np.isin(rawDict['Ts'][i],uniqueTs_TeamA_Rows)]

		uniqueTs_TeamB_Rows = rawDict['Ts'][rawDict['TeamID'] == TeamBstring].unique()
		ind_groupRowsB = [i for i in ind_groupRows if np.isin(rawDict['Ts'][i],uniqueTs_TeamB_Rows)]


		# print(len(ind_groupRowsA))
		# pdb.set_trace()

		# ind_groupRows_to_be_removed = [i for i in ind_groupRows if not np.isin(rawDict['Ts'][i],uniqueTs_TeamA_Rows)]


		# ind_groupRowsA = ind_groupRows[ind_groupRows != ind_groupRows_to_be_removed]
		# print(len(ind_groupRowsA))
		# print(ind_groupRows_to_be_removed)
		# pdb.set_trace()

		# uniqueTs_GroupRows = df_cleaned['Ts'][df_cleaned['PlayerID'] == 'groupRow'].unique()
		# uniqueTs_TeamA_Rows = df_cleaned['Ts'][df_cleaned['TeamID'] == TeamAstring].unique()

		# Ts_to_be_removed = [i for i in uniqueTs_GroupRows if not np.isin(i,uniqueTs_nonGroupRows)]

		# ind_groupRows = attributeDict[rawDict['PlayerID'] == 'groupRow'].index

	# Create empty DataFrame to store results, NB: columns need to be assigend beforehand.
	# newAttributes = pd.DataFrame(index = ind_groupRows,columns = ['TeamCentXA', 'TeamCentYA', 'LengthA', 'WidthA', 'TeamCentXB', 'TeamCentYB', 'LengthB', 'WidthB'])
	newAttributesA = pd.DataFrame(index = ind_groupRowsA,columns = ['TeamCentXA', 'TeamCentYA', 'LengthA', 'WidthA'])
	newAttributesB = pd.DataFrame(index = ind_groupRowsB,columns = ['TeamCentXB', 'TeamCentYB', 'LengthB', 'WidthB'])

	# Compute the new attributes and store them with the index that corresponds to attributeDict
	pd.options.mode.chained_assignment = 'None'  # default='warn' # NB: The code below gives a warning because it may be uncertain whether the right ind_groupRows are called. If you know a work-around, let me know.

	# For team A
	############## if Team_A_X.shape == (0,0):
	if len(Team_A_X) == 0:
	  exit('\nFATAL WARNING: No data in TeamA\'s X positions.\nThis is likely because the TeamAstring and TeamBstring do not correspond with the strings in the dataset.\nThis may be because the user input is incorrect OR, because the Team strings are unsuccesfully derived from the filename (especialy when using dataType FDP for a new dataset):\nConsider writing a specific function in dissectFilename.py.\n')

	##########newAttributesA['TeamCentXA'][ind_groupRowsA] = Team_A_X.mean(axis=0, skipna=True)
	##########newAttributesA['TeamCentYA'][ind_groupRowsA] = Team_A_Y.mean(axis=0, skipna=True)
	##########newAttributesA['LengthA'][ind_groupRowsA] = Team_A_X.max(axis=0, skipna=True) - Team_A_X.min(axis=0, skipna=True)
	##########newAttributesA['WidthA'][ind_groupRowsA] = Team_A_Y.max(axis=0, skipna=True) - Team_A_Y.min(axis=0, skipna=True)
	# print( Team_A_X.mean())
	# print( len(Team_A_X.mean()))
	# # print(Team_A_X_pivotted.mean(axis=0,skipna=True))
	# # print(len(Team_A_X_pivotted.mean(axis=0,skipna=True)))
	# print(len(ind_groupRowsA))
	# for name, group in Team_A_X:
	# 	print('************')
	# 	print('Name:')
	# 	print(name)
	# 	print('************')
	# 	print('Group:')
	# 	print(group)
	# 	print('************')
	# print(len(Team_A_X))
	# print(len(ind_groupRowsA))
	newAttributesA['TeamCentXA'][ind_groupRowsA] = Team_A_X['X'].mean()
	newAttributesA['TeamCentYA'][ind_groupRowsA] = Team_A_Y['Y'].mean()
	newAttributesA['LengthA'][ind_groupRowsA] = Team_A_X['X'].max() - Team_A_X['X'].min()
	newAttributesA['WidthA'][ind_groupRowsA] = Team_A_Y['Y'].max() - Team_A_Y['Y'].min()

	# And for team B
	newAttributesB['TeamCentXB'][ind_groupRowsB] = Team_B_X.mean(axis=0, skipna=True)
	newAttributesB['TeamCentYB'][ind_groupRowsB] = Team_B_Y.mean(axis=0, skipna=True)
	newAttributesB['LengthB'][ind_groupRowsB] = Team_B_X.max(axis=0, skipna=True) - Team_B_X.min(axis=0, skipna=True)
	newAttributesB['WidthB'][ind_groupRowsB] = Team_B_Y.max(axis=0, skipna=True) - Team_B_Y.min(axis=0, skipna=True)
	pd.options.mode.chained_assignment = 'None'  # default='warn'

	warn('\nUnverified assumption: field width = X-axis, field length = Y-axis\n')

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

def distanceToInPossession(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF,skipSpatAgg):
	def distance(X_1,Y_1,X_2,Y_2):
		return np.sqrt((X_1 - X_2)**2 + (Y_1 - Y_2)**2)

	if skipSpatAgg: # Return early if spatial aggregation is being skipped
		return attributeDict,attributeLabel

	goalkeeperA,goalkeeperB = determineGoalKeepers(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF)
	#All the players on the field
	tmp = rawDict[rawDict['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']
	#We doen vanaf nu inclusief keepers!!
	players = players[players['PlayerID'] != goalkeeperA]
	players = players[players['PlayerID'] != goalkeeperB]

	#Create new attribute distanceToGoal
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distanceToInPossession'])

	newAttributes.loc[players.index,'distanceToInPossession'] = np.nan

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
	# altogether = pd.concat([rawDict,attributeDict], axis=1)
	# altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

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

def angleOpponentToPassline(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF,skipSpatAgg):
	def distance(X_1,Y_1,X_2,Y_2):
		return np.sqrt((X_1 - X_2)**2 + (Y_1 - Y_2)**2)

	if skipSpatAgg: # Return early if spatial aggregation is being skipped
		return attributeDict,attributeLabel

	goalkeeperA,goalkeeperB = determineGoalKeepers(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF)

	#Create new attribute distanceToGoal
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['angleOpponentToPassline'])

	inPossession = rawDict.ix[attributeDict['InBallPos'] == 1]

	tmp = rawDict[rawDict['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']
	#We doen vanaf nu inclusief keepers!!
	players = players[players['PlayerID'] != goalkeeperA]
	players = players[players['PlayerID'] != goalkeeperB]

	ball = rawDict[rawDict['PlayerID'] == 'ball']
	group = rawDict[rawDict['PlayerID'] == 'groupRow']

	newAttributes.loc[players.index,'angleOpponentToPassline'] = np.nan

	#for idx,i in enumerate(pd.unique(rawDict['Ts'])):
	for idx,i in enumerate(pd.unique(passDF['PassTimes'])):
		curTime = i
		if (curTime > 60.0 and korteWedstrijd == True):
			continue
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
				if (angleToPassline == 180.0):
					newAttributes.loc[currentPlayerA.index,'angleOpponentToPassline'] = np.nan
				else:
					newAttributes.loc[currentPlayerA.index,'angleOpponentToPassline'] = angleToPassline

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
				if (angleToPassline == 180.0):
					newAttributes.loc[currentPlayerB.index,'angleOpponentToPassline'] = np.nan
				else:
					newAttributes.loc[currentPlayerB.index,'angleOpponentToPassline'] = angleToPassline

		else:
			continue

	newAttributes.loc[ball.index,'angleOpponentToPassline'] = np.nan
	newAttributes.loc[group.index,'angleOpponentToPassline'] = np.nan
	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	angleToPassline = 'Angle from opponent to the passline'

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'angleOpponentToPassline': angleToPassline}
	attributeLabel.update(attributeLabel_tmp)
	# altogether = pd.concat([rawDict,attributeDict], axis=1)
	# altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

	return attributeDict,attributeLabel

def distanceToBall(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF,skipSpatAgg):
	def distance(X_1,Y_1,X_2,Y_2):
		return np.sqrt((X_1 - X_2)**2 + (Y_1 - Y_2)**2)

	if skipSpatAgg: # Return early if spatial aggregation is being skipped
		return attributeDict,attributeLabel

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

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'distanceToBall': tmpdistanceToBall}
	attributeLabel.update(attributeLabel_tmp)
	# altogether = pd.concat([rawDict,attributeDict], axis=1)
	# altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

	return attributeDict,attributeLabel

def distanceToCentroid(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	###############
	# Use this as an example to compute a PLAYER level variable. Pay attention to the indexing. Let me know if you have an easier way.
	###############

	##### THE DATA #####
	# In this case, the new attribute will be computed based on a group (i.e., team) value
	TeamVals = attributeDict[rawDict['PlayerID'] == 'groupRow'].set_index('Ts')
	# Create empty DataFrame to store results, NB: columns need to be assigend beforehand.
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distToCent'], dtype = np.float64)

	newAttributes['distToCent']	= np.nan

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

def distanceToGoal(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF,skipSpatAgg):
	def distance(X_1,Y_1,X_2,Y_2):
		return np.sqrt((X_1 - X_2)**2 + (Y_1 - Y_2)**2)

	if skipSpatAgg: # Return early if spatial aggregation is being skipped
		return attributeDict,attributeLabel

	#All the players on the field
	tmp = rawDict[rawDict['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']

	#Create new attribute distanceToGoal
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distanceToOpponentGoal', 'distanceToOwnGoal'])

	newAttributes.loc[players.index,'distanceToOpponentGoal'] = np.nan
	newAttributes.loc[players.index,'distanceToOwnGoal'] = np.nan

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
	# altogether = pd.concat([rawDict,attributeDict], axis=1)
	# altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

	return attributeDict,attributeLabel

def second_smallest(numbers):
    m1, m2 = float('inf'), float('inf')
    for x in numbers:
        if x <= m1:
            m1, m2 = x, m1
        elif x < m2:
            m2 = x
    return m2

def rankPlayersPerFeature(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF,skipSpatAgg):
	if skipSpatAgg:
		return attributeDict,attributeLabel

	def distance(X_1,Y_1,X_2,Y_2):
		return np.sqrt((X_1 - X_2)**2 + (Y_1 - Y_2)**2)

	firstIteration = True

	#Create new attribute for the positioning positioningRating
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distanceToOpponentRank', 'angleToPasslineRank', 'distanceToPossessionRank', 'distanceToOpponentGoalRank', 'curPositioningRank'])

	dictionary = pd.concat([rawDict, attributeDict], axis=1)
	dictionary = dictionary.loc[:,~dictionary.columns.duplicated()]

	goalkeeperA,goalkeeperB = determineGoalKeepers(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF)
	#All the players on the field, exluding goalkeepers
	tmp = dictionary[dictionary['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']
	players = players[players['PlayerID'] != goalkeeperA]
	players = players[players['PlayerID'] != goalkeeperB]

	newAttributes.loc[players.index,'distanceToOpponentRank'] = np.nan
	newAttributes.loc[players.index,'angleToPasslineRank'] = np.nan
	newAttributes.loc[players.index,'distanceToPossessionRank'] = np.nan
	newAttributes.loc[players.index,'distanceToOpponentGoalRank'] = np.nan
	newAttributes.loc[players.index,'curPositioningRank'] = np.nan

	inPossession = dictionary[dictionary['InBallPos'] == 1]

	for idx,i in enumerate(passDF['PassTimes']):
		curTime = i
		if (curTime > 60.0 and korteWedstrijd == True):
			continue
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
			distanceToOpponentRank = curTeamA['dist to closest visitor'].rank(ascending=0) #0 voor Leiden, 1 voor KNVB

			#Rate angle to Opponent
			curTeamA['angleOpponentToPassline'] = curTeamA['angleOpponentToPassline'].replace(np.nan, 200)
			angleRank = curTeamA['angleOpponentToPassline'].rank(ascending=0)

			#Rate distance to player in possession
			distanceToPossessionRank = curTeamA['distanceToInPossession'].rank(ascending=0) #0 voor Leiden, 1 voor KNVB

			#Rank distanceToOpponentGoal
			distanceToOpponentGoalRank = curTeamA['distanceToOpponentGoal'].rank(ascending=1) #1 voor Leiden, 0 voor KNVB

			newAttributes['distanceToOpponentRank'][curTeamA.index] = distanceToOpponentRank
			newAttributes['angleToPasslineRank'][curTeamA.index] = angleRank
			newAttributes['distanceToPossessionRank'][curTeamA.index] = distanceToPossessionRank
			newAttributes['distanceToOpponentGoalRank'][curTeamA.index] = distanceToOpponentGoalRank

			curPositioningRank = newAttributes['distanceToOpponentRank'][curTeamA.index] + newAttributes['angleToPasslineRank'][curTeamA.index] + newAttributes['distanceToPossessionRank'][curTeamA.index] + newAttributes['distanceToOpponentGoalRank'][curTeamA.index]

			curPositioningRank = curPositioningRank.rank(ascending=1) #1 voor Leiden, 0 voor KNVB

			newAttributes['curPositioningRank'][curTeamA.index] = curPositioningRank

		elif all(curTeamInPossession == TeamBstring):
			#Rate distance to nearest opponent
			distanceToOpponentRank = curTeamB['dist to closest visitor'].rank(ascending=0)

			#Rate angle to Opponent
			curTeamB['angleOpponentToPassline'] = curTeamB['angleOpponentToPassline'].replace(np.nan, 200)
			angleRank = curTeamB['angleOpponentToPassline'].rank(ascending=0)

			#Rate distance to player in possession
			distanceToPossessionRank = curTeamB['distanceToInPossession'].rank(ascending=0)

			#Rank distanceToOpponentGoal
			distanceToOpponentGoalRank = curTeamB['distanceToOpponentGoal'].rank(ascending=1)

			newAttributes['distanceToOpponentRank'][curTeamB.index] = distanceToOpponentRank
			newAttributes['angleToPasslineRank'][curTeamB.index] = angleRank
			newAttributes['distanceToPossessionRank'][curTeamB.index] = distanceToPossessionRank
			newAttributes['distanceToOpponentGoalRank'][curTeamB.index] = distanceToOpponentGoalRank

			curPositioningRank = newAttributes['distanceToOpponentRank'][curTeamB.index] + newAttributes['angleToPasslineRank'][curTeamB.index] + newAttributes['distanceToPossessionRank'][curTeamB.index] + newAttributes['distanceToOpponentGoalRank'][curTeamB.index]

			curPositioningRank = curPositioningRank.rank(ascending=1)

			newAttributes['curPositioningRank'][curTeamB.index] = curPositioningRank

		else:
			continue

	players['angleOpponentToPassline'] = players['angleOpponentToPassline'].replace(200, np.nan)

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	tmp_distanceToOpponentRank = 'Rank(1-9) for the distance a player has to his opponent'
	tmp_angleToPasslineRank = 'Rank(1-9) for the angle an opponent has to the passline'
	tmp_distanceToPossessionRank = 'Rank(1-9) for the distance a player has to his teammate in possession'
	tmp_distanceToOpponentGoalRank = 'Rank(1-9) for distance that a player is closter to the goal to than the player in possession'
	tmp_curPositioningRank= 'Rank(1-9) for the positioning of a player at a certain timestamp'

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'distanceToOpponentRank': tmp_distanceToOpponentRank, 'angleToPasslineRank': tmp_angleToPasslineRank, 'distanceToPossessionRank': tmp_distanceToPossessionRank, 'distanceToOpponentGoalRank': tmp_distanceToOpponentGoalRank, 'curPositioningRank': tmp_curPositioningRank}
	attributeLabel.update(attributeLabel_tmp)
	# altogether = pd.concat([rawDict,attributeDict], axis=1)
	# altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

	return attributeDict,attributeLabel

def ratePlayersPerFeature(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF,skipSpatAgg):
	if skipSpatAgg: # Return early if spatial aggregation is being skipped
		return attributeDict,attributeLabel

	def distance(X_1,Y_1,X_2,Y_2):
		return np.sqrt((X_1 - X_2)**2 + (Y_1 - Y_2)**2)

	#Create new attribute for the positioning positioningRating
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distanceToOpponentRating', 'angleToPasslineRating', 'distanceToPossessionRating', 'distanceToOpponentGoalRating', 'curPositioningRating'])

	dictionary = pd.concat([rawDict, attributeDict], axis=1)
	#dictionary = pd.read_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv',low_memory=False)
	dictionary = dictionary.loc[:,~dictionary.columns.duplicated()]

	goalkeeperA,goalkeeperB = determineGoalKeepers(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF)
	#All the players on the field
	tmp = dictionary[dictionary['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']
	#We doen vanaf nu inclusief keepers!!
	players = players[players['PlayerID'] != goalkeeperA]
	players = players[players['PlayerID'] != goalkeeperB]

	newAttributes.loc[players.index,'distanceToOpponentRating'] = np.nan
	newAttributes.loc[players.index,'angleToPasslineRating'] = np.nan
	newAttributes.loc[players.index,'distanceToPossessionRating'] = np.nan
	newAttributes.loc[players.index,'distanceToOpponentGoalRating'] = np.nan
	newAttributes.loc[players.index,'curPositioningRating'] = np.nan

	inPossession = dictionary[dictionary['InBallPos'] == 1]

	for idx,i in enumerate(passDF['PassTimes']):
		curTime = i
		if (curTime > 60.0 and korteWedstrijd == True):
			continue
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
			minAngle = curTeamA['angleOpponentToPassline'].min()
			maxAngle = curTeamA['angleOpponentToPassline'].max()
			angleRating = (curTeamA['angleOpponentToPassline'] - minAngle) / (maxAngle - minAngle) * 8 + 1
			curTeamA_NaN = curTeamA[curTeamA['angleOpponentToPassline'].isnull()]

			#Rate distance to player in possession
			minDistanceToPossession = min(curTeamA['distanceToInPossession'])
			maxDistanceToPossession = max(curTeamA['distanceToInPossession'])
			distanceToPossessionRating = (curTeamA['distanceToInPossession'] - minDistanceToPossession) / (maxDistanceToPossession - minDistanceToPossession) * 9 + 1

			#Rate decrease in distance to goal
			possessionToGoal = int(curInPossession['distanceToOpponentGoal'])
			closerToGoal = curTeamA[curTeamA['distanceToOpponentGoal'] < possessionToGoal]
			furtherFromGoal = curTeamA[curTeamA['distanceToOpponentGoal'] > possessionToGoal]

			newAttributes['distanceToOpponentRating'][curTeamA.index] = distanceToOpponentRating
			newAttributes['angleToPasslineRating'][curTeamA.index] = angleRating
			newAttributes['angleToPasslineRating'][curTeamA_NaN.index] = 10
			newAttributes['distanceToPossessionRating'][curTeamA.index] = distanceToPossessionRating
			newAttributes['distanceToOpponentGoalRating'][closerToGoal.index] = 10
			newAttributes['distanceToOpponentGoalRating'][furtherFromGoal.index] = 1

			curPositioningRating = newAttributes['distanceToPossessionRating'][curTeamA.index] * 1/4 + newAttributes['distanceToOpponentGoalRating'][curTeamA.index] * 1/4 + newAttributes['distanceToOpponentRating'][curTeamA.index] * 1/4 + newAttributes['angleToPasslineRating'][curTeamA.index] * 1/4

			newAttributes['curPositioningRating'][curTeamA.index] = curPositioningRating

		elif all(curTeamInPossession == TeamBstring):
			#Rate distance to nearest opponent
			minDistance = min(curTeamB['dist to closest visitor'])
			maxDistance = max(curTeamB['dist to closest visitor'])
			distanceToOpponentRating = (curTeamB['dist to closest visitor'] - minDistance) / (maxDistance - minDistance) * 9 + 1

			#Rate angle to Opponent
			minAngle = curTeamB['angleOpponentToPassline'].min()
			maxAngle = curTeamB['angleOpponentToPassline'].max()
			angleRating = (curTeamB['angleOpponentToPassline'] - minAngle) / (maxAngle - minAngle) * 8 + 1
			curTeamB_NaN = curTeamB[curTeamB['angleOpponentToPassline'].isnull()]

			#Rate distance to player in possession
			minDistanceToPossession = min(curTeamB['distanceToInPossession'])
			maxDistanceToPossession = max(curTeamB['distanceToInPossession'])
			distanceToPossessionRating = (curTeamB['distanceToInPossession'] - minDistanceToPossession) / (maxDistanceToPossession - minDistanceToPossession) * 9 + 1

			#Rate decrease in distance to goal
			possessionToGoal = int(curInPossession['distanceToOpponentGoal'])
			closerToGoal = curTeamB[curTeamB['distanceToOpponentGoal'] < possessionToGoal]
			furtherFromGoal = curTeamB[curTeamB['distanceToOpponentGoal'] > possessionToGoal]

			newAttributes['distanceToOpponentRating'][curTeamB.index] = distanceToOpponentRating
			newAttributes['angleToPasslineRating'][curTeamB.index] = angleRating
			newAttributes['angleToPasslineRating'][curTeamB_NaN.index] = 10
			newAttributes['distanceToPossessionRating'][curTeamB.index] = distanceToPossessionRating
			newAttributes['distanceToOpponentGoalRating'][closerToGoal.index] = 10
			newAttributes['distanceToOpponentGoalRating'][furtherFromGoal.index] = 1

			curPositioningRating = newAttributes['distanceToPossessionRating'][curTeamB.index] * 1/4 + newAttributes['distanceToOpponentGoalRating'][curTeamB.index] * 1/4 + newAttributes['distanceToOpponentRating'][curTeamB.index] * 1/4 + newAttributes['angleToPasslineRating'][curTeamB.index] * 1/4

			newAttributes['curPositioningRating'][curTeamB.index] = curPositioningRating

		else:
			continue

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	tmp_distanceToOpponentRating = 'Rating (1-10) for the distance a player has to his opponent'
	tmp_angleToPasslineRating = 'Rating (1-10) for the angle an opponent has to the passline'
	tmp_distanceToPossessionRating = 'Rating (1-10) for the distance a player has to his teammate in possession'
	tmp_distanceToOpponentGoalRating = 'Rating (1-10) for distance that a player is closter to the goal to than the player in possession'
	tmp_curPositioningRating = 'Rating (1-10) for the positioning of a player at a certain timestamp'

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'distanceToOpponentRating': tmp_distanceToOpponentRating, 'angleToPasslineRating': tmp_angleToPasslineRating, 'distanceToPossessionRating': tmp_distanceToPossessionRating, 'distanceToOpponentGoalRating': tmp_distanceToOpponentGoalRating, 'curPositioningRating': tmp_curPositioningRating}
	attributeLabel.update(attributeLabel_tmp)
	# altogether = pd.concat([rawDict,attributeDict], axis=1)
	# altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

	return attributeDict,attributeLabel

def playerPassedToRating(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF,skipSpatAgg):
	if skipSpatAgg: # Return early if spatial aggregation is being skipped
		return attributeDict,attributeLabel

	#Create new attribute for the positioning positioningRating
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['ballPassedTo','passRating','passDistanceToGoalRating','passAngleRating','passDistToOpRating','passDistToPosRating','passDistanceToGoal','passAngle','passDistToOp','passDistToPos'])

	goalkeeperA,goalkeeperB = determineGoalKeepers(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF)

	dictionary = pd.concat([rawDict, attributeDict], axis=1)
	dictionary = dictionary.loc[:,~dictionary.columns.duplicated()]

	tmp = dictionary[dictionary['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']
	newAttributes.loc[players.index,'ballPassedTo'] = np.nan
	newAttributes.loc[players.index,'passRating'] = np.nan
	newAttributes.loc[players.index,'passDistanceToGoalRating'] = np.nan
	newAttributes.loc[players.index,'passAngleRating'] = np.nan
	newAttributes.loc[players.index,'passDistToOpRating'] = np.nan
	newAttributes.loc[players.index,'passDistToPosRating'] = np.nan
	newAttributes.loc[players.index,'passDistanceToGoal'] = np.nan
	newAttributes.loc[players.index,'passAngle'] = np.nan
	newAttributes.loc[players.index,'passDistToOp'] = np.nan
	newAttributes.loc[players.index,'passDistToPos'] = np.nan
	players = players[players['PlayerID'] != goalkeeperA]
	players = players[players['PlayerID'] != goalkeeperB]

	ball = dictionary[dictionary['PlayerID'] == 'ball']

	inBallPos = dictionary[dictionary['InBallPos'] == 1]
	inBallPos = inBallPos.sort_values('Ts')

	passTime = 0
	passedTo = 0.0

	searching = False # Boolean that tells whether we are searching for the player that the ball is passed to

	for idx,i in enumerate(pd.unique(inBallPos['Ts'])):
		curTime = round(i,1)
		if(len(inBallPos['PlayerID'][inBallPos['Ts'] == curTime]) == 1):
			curInPossession = inBallPos['PlayerID'][inBallPos['Ts'] == curTime].item()
		else:
			continue
		if (searching == True and curInPossession != prevInPossession and (curTime - passTime) < 5.0):
			teamCurrentInPossession = inBallPos['TeamID'][inBallPos['Ts'] == curTime].item()
			if (teamCurrentInPossession != teamPrevInPossession):
				searching = False
				continue
			passedTo = curInPossession
			passedToIndex = players[players['PlayerID'] == curInPossession]
			passedToIndex =  passedToIndex[passedToIndex['Ts'] == passTime]
			if(len(players[players['PlayerID'] == curInPossession]) == 0): #Keeper heeft de bal en die krijgt geen positiescore
				searching = False
				continue
			passRating = passedToIndex['curPositioningRating'].item()
			passDistanceToGoalRating = passedToIndex['distanceToOpponentGoalRating'].item()
			passAngleRating = passedToIndex['angleToPasslineRating'].item()
			passDistToOpRating = passedToIndex['distanceToOpponentRating'].item()
			passDistPosRating = passedToIndex['distanceToPossessionRating'].item()
			passDistanceToGoal = passedToIndex['distanceToOpponentGoal'].item()
			passAngle = passedToIndex['angleOpponentToPassline'].item()
			passDistToOp = passedToIndex['dist to closest visitor'].item()
			passDistPos = passedToIndex['distanceToInPossession'].item()

			newAttributes['ballPassedTo'][passedToIndex.index] = 1
			newAttributes['passRating'][passedToIndex.index] = passRating
			newAttributes['passDistanceToGoalRating'][passedToIndex.index] = passDistanceToGoalRating
			newAttributes['passAngleRating'][passedToIndex.index] = passAngleRating
			newAttributes['passDistToOpRating'][passedToIndex.index] = passDistToOpRating
			newAttributes['passDistToPosRating'][passedToIndex.index] = passDistPosRating
			newAttributes['passDistanceToGoal'][passedToIndex.index] = passDistanceToGoal
			newAttributes['passAngle'][passedToIndex.index] = passAngle
			newAttributes['passDistToOp'][passedToIndex.index] = passDistToOp
			newAttributes['passDistToPos'][passedToIndex.index] = passDistPos

			searching = False

		if(curTime in passDF['PassTimes'].values):
			passTime = curTime
			teamPrevInPossession = inBallPos['TeamID'][inBallPos['Ts'] == curTime].item()
			setToZero = players[players['Ts'] == curTime]
			setToZero = setToZero[setToZero['TeamID'] == teamPrevInPossession]
			newAttributes['ballPassedTo'][setToZero.index] = 0
			prevInPossession = inBallPos['PlayerID'][inBallPos['Ts'] == curTime].item()
			searching = True

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	tmp_ballPassedTo = 'Binary value (0/1) that indicates whether the player is passed the ball to'
	tmp_passRating = 'The positioning rating of the player who received the pass'
	tmp_passDistanceToGoalRating = 'The distance rating of the player who received the pass'
	tmp_passAngleRating = 'The pass angle rating of the player who received the pass'
	tmp_passDistToOpRating = 'The distance to opponent rating of the player who received the pass'
	tmp_passDistToPosRating = 'The distance to the player in possession rating of the player who received the pass'
	tmp_passDistanceToGoal = 'The distance rating of the player who received the pass'
	tmp_passAngle = 'The pass angle rating of the player who received the pass'
	tmp_passDistToOp = 'The distance to opponent rating of the player who received the pass'
	tmp_passDistToPos = 'The distance to the player in possession rating of the player who received the pass'

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'ballPassedTo': tmp_ballPassedTo,'passRating': tmp_passRating,'passDistanceToGoalRating': tmp_passDistanceToGoalRating,'passAngleRating': tmp_passAngleRating,'passDistToOpRating': tmp_passDistToOpRating,'passDistToPosRating': tmp_passDistToPosRating,'passDistanceToGoal': tmp_passDistanceToGoal,'passAngle': tmp_passAngle,'passDistToOp': tmp_passDistToOp,'passDistToPos': tmp_passDistToPos}
	attributeLabel.update(attributeLabel_tmp)
	# altogether = pd.concat([rawDict,attributeDict], axis=1)
	# altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

	return attributeDict,attributeLabel

def playerPassedToRank(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF,skipSpatAgg):
	if skipSpatAgg: # Return early if spatial aggregation is being skipped
		return attributeDict,attributeLabel

	#Create new attribute for the positioning positioningRating
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['ballPassedTo','passRank','passDistanceToGoalRank','passAngleRank','passDistToOpRank','passDistToPosRank','passDistanceToGoal','passAngle','passDistToOp','passDistToPos', 'passPopRank', 'passPopDistanceToGoalRank', 'passPopDistToOpRank', 'passPopDistToPosRank','passPopAngleRank'])

	goalkeeperA,goalkeeperB = determineGoalKeepers(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF)

	dictionary = pd.concat([rawDict, attributeDict], axis=1)
	dictionary = dictionary.loc[:,~dictionary.columns.duplicated()]

	tmp = dictionary[dictionary['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']
	newAttributes.loc[players.index,'ballPassedTo'] = np.nan
	newAttributes.loc[players.index,'passRank'] = np.nan
	newAttributes.loc[players.index,'passDistanceToGoalRank'] = np.nan
	newAttributes.loc[players.index,'passAngleRank'] = np.nan
	newAttributes.loc[players.index,'passDistToOpRank'] = np.nan
	newAttributes.loc[players.index,'passDistToPosRank'] = np.nan
	newAttributes.loc[players.index,'passPopRank'] = np.nan
	newAttributes.loc[players.index,'passPopDistanceToGoalRank'] = np.nan
	newAttributes.loc[players.index,'passPopAngleRank'] = np.nan
	newAttributes.loc[players.index,'passPopDistToOpRank'] = np.nan
	newAttributes.loc[players.index,'passPopDistToPosRank'] = np.nan
	newAttributes.loc[players.index,'passDistanceToGoal'] = np.nan
	newAttributes.loc[players.index,'passAngle'] = np.nan
	newAttributes.loc[players.index,'passDistToOp'] = np.nan
	newAttributes.loc[players.index,'passDistToPos'] = np.nan
	players = players[players['PlayerID'] != goalkeeperA]
	players = players[players['PlayerID'] != goalkeeperB]

	ball = dictionary[dictionary['PlayerID'] == 'ball']

	inBallPos = dictionary[dictionary['InBallPos'] == 1]
	inBallPos = inBallPos.sort_values('Ts')

	passTime = 0
	passedTo = 0.0

	searching = False # Boolean that tells whether we are searching for the player that the ball is passed to

	for idx,i in enumerate(pd.unique(inBallPos['Ts'])):
		curTime = round(i,1)
		if(len(inBallPos['PlayerID'][inBallPos['Ts'] == curTime]) == 1):
			curInPossession = inBallPos['PlayerID'][inBallPos['Ts'] == curTime].item()
		else:
			continue
		if (searching == True and curInPossession != prevInPossession and (curTime - passTime) < 5.0):
			teamCurrentInPossession = inBallPos['TeamID'][inBallPos['Ts'] == curTime].item()
			if (teamCurrentInPossession != teamPrevInPossession):
				searching = False
				continue
			passedTo = curInPossession
			passedToIndex = players[players['PlayerID'] == curInPossession]
			passedToIndex =  passedToIndex[passedToIndex['Ts'] == passTime]
			if(len(players[players['PlayerID'] == curInPossession]) == 0): #Keeper heeft de bal en die krijgt geen positiescore
				searching = False
				continue
			passRating = passedToIndex['curPositioningRank'].item()
			passDistanceToGoalRating = passedToIndex['distanceToOpponentGoalRank'].item()
			passAngleRating = passedToIndex['angleToPasslineRank'].item()
			passDistToOpRating = passedToIndex['distanceToOpponentRank'].item()
			passDistPosRating = passedToIndex['distanceToPossessionRank'].item()
			passPopRating = passedToIndex['popPositioningRank'].item()
			passPopDistanceToGoalRating = passedToIndex['popDistanceToGoalRank'].item()
			passPopAngleRating = passedToIndex['popAngleRank'].item()
			passPopDistToOpRating = passedToIndex['popDistToOpRank'].item()
			passPopDistPosRating = passedToIndex['popDistToPosRank'].item()
			passDistanceToGoal = passedToIndex['distanceToOpponentGoal'].item()
			passAngle = passedToIndex['angleOpponentToPassline'].item()
			passDistToOp = passedToIndex['dist to closest visitor'].item()
			passDistPos = passedToIndex['distanceToInPossession'].item()

			newAttributes['ballPassedTo'][passedToIndex.index] = 1
			newAttributes['passRank'][passedToIndex.index] = passRating
			newAttributes['passDistanceToGoalRank'][passedToIndex.index] = passDistanceToGoalRating
			newAttributes['passAngleRank'][passedToIndex.index] = passAngleRating
			newAttributes['passDistToOpRank'][passedToIndex.index] = passDistToOpRating
			newAttributes['passDistToPosRank'][passedToIndex.index] = passDistPosRating
			newAttributes['passDistanceToGoal'][passedToIndex.index] = passDistanceToGoal
			newAttributes['passAngle'][passedToIndex.index] = passAngle
			newAttributes['passDistToOp'][passedToIndex.index] = passDistToOp
			newAttributes['passDistToPos'][passedToIndex.index] = passDistPos

			newAttributes['passPopRank'][passedToIndex.index] = passPopRating
			newAttributes['passPopDistanceToGoalRank'][passedToIndex.index] = passPopDistanceToGoalRating
			newAttributes['passPopAngleRank'][passedToIndex.index] = passPopAngleRating
			newAttributes['passPopDistToOpRank'][passedToIndex.index] = passPopDistToOpRating
			newAttributes['passDistanceToGoal'][passedToIndex.index] = passPopDistPosRating

			searching = False

		if(curTime in passDF['PassTimes'].values):
			passTime = curTime
			teamPrevInPossession = inBallPos['TeamID'][inBallPos['Ts'] == curTime].item()
			setToZero = players[players['Ts'] == curTime]
			setToZero = setToZero[setToZero['TeamID'] == teamPrevInPossession]
			newAttributes['ballPassedTo'][setToZero.index] = 0
			prevInPossession = inBallPos['PlayerID'][inBallPos['Ts'] == curTime].item()
			searching = True

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	tmp_ballPassedTo = 'Binary value (0/1) that indicates whether the player is passed the ball to'
	tmp_passRating = 'The positioning rating of the player who received the pass'
	tmp_passDistanceToGoalRating = 'The distance rating of the player who received the pass'
	tmp_passAngleRating = 'The pass angle rating of the player who received the pass'
	tmp_passDistToOpRating = 'The distance to opponent rating of the player who received the pass'
	tmp_passDistToPosRating = 'The distance to the player in possession rating of the player who received the pass'
	tmp_passPopRating = 'The population based positioning rating of the player who received the pass'
	tmp_passPopDistanceToGoalRating = 'The population based distance rating of the player who received the pass'
	tmp_passPopAngleRating = 'The population based pass angle rating of the player who received the pass'
	tmp_passPopDistToOpRating = 'The population based distance to opponent rating of the player who received the pass'
	tmp_passPopDistToPosRating = 'The population based distance to the player in possession rating of the player who received the pass'
	tmp_passDistanceToGoal = 'The distance rating of the player who received the pass'
	tmp_passAngle = 'The pass angle rating of the player who received the pass'
	tmp_passDistToOp = 'The distance to opponent rating of the player who received the pass'
	tmp_passDistToPos = 'The distance to the player in possession rating of the player who received the pass'

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'ballPassedTo': tmp_ballPassedTo,'passRank': tmp_passRating,'passDistanceToGoalRank': tmp_passDistanceToGoalRating,'passAngleRank': tmp_passAngleRating,'passDistToOpRank': tmp_passDistToOpRating,'passDistToPosRank': tmp_passDistToPosRating,'passDistanceToGoal': tmp_passDistanceToGoal,'passAngle': tmp_passAngle,'passDistToOp': tmp_passDistToOp,'passDistToPos': tmp_passDistToPos,
	'passPopRank': tmp_passPopRating, 'passPopDistanceToGoalRank': tmp_passPopDistanceToGoalRating, 'passPopDistToOpRank': tmp_passPopDistToOpRating, 'passPopDistToPosRank': tmp_passPopDistToPosRating, 'passPopAngleRank': tmp_passPopAngleRating}
	attributeLabel.update(attributeLabel_tmp)
	# altogether = pd.concat([rawDict,attributeDict], axis=1)
	# altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

	return attributeDict,attributeLabel

def ratePlayers(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF,skipSpatAgg):
	if skipSpatAgg: # Return early if spatial aggregation is being skipped
		return attributeDict,attributeLabel

	#Create new attribute for the positioningRating
	newAttributes = pd.DataFrame(columns = ['PlayerID', 'positioningRating'])

	newAttributes['PlayerID'] = np.nan
	newAttributes['positioningRating'] = np.nan

	dictionary = pd.concat([rawDict, attributeDict], axis=1)
	dictionary = dictionary.loc[:,~dictionary.columns.duplicated()]

	for idx,i in enumerate(pd.unique(dictionary['PlayerID'])):
		curPlayer = dictionary[dictionary['PlayerID'] == i]
		curPlayerDict = curPlayer.set_index('Ts')
		if all(curPlayer['PlayerID'] == 'groupRow'):
			# It's actually not a player, but a group, so skip it.
			continue # do nothing
		elif all(curPlayer['PlayerID'] == 'ball'):
			# It's actually not a player, but the ball, so skip it.
			continue # do nothing
		else:
			playerPositioningRatings = curPlayer['curPositioningRating'].dropna()
			positionRating = playerPositioningRatings.sum() / playerPositioningRatings.count()
			newAttributes.loc[len(newAttributes)]=[str(pd.unique(curPlayer['PlayerID'])),positionRating]

	newAttributes.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/playerRatings.csv')

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	tmp_positioningRating = 'Rating (1-10) for the positioning of a player over the course of a match'

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'positioningRating': tmp_positioningRating}
	attributeLabel.update(attributeLabel_tmp)
	#altogether = pd.concat([rawDict,attributeDict], axis=1)
	#altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

def determineFinalThird(leftSide,TeamAstring,TeamBstring,goal_A_X,goal_B_X):
	beginZone = 34 #LT: aanpassen naar 35 maar dan ook de CSV aanpassen. En goed controleren!
	zoneMin_X = fieldLength / 2 - beginZone
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
		warn('\nWARNING: Cannot determine the side, because the players of the teams are not on the same side at the first timestamp.\n')
		return 'Err','Err','Err','Err','Err','Err','Err','Err'

	#determine zone Y, For both teams the same
	zoneMin_Y = (fieldWidth / 2) * -1
	zoneMax_Y = (fieldWidth / 2)

	return beginZone,zoneMin_X,zoneA,zoneB,zoneMin_A_X,zoneMax_A_X,zoneMin_B_X,zoneMax_B_X,zoneMin_Y,zoneMax_Y

def playerInFinalThird(curPlayerTeam,TeamString,curPlayerX,zoneMin_X,zoneMax_X,curPlayerY,zoneMin_Y,zoneMax_Y):
	return all(curPlayerTeam == TeamString) and all(curPlayerX >= zoneMin_X) and all(curPlayerX <= zoneMax_X) and all(curPlayerY >= zoneMin_Y) and all(curPlayerY <= zoneMax_Y)

def playerOnOpponentsHalf(curPlayerTeam,TeamString,curPlayerX,zoneMin_X,zoneMax_X,curPlayerY,zoneMin_Y,zoneMax_Y):
	if(zoneMin_X > 0):
		zoneMin_X = 0
	else:
		zoneMax_X = 0

	return all(curPlayerTeam == TeamString) and all(curPlayerX >= zoneMin_X) and all(curPlayerX <= zoneMax_X) and all(curPlayerY >= zoneMin_Y) and all(curPlayerY <= zoneMax_Y)

def zone(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg):#,secondHalfTime):
	if skipSpatAgg: # Return early if spatial aggregation is being skipped
		return attributeDict,attributeLabel
	##############   READ ZONE CSV   ###############
	dirPath = os.path.dirname(os.path.realpath(__file__))
	fileName = dirPath + '/Zone.csv'
	zoneMatrix = np.loadtxt(open(fileName, 'r'),delimiter=';')

	##############   DETERMINE SIDE FIRST HALF   ###############
	leftSide, goal_A_X, goal_B_X, goal_Y = determineSide(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	beginZone,zoneMin_X,zoneA,zoneB,zoneMin_A_X,zoneMax_A_X,zoneMin_B_X,zoneMax_B_X,zoneMin_Y,zoneMax_Y = determineFinalThird(leftSide,TeamAstring,TeamBstring,goal_A_X,goal_B_X)

	##############   CREATE ATTRIBUTES   ###############
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['zone','inZone','opponentsHalf'])
	newAttributes['inZone'] = 0
	newAttributes['zone'] = 0
	newAttributes['opponentsHalf'] = 0

	#check if it is half time
	halfTimeBool = False

	#only for players in possession
	InBallPos = rawDict[attributeDict['InBallPos'] == 1]

	##############   DETERMINE ZONE VALUE IF PLAYER IS IN ZONE   ###############
	for idx,i in enumerate(pd.unique(InBallPos['Ts'])):
		curTime = i

		curPlayerX = InBallPos['X'][InBallPos['Ts'] == curTime]
		curPlayerY = InBallPos['Y'][InBallPos['Ts'] == curTime]
		curPlayerTeam = InBallPos['TeamID'][InBallPos['Ts'] == curTime]
		#skip if NaN
		if all(np.isnan(curPlayerX)) or all(np.isnan(curPlayerY)):
		 	continue

		#TEAM A
		if playerInFinalThird(curPlayerTeam,TeamAstring,curPlayerX,zoneMin_A_X,zoneMax_A_X,curPlayerY,zoneMin_Y,zoneMax_Y):
			zone_X = int(round(abs(curPlayerX + (zoneA * zoneMin_X))))
		#TEAM B
		elif playerInFinalThird(curPlayerTeam,TeamBstring,curPlayerX,zoneMin_B_X,zoneMax_B_X,curPlayerY,zoneMin_Y,zoneMax_Y):
			zone_X = int(round(abs(curPlayerX + (zoneB * zoneMin_X))))
		#skip if player is not in final third, but he can still be on the opponents half
		else:
			if playerOnOpponentsHalf(curPlayerTeam,TeamAstring,curPlayerX,zoneMin_A_X,zoneMax_A_X,curPlayerY,zoneMin_Y,zoneMax_Y):
				newAttributes.loc[curPlayerTeam.index,'opponentsHalf'] = 1
			elif playerOnOpponentsHalf(curPlayerTeam,TeamBstring,curPlayerX,zoneMin_B_X,zoneMax_B_X,curPlayerY,zoneMin_Y,zoneMax_Y):
				newAttributes.loc[curPlayerTeam.index,'opponentsHalf'] = 1
			continue

		zone_Y = int(round(abs(curPlayerY + zoneMax_Y)))
		zoneValue = zoneMatrix[zone_X][zone_Y]
		newAttributes.loc[curPlayerTeam.index,'zone'] = zoneValue
		newAttributes.loc[curPlayerTeam.index,'inZone'] = 1
		newAttributes.loc[curPlayerTeam.index,'opponentsHalf'] = 1

	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	##### THE STRINGS #####
	tmpZone = 'Value of player with ball in the final third (last ' + str(beginZone) + ' meters).'
	tmpInZone = 'Is player with ball in the final third (last ' + str(beginZone) + ' meters)?'
	tmpOpponentsHalf = 'Is player on the half of the opponent?'

	attributeLabel_tmp = {'zone': tmpZone, 'inZone': tmpInZone, 'opponentsHalf': tmpOpponentsHalf}
	attributeLabel.update(attributeLabel_tmp)

	# altogether = pd.concat([rawDict,attributeDict], axis=1)
	# altogether.to_csv('D:\\KNVB\\test.csv')

	# pdb.set_trace()

	return attributeDict,attributeLabel

def lastBuildUpPassRating(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg,targetEvents):
	#Create new attribute for the positioning positioningRating
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['lastPassRating','lastPassDistanceToGoalRating','lastPassAngleRating','lastPassDistToOpRating','lastPassDistToPosRating','lastPassDistanceToGoal','lastPassAngle','lastPassDistToOp','lastPassDistToPos','finalDistanceToGoal'])

	allDict = pd.concat([rawDict, attributeDict], axis=1)
	allDict = allDict.loc[:,~allDict.columns.duplicated()]
	allDict = allDict.sort_values('Ts')

	tmp = allDict[allDict['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']

	newAttributes.loc[players.index,'lastPassRating'] = np.nan
	newAttributes.loc[players.index,'lastPassDistanceToGoalRating'] = np.nan
	newAttributes.loc[players.index,'lastPassAngleRating'] = np.nan
	newAttributes.loc[players.index,'lastPassDistToOpRating'] = np.nan
	newAttributes.loc[players.index,'lastPassDistToPosRating'] = np.nan

	newAttributes.loc[players.index,'lastPassDistanceToGoal'] = np.nan
	newAttributes.loc[players.index,'lastPassAngle'] = np.nan
	newAttributes.loc[players.index,'lastPassDistToOp'] = np.nan
	newAttributes.loc[players.index,'lastPassDistToPos'] = np.nan
	newAttributes.loc[players.index,'finalDistanceToGoal'] = np.nan

	attackEvents = targetEvents['buildUp']

	for idx, i in enumerate(attackEvents):
		endTime = i[0]
		beginTime = i[2]
		buildUpPossessions = players[players['Ts'] >= beginTime]
		buildUpPossessions = buildUpPossessions[buildUpPossessions['Ts'] <= endTime]
		passesDuringPossession = buildUpPossessions[buildUpPossessions['ballPassedTo'] == 1]
		lastBuildUpPass = passesDuringPossession.tail(1)
		#print(passesDuringPossession)
		#print("*******")
		#print(lastBuildUpPass)
		#pdb.set_trace()
		if(len(lastBuildUpPass) != 1):
			continue
		lastPassRating = lastBuildUpPass['curPositioningRating'].item()
		lastPassDistanceToGoalRating = lastBuildUpPass['distanceToOpponentGoalRating'].item()
		lastPassAngleRating = lastBuildUpPass['angleToPasslineRating'].item()
		lastPassDistToOpRating = lastBuildUpPass['distanceToOpponentRating'].item()
		lastPassDistToPosRating = lastBuildUpPass['distanceToPossessionRating'].item()

		lastPassDistanceToGoal = lastBuildUpPass['distanceToOpponentGoal'].item()
		lastPassAngle = lastBuildUpPass['angleOpponentToPassline'].item()
		lastPassDistToOp = lastBuildUpPass['dist to closest visitor'].item()
		lastPassDistToPos = lastBuildUpPass['distanceToInPossession'].item()

		newAttributes['lastPassRating'][lastBuildUpPass.index] = lastPassRating
		newAttributes['lastPassDistanceToGoalRating'][lastBuildUpPass.index] = lastPassDistanceToGoalRating
		newAttributes['lastPassAngleRating'][lastBuildUpPass.index] = lastPassAngleRating
		newAttributes['lastPassDistToOpRating'][lastBuildUpPass.index] = lastPassDistToOpRating
		newAttributes['lastPassDistToPosRating'][lastBuildUpPass.index] = lastPassDistToPosRating
		newAttributes['lastPassDistanceToGoal'][lastBuildUpPass.index] = lastPassDistanceToGoal
		newAttributes['lastPassAngle'][lastBuildUpPass.index] = lastPassAngle
		newAttributes['lastPassDistToOp'][lastBuildUpPass.index] = lastPassDistToOp
		newAttributes['lastPassDistToPos'][lastBuildUpPass.index] = lastPassDistToPos
		lastBuildUpPossession = players[players['InBallPos'] == 1]
		lastBuildUpPossession = lastBuildUpPossession[lastBuildUpPossession['Ts'] == endTime]
		if (len(lastBuildUpPossession) != 1):
			continue
		finalDistanceToGoal = lastBuildUpPossession['distanceToOpponentGoal'].item()
		newAttributes['finalDistanceToGoal'][lastBuildUpPossession.index] = finalDistanceToGoal

	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	tmp_lastPassRank = 'The positioning rating of the player who received the pass'
	tmp_lastPassDistanceToGoalRank = 'The distance rating of the player who received the pass'
	tmp_lastPassAngleRank = 'The pass angle rating of the player who received the pass'
	tmp_lastPassDistToOpRank = 'The distance rating to opponent rating of the player who received the pass'
	tmp_lastPassDistToPosRank = 'The distance rating to the player in possession rating of the player who received the pass'

	tmp_lastPassDistanceToGoal = 'The distance rating of the player who received the pass'
	tmp_lastPassAngle = 'The pass angle rating of the player who received the pass'
	tmp_lastPassDistToOp = 'The distance to opponent rating of the player who received the pass'
	tmp_lastPassDistToPos = 'The distance to the player in possession rating of the player who received the pass'
	tmp_finalDistanceToGoal = 'The player\'s distance to the goal at the end of the builUp'

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'lastPassRating': tmp_lastPassRank,'lastPassDistanceToGoalRating': tmp_lastPassDistanceToGoalRank,'lastPassAngleRating': tmp_lastPassAngleRank,'lastPassDistToOpRating': tmp_lastPassDistToOpRank,'lastPassDistToPosRating': tmp_lastPassDistToPosRank,'lastPassDistanceToGoal': tmp_lastPassDistanceToGoal,'lastPassAngle': tmp_lastPassAngle,'lastPassDistToOp': tmp_lastPassDistToOp,'lastPassDistToPos': tmp_lastPassDistToPos,'finalDistanceToGoal': tmp_finalDistanceToGoal}

	attributeLabel.update(attributeLabel_tmp)
	# altogether = pd.concat([rawDict,attributeDict], axis=1)
	# altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

	return attributeDict,attributeLabel

def lastBuildUpPassRank(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg,targetEvents):
	#Create new attribute for the positioning positioningRating
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['lastPassRank','lastPassDistanceToGoalRank','lastPassAngleRank','lastPassDistToOpRank','lastPassDistToPosRank','lastPassDistanceToGoal','lastPassAngle','lastPassDistToOp','lastPassDistToPos','finalDistanceToGoal','lastPassPopRank','lastPassPopDistanceToGoalRank','lastPassPopAngleRank','lastPassPopDistToOpRank','lastPassPopDistToPosRank'])

	allDict = pd.concat([rawDict, attributeDict], axis=1)
	allDict = allDict.loc[:,~allDict.columns.duplicated()]
	allDict = allDict.sort_values('Ts')

	tmp = allDict[allDict['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']

	newAttributes.loc[players.index,'lastPassRank'] = np.nan
	newAttributes.loc[players.index,'lastPassDistanceToGoalRank'] = np.nan
	newAttributes.loc[players.index,'lastPassAngleRank'] = np.nan
	newAttributes.loc[players.index,'lastPassDistToOpRank'] = np.nan
	newAttributes.loc[players.index,'lastPassDistToPosRank'] = np.nan

	newAttributes.loc[players.index,'lastPassPopRank'] = np.nan
	newAttributes.loc[players.index,'lastPassPopDistanceToGoalRank'] = np.nan
	newAttributes.loc[players.index,'lastPassPopAngleRank'] = np.nan
	newAttributes.loc[players.index,'lastPassPopDistToOpRank'] = np.nan
	newAttributes.loc[players.index,'lastPassPopDistToPosRank'] = np.nan

	newAttributes.loc[players.index,'lastPassDistanceToGoal'] = np.nan
	newAttributes.loc[players.index,'lastPassAngle'] = np.nan
	newAttributes.loc[players.index,'lastPassDistToOp'] = np.nan
	newAttributes.loc[players.index,'lastPassDistToPos'] = np.nan
	newAttributes.loc[players.index,'finalDistanceToGoal'] = np.nan

	attackEvents = targetEvents['buildUp']

	for idx, i in enumerate(attackEvents):
		endTime = i[0]
		beginTime = i[2]
		buildUpPossessions = players[players['Ts'] >= beginTime]
		buildUpPossessions = buildUpPossessions[buildUpPossessions['Ts'] <= endTime]
		passesDuringPossession = buildUpPossessions[buildUpPossessions['ballPassedTo'] == 1]
		lastBuildUpPass = passesDuringPossession.tail(1)
		#print(passesDuringPossession)
		#print("*******")
		#print(lastBuildUpPass)
		#pdb.set_trace()
		if(len(lastBuildUpPass) != 1):
			continue
		lastPassRating = lastBuildUpPass['curPositioningRank'].item()
		lastPassDistanceToGoalRating = lastBuildUpPass['distanceToOpponentGoalRank'].item()
		lastPassAngleRating = lastBuildUpPass['angleToPasslineRank'].item()
		lastPassDistToOpRating = lastBuildUpPass['distanceToOpponentRank'].item()
		lastPassDistToPosRating = lastBuildUpPass['distanceToPossessionRank'].item()

		lastPassPopRating = lastBuildUpPass['popPositioningRank'].item()
		lastPassPopDistanceToGoalRating = lastBuildUpPass['popDistanceToGoalRank'].item()
		lastPassPopAngleRating = lastBuildUpPass['popAngleRank'].item()
		lastPassPopDistToOpRating = lastBuildUpPass['popDistToOpRank'].item()
		lastPassPopDistPosRating = lastBuildUpPass['popDistToPosRank'].item()

		lastPassDistanceToGoal = lastBuildUpPass['distanceToOpponentGoal'].item()
		lastPassAngle = lastBuildUpPass['angleOpponentToPassline'].item()
		lastPassDistToOp = lastBuildUpPass['dist to closest visitor'].item()
		lastPassDistToPos = lastBuildUpPass['distanceToInPossession'].item()

		newAttributes['lastPassPopRank'][lastBuildUpPass.index] = lastPassPopRating
		newAttributes['lastPassPopDistanceToGoalRank'][lastBuildUpPass.index] = lastPassPopDistanceToGoalRating
		newAttributes['lastPassPopAngleRank'][lastBuildUpPass.index] = lastPassPopAngleRating
		newAttributes['lastPassPopDistToOpRank'][lastBuildUpPass.index] = lastPassPopDistToOpRating
		newAttributes['lastPassPopDistToPosRank'][lastBuildUpPass.index] = lastPassPopDistPosRating

		newAttributes['lastPassRank'][lastBuildUpPass.index] = lastPassRating
		newAttributes['lastPassDistanceToGoalRank'][lastBuildUpPass.index] = lastPassDistanceToGoalRating
		newAttributes['lastPassAngleRank'][lastBuildUpPass.index] = lastPassAngleRating
		newAttributes['lastPassDistToOpRank'][lastBuildUpPass.index] = lastPassDistToOpRating
		newAttributes['lastPassDistToPosRank'][lastBuildUpPass.index] = lastPassDistToPosRating
		newAttributes['lastPassDistanceToGoal'][lastBuildUpPass.index] = lastPassDistanceToGoal
		newAttributes['lastPassAngle'][lastBuildUpPass.index] = lastPassAngle
		newAttributes['lastPassDistToOp'][lastBuildUpPass.index] = lastPassDistToOp
		newAttributes['lastPassDistToPos'][lastBuildUpPass.index] = lastPassDistToPos
		lastBuildUpPossession = players[players['InBallPos'] == 1]
		lastBuildUpPossession = lastBuildUpPossession[lastBuildUpPossession['Ts'] == endTime]
		if (len(lastBuildUpPossession) != 1):
			continue
		finalDistanceToGoal = lastBuildUpPossession['distanceToOpponentGoal'].item()
		newAttributes['finalDistanceToGoal'][lastBuildUpPossession.index] = finalDistanceToGoal

	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	tmp_lastPassRank = 'The positioning rating of the player who received the pass'
	tmp_lastPassDistanceToGoalRank = 'The distance rating of the player who received the pass'
	tmp_lastPassAngleRank = 'The pass angle rating of the player who received the pass'
	tmp_lastPassDistToOpRank = 'The distance rating to opponent rating of the player who received the pass'
	tmp_lastPassDistToPosRank = 'The distance rating to the player in possession rating of the player who received the pass'

	tmp_lastPassPopRank = 'The population basesd positioning rating of the player who received the pass'
	tmp_lastPassPopDistanceToGoalRank = 'The population basesd distance rating of the player who received the pass'
	tmp_lastPassPopAngleRank = 'The population basesd pass angle rating of the player who received the pass'
	tmp_lastPassPopDistToOpRank = 'The population basesd distance rating to opponent rating of the player who received the pass'
	tmp_lastPassPopDistToPosRank = 'The population basesd distance rating to the player in possession rating of the player who received the pass'

	tmp_lastPassDistanceToGoal = 'The distance rating of the player who received the pass'
	tmp_lastPassAngle = 'The pass angle rating of the player who received the pass'
	tmp_lastPassDistToOp = 'The distance to opponent rating of the player who received the pass'
	tmp_lastPassDistToPos = 'The distance to the player in possession rating of the player who received the pass'
	tmp_finalDistanceToGoal = 'The player\'s distance to the goal at the end of the builUp'

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'lastPassRank': tmp_lastPassRank,'lastPassDistanceToGoalRank': tmp_lastPassDistanceToGoalRank,'lastPassAngleRank': tmp_lastPassAngleRank,'lastPassDistToOpRank': tmp_lastPassDistToOpRank,'lastPassDistToPosRank': tmp_lastPassDistToPosRank,'lastPassDistanceToGoal': tmp_lastPassDistanceToGoal,'lastPassAngle': tmp_lastPassAngle,'lastPassDistToOp': tmp_lastPassDistToOp,'lastPassDistToPos': tmp_lastPassDistToPos,'finalDistanceToGoal': tmp_finalDistanceToGoal, 'lastPassPopRank': tmp_lastPassPopRank, 'lastPassPopDistanceToGoalRank': tmp_lastPassPopDistanceToGoalRank, 'lastPassPopAngleRank': tmp_lastPassPopAngleRank, 'lastPassPopDistToOpRank': tmp_lastPassPopDistToOpRank, 'lastPassPopDistToPosRank': tmp_lastPassPopDistToPosRank}

	attributeLabel.update(attributeLabel_tmp)
	# altogether = pd.concat([rawDict,attributeDict], axis=1)
	# altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

	return attributeDict,attributeLabel

def populationBasedRankings(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF,skipSpatAgg):
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['popDistanceToGoalRank','popAngleRank','popDistToOpRank','popDistToPosRank','popPositioningRank'])

	goalkeeperA,goalkeeperB = determineGoalKeepers(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,passDF)

	distToGoalValues = [0, 34.83, 41.856, 47.572, 52.652, 57.783, 62.979, 69.427, 77.786]
	angleValues = [0, 13.055, 18.973, 24.462, 29.664, 35.385, 41.8, 50.063, 61.944]
	distToOpValues = [0, 4.04, 5.847, 7.406, 8.854, 10.367, 12.0, 13.904, 16.587]
	distToPosValues = [0, 7.192, 9.303, 11.13, 12.945, 14.976, 17.283, 20.25, 25.028]

	allDict = pd.concat([rawDict, attributeDict], axis=1)
	allDict = allDict.loc[:,~allDict.columns.duplicated()]
	allDict = allDict.sort_values('Ts')

	tmp = allDict[allDict['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']

	newAttributes.loc[players.index,'popDistanceToGoalRank'] = np.nan
	newAttributes.loc[players.index,'popAngleRank'] = np.nan
	newAttributes.loc[players.index,'popDistToOpRank'] = np.nan
	newAttributes.loc[players.index,'popDistToPosRank'] = np.nan
	newAttributes.loc[players.index,'popPositioningRank'] = np.nan
	players = players[players['PlayerID'] != goalkeeperA]
	players = players[players['PlayerID'] != goalkeeperB]

	for x in range(0,9):
		if (x < 8):
			#AngleOpponentToPassline
			anglePlayers = players[players['angleOpponentToPassline'] >= angleValues[x]]
			anglePlayers = anglePlayers[anglePlayers['angleOpponentToPassline'] < angleValues[x+1]]
			if KNVB:
				newAttributes['popAngleRank'][anglePlayers.index] = x+1 #KNVB
			else:
				newAttributes['popAngleRank'][anglePlayers.index] = 9 - x * 1 #Leiden

			#distanceToGoal
			distToGoalPlayers = players[players['distanceToOpponentGoal'] >= distToGoalValues[x]]
			distToGoalPlayers = distToGoalPlayers[distToGoalPlayers['distanceToOpponentGoal'] < distToGoalValues[x+1]]
			if KNVB:
				newAttributes['popDistanceToGoalRank'][distToGoalPlayers.index] = 9 - x * 1 #KNVB
			else:
				newAttributes['popDistanceToGoalRank'][distToGoalPlayers.index] = x+1 #Leiden

			#distanceToPossession
			distToPoslayers = players[players['distanceToInPossession'] >= distToPosValues[x]]
			distToPoslayers = distToPoslayers[distToPoslayers['distanceToInPossession'] < distToPosValues[x+1]]
			if KNVB:
				newAttributes['popDistToPosRank'][distToPoslayers.index] = x+1 #KNVB
			else:
				newAttributes['popDistToPosRank'][distToPoslayers.index] = 9 - x * 1 #Leiden

			#distanceToOpponent
			distToOplayers = players[players['dist to closest visitor'] >= distToOpValues[x]]
			distToOplayers = distToOplayers[distToOplayers['dist to closest visitor'] < distToOpValues[x+1]]
			if KNVB:
				newAttributes['popDistToOpRank'][distToOplayers.index] = x+1 #KNVB
			else:
				newAttributes['popDistToOpRank'][distToOplayers.index] = 9 - x * 1 #Leiden

		else:
			anglePlayers = players[players['angleOpponentToPassline'] >= angleValues[x]]
			if (KNVB):
				newAttributes['popAngleRank'][anglePlayers.index] = 9 #KNVB
			else:
				newAttributes['popAngleRank'][anglePlayers.index] = 1 #Leiden

			distToGoalPlayers = players[players['distanceToOpponentGoal'] >= distToGoalValues[x]]
			if (KNVB):
				newAttributes['popDistanceToGoalRank'][distToGoalPlayers.index] = 1 #KNVB
			else:
				newAttributes['popDistanceToGoalRank'][distToGoalPlayers.index] = 9 #Leiden

			distToPoslayers = players[players['distanceToInPossession'] >= distToPosValues[x]]
			if (KNVB):
				newAttributes['popDistToPosRank'][distToPoslayers.index] = 9 #KNVB
			else:
				newAttributes['popDistToPosRank'][distToPoslayers.index] = 1 #Leiden

			#distanceToOpponent
			distToOplayers = players[players['dist to closest visitor'] >= distToOpValues[x]]
			if (KNVB):
				newAttributes['popDistToOpRank'][distToPoslayers.index] = 9 #KNVB
			else:
				newAttributes['popDistToOpRank'][distToPoslayers.index] = 1 #Leiden

	nanAngle = players[players['angleOpponentToPassline'].isnull()]
	if (KNVB):
		newAttributes['popAngleRank'][nanAngle.index] = 9 #Leiden
	else:
		newAttributes['popAngleRank'][nanAngle.index] = 1 #Leiden

	newAttributes['popPositioningRank'][players.index] = newAttributes['popAngleRank'][players.index] + newAttributes['popDistanceToGoalRank'][players.index] + newAttributes['popDistToPosRank'][players.index] + newAttributes['popDistToOpRank'][players.index]

	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	tmp_popAngleRank= 'The population based rating for the angle from an opponent to the passline'
	tmp_popDistanceToGoalRank = 'The population based ranking for the distance to the goal of a player'
	tmp_popDistToPosRank = 'The population based ranking for a player to the player in possession'
	tmp_popDistToOpRank = 'The population based ranking for the disting from a player to his opponent'
	tmp_popPositioningRank = 'The population based ranking for the overall positioning of a player'
	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'popAngleRank': tmp_popAngleRank,'popDistanceToGoalRank': tmp_popDistanceToGoalRank,'popDistToPosRank': tmp_popDistToPosRank,'popDistToOpRank': tmp_popDistToOpRank,'popPositioningRank': tmp_popPositioningRank}

	attributeLabel.update(attributeLabel_tmp)
	# altogether = pd.concat([rawDict,attributeDict], axis=1)
	# altogether.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/test.csv')

	return attributeDict,attributeLabel
