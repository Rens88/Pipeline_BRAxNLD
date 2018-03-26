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
from warnings import warn
import pdb; #pdb.set_trace()

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
def process(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	# Use this is an example for a GROUP level aggregate
	attributeDict_EXAMPLE,attributeLabel_EXAMPLE = \
	teamCentroid_panda(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	# Use this is an example for a PLAYER level aggregate
	attributeDict_EXAMPLE,attributeLabel_EXAMPLE = \
	distanceToCentroid(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	attributeDict,attributeLabel = \
	control(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	attributeDict,attributeLabel = \
	distToBall(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	attributeDict,attributeLabel = \
	ballPossession(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)

	# NB: Centroid and distance to centroid are stored in example variables that are not exported
	# when 'process' is finished, because these features are already embedded in the main pipeline.
	# Make sure that the name of the output variables that you create corresponds to the variables
	# that this function returns (i.e., 'attributeDict' and 'attributeLabel').

	return attributeDict,attributeLabel

#def zone(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):

def distToBall(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	###############
	# Use this as an example to compute a PLAYER level variable. Pay attention to the indexing. Let me know if you have an easier way.
	###############
	
	##### THE DATA #####
	# In this case, the new attribute will be computed based on a group (i.e., team) value
	ballComplete = rawDict[rawDict['PlayerID'] == 'ball']

	# NB Ball er nog uit!!
	tmp = rawDict[rawDict['PlayerID'] != 'ball']
	players = tmp[tmp['PlayerID'] != 'groupRow']

	# Create empty DataFrame to store results, NB: columns need to be assigend beforehand.
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distToBall', 'InPossession'])
	newAttributes['InPossession'] = 0

	lowestDistance = 999999;
	playerInPossession = 0;
	
	# For every player in the dataFrame
	for idx,i in enumerate(ballComplete['Ts']):
		curTime = ballComplete['Ts'][idx]
		
		curBallX = ballComplete['X'][idx]
		curBallY = ballComplete['Y'][idx]

		# Take all corresponding Ts (for PlayerID != 'groupRow')
		curPlayersX = players['X'][players['Ts'] == curTime]
		curPlayersY = players['Y'][players['Ts'] == curTime]

		curPlayersID = players['PlayerID'][players['Ts'] == curTime]

		curPlayer_distToBall = np.sqrt((curPlayersX - curBallX)**2 + (curPlayersY - curBallY)**2)
		newAttributes['distToBall'][curPlayer_distToBall.index] = curPlayer_distToBall
		# print(curPlayer_distToBall)
		# mindist = 
		# print('--')
		idxPossession = curPlayer_distToBall[curPlayer_distToBall == min(curPlayer_distToBall)].index
		newAttributes['InPossession'][idxPossession] = 1
		# # tmpRange = np.arange(int(idxPossession)-5,int(idxPossession)+5)
		# print(newAttributes['InPossession'][idxPossession-1])
		# print(newAttributes['InPossession'][idxPossession])
		# print(newAttributes['InPossession'][idxPossession+1])
		# IDEA??
		# Duration threshold?

		# absolute threshold (<3m?)

		# velocity (same direction?)

		# prioritization?


		#print(curPlayersX,curPlayersY)

		# Take the smallest distance
		#if any(curPlayer_distToBall < lowestDistance):
		#	lowestDistance = curPlayer_distToBall
		#	playerInPossession = curPlayersID
		#	print(playerInPossession)

		# newAttributes['distToBall'][curPlayersID.index] = curPlayer_distToBall[curPlayersID.index]

	'''
	for idx,i in enumerate(pd.unique(rawDict['PlayerID'])):
		curPlayer = rawDict[rawDict['PlayerID'] == i]
		curPlayerDict = curPlayer.set_index('Ts')

		if all(curPlayer['PlayerID'] != 'groupRow') and all(curPlayer['PlayerID'] != 'ball'):
			curPlayer_distToBall = np.sqrt((curPlayerDict['X'] - ball['X'])**2 + (curPlayerDict['Y'] - ball['Y'])**2)
			
			print(curPlayer_distToBall)
			print('---')
			print(lowestDistance)
			print(curPlayer_distToBall < lowestDistance)
			#curPlayer_distToBall = curPlayer_distToBall[~np.isnan(curPlayer_distToBall)]
			if any(curPlayer_distToBall < lowestDistance):
				lowestDistance = curPlayer_distToBall
				playerInPossession = curPlayer['PlayerID'];
				print(playerInPossession)
			#print(curPlayer_distToBall)
			pdb.set_trace()

		else:
			continue


		# Put compute values in the right place in the dataFrame
		newAttributes['distToBall'][curPlayer.index] = curPlayer_distToBall[curPlayerDict.index]
	'''

	'''
	for idx,i in enumerate(pd.unique(rawDict['Ts'])):
		curTs = rawDict[rawDict['Ts'] == i]
		curTsDict = curTs.set_index('Ts')
		print(idx, min(curPlayer_distToBall[curTsDict.index]))
	'''

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)
	# altogether = pd.concat([rawDict,attributeDict], axis=1)
	# altogether.to_csv('D:\\KNVB\\test.csv')

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	tmpDistToBallString = 'Distance from player to ball.'
	attributeLabel.update({'distToBall':tmpDistToBallString})
	print('voeg string description toe voor possession')
	
	return attributeDict,attributeLabel

#Victor
def ballPossession(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	TeamVals = attributeDict[rawDict['PlayerID'] == 'groupRow'].set_index('Ts')
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['ballPosession'])
	lowestDistance = 999999;
	playerInPossession = 0;

	#Search the ball in de DataFrame (Kan dit zonder for-loop)?
	for idx,i in enumerate(pd.unique(rawDict['PlayerID'])):
		ball = rawDict[rawDict['PlayerID'] == i]
		ballDict = ball.set_index('Ts')
		if all(ball['PlayerID'] == 'ball'):
			for idx,j in enumerate(pd.unique(rawDict['PlayerID'])):
				curPlayer = rawDict[rawDict['PlayerID'] == j]
				curPlayerDict = curPlayer.set_index('Ts')
				if all(ball['PlayerID'] == 'ball'):
					continue
				elif all(ball['PlayerID'] == 'groupRow'):
					continue
				else:
					curPlayer_distToBall = np.sqrt((curPlayerDict['X'] - ballDict['X'])**2 + (curPlayerDict['Y'] - ballDict['Y'])**2)
					if(curPlayer_distToBall < lowestDistance):
						lowestDistance = curPlayer_distToBall
						playerInPossession = curPlayer['PlayerID'];
						#print(playerInPossession)

	for idx,k in enumerate(pd.unique(rawDict['PlayerID'])):
		currentPlayer = rawDict[rawDict['PlayerID'] == k]
		currentPlayer = currentPlayer.set_index('Ts')
		if all(currentPlayer['PlayerID'] == playerInPossession):
			newAttributes['ballPosession'][currentPlayer.index] = 1;
		#elif all(currentPlayer['PlayerID'] != playerInPossession):
			#newAttributes['ballPosession'][currentPlayer.index] = 0;

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	tmpPossessionString = 'Bool Player in Possession (m)'
	attributeLabel.update({'InPossession':tmpPossessionString})

	return attributeDict,attributeLabel


'''
def ballPossession(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	
	notball = attributeDict[rawDict['PlayerID'] != 'ball'].set_index('Ts')
	distToBall = notball['distToBall']
	#distToBall = distToBall[~np.isnan(distToBall)]
	for idx,i in enumerate(pd.unique(rawDict['Ts'])):
		ballposs = [k for k in distToBall if distToBall[k] == min(distToBall[k])]
		print(ballposs)
		#print(ballPoss)
	
	for idx,i in enumerate(pd.unique(rawDict['Ts'])):
		#print(idx, min(attributeDict['distToBall']))
		continue

	return attributeDict,attributeLabel
'''

def control(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	ball = attributeDict[rawDict['PlayerID'] == 'ball'].set_index('Ts')
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['avgRelSpeedPlayerBall','control'])

	##LT: how to determine this constant? 
	constant = 1
	
	for idx,i in enumerate(pd.unique(rawDict['PlayerID'])):
		curPlayer = rawDict[rawDict['PlayerID'] == i]
		curPlayerDict = curPlayer.set_index('Ts')
		curPlayerAtt = attributeDict[rawDict['PlayerID'] == i]
		curPlayerDictAtt = curPlayerAtt.set_index('Ts')

		##LT: if player is in ball possesion? 
		if all(curPlayer['PlayerID'] != 'groupRow') and all(curPlayer['PlayerID'] != 'ball'):
			#average relative speed of player and ball. Always value between 0 and 1
			curPlayer_speedDiffBall = abs((curPlayerDictAtt['Snelheid'] - ball['Snelheid'])/ball['Snelheid'])
			control = 1 - constant * curPlayer_speedDiffBall**2
			#print(control)
		else:
			continue

		# Put compute values in the right place in the dataFrame
		newAttributes['avgRelSpeedPlayerBall'][curPlayer.index] = curPlayer_speedDiffBall[curPlayerDict.index]
		newAttributes['control'][curPlayer.index] = control[curPlayerDict.index]

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	tmpAvgRelSpeedPlayerBall = 'Average relative speed of player and ball.'
	tmpControl = 'Control of the player with ball'

	attributeLabel_tmp = {'avgRelSpeedPlayerBall': tmpAvgRelSpeedPlayerBall, 'control': tmpControl}
	attributeLabel.update(attributeLabel_tmp)
	
	return attributeDict,attributeLabel

#def pressure(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):



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