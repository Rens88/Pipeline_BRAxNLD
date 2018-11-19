# 08-12-2017 Rens Meerhoff

import matplotlib.pyplot as plt
import csv
import pdb; #pdb.set_trace()
import numpy as np
from os.path import isfile, join, isdir
from os import listdir, path
from warnings import warn
import math
# From my own library:
import plotSnapshot
import safetyWarning
import pandas as pd
import time
import student_LT_spatialAggregation

if __name__ == '__main__':
	process(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring)
	# IDEA: Could split up module in sub-module with team and individual level attributes separately.

	# Inidividual attributes (CHECK individualAttributes.py for more details, including a start of a script that checks which players are currently in play)
	vNorm(rawDict, frameRateData)
	# Nonlinear pedagogy only:
	# Every new Run (Nonlinear pedagogy data only) has a jump in time (and position), for which velocity is set to 0.
	correctVNorm(rawDict,attributeDict)

	# Team attributes
	teamCentroid(indsMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,TeamAstring,TeamBstring)
	teamSpread(attributeDict,uniqueTsS,uniquePlayers,teamMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,indsMatrix,TeamAstring,TeamBstring)
	teamSurface(indsMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,TeamAstring,TeamBstring)
	groupSurface(X,Y)

	obtainIndices(rawDict,TeamAstring,TeamBstring)

	#####################################################################################
	#####################################################################################

def process(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg,eventsPanda,spatAggFolder,spatAggFname,debuggingMode):
	tSpatAgg = time.time()
	# Per Match (i.e., file)
	# Per Team and for both teams

	# Ball possession
	# computeBallPossession = False # temporarily turned off
	# if computeBallPossession:
	# 	attributeDict,attributeLabel = \
	# 	ballPossession(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg)

	# # Use this is an example for a GROUP level aggregate
	# attributeDict,attributeLabel = \
	# teamCentroid_panda(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg)
	# if debuggingMode:
	# 	elapsed = str(round(time.time() - tSpatAgg, 2))
	# 	print('*****----- Time elapsed during spatialAggregation.teamCentroid_panda(): %ss' %elapsed)
	# 	tSpatAgg_cent = time.time()

	# # Use this is an example for a PLAYER level aggregate
	# attributeDict,attributeLabel = \
	# distanceToCentroid(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg)
	# if debuggingMode:
	# 	elapsed = str(round(time.time() - tSpatAgg_cent, 2))
	# 	print('*****----- Time elapsed during spatialAggregation.distanceToCentroid(): %ss' %elapsed)
	# 	tSpatAgg_dist = time.time()

	# attributeDict,attributeLabel = \
	# teamSpread_asPanda(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg)
	# if debuggingMode:
	# 	elapsed = str(round(time.time() - tSpatAgg_dist, 2))
	# 	print('*****----- Time elapsed during spatialAggregation.teamSpread_asPanda(): %ss' %elapsed)
	# 	tSpatAgg_spread = time.time()

	# attributeDict,attributeLabel = \
	# teamSurface_asPanda(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg)
	# if debuggingMode:
	# 	elapsed = str(round(time.time() - tSpatAgg_spread, 2))
	# 	print('*****----- Time elapsed during spatialAggregation.teamSurface_asPanda(): %ss' %elapsed)
	# 	tSpatAgg_surf = time.time()

	# attributeDict,attributeLabel = \
	# vNorm(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg)
	# if debuggingMode:
	# 	elapsed = str(round(time.time() - tSpatAgg_surf, 2))
	# 	print('*****----- Time elapsed during spatialAggregation.vNorm(): %ss' %elapsed)
	tSpatAgg_vNorm = time.time()

	attributeDict,attributeLabel = \
	student_LT_spatialAggregation.process(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg)
	if debuggingMode:
		elapsed = str(round(time.time() - tSpatAgg_vNorm, 2))
		print('*****----- Time elapsed during spatialAggregation.student_LT_spatialAggregation(): %ss' %elapsed)
		tSpatAgg_student = time.time()
	## debugging only
	# allesBijElkaar = pd.concat([rawDict, attributeDict], axis=1) # debugging only
	# allesBijElkaar.to_csv('C:\\Users\\rensm\\Documents\\PostdocLeiden\\BRAxNLD repository\\Data\\tmp\\test.csv') # debugging only		
	# pdb.set_trace()		 
	
	# Spatially aggregated data
	print('skipspat',skipSpatAgg)
	if not skipSpatAgg: #LT: terugzetten!
		spatAggPanda = pd.concat([rawDict, eventsPanda.loc[:, eventsPanda.columns != 'Ts'], attributeDict.loc[:, attributeDict.columns != 'Ts']], axis=1) # Skip the duplicate 'Ts' columns
		spatAggPanda.to_csv(spatAggFolder + spatAggFname)

	# print(attributeDict.columns.values,attributeLabel)
	# #LT: drop defender variables
	# excludeList = ['InBallPos','Shirt','dist to closest home','dist to closest visitor','pressureFromDefender','pressureZone','angleInPossDefGoal','distToPlayerWithBall','shotDensityFromDefender','playerInIZ']
	# try:
	# 	attributeDict = attributeDict.drop(excludeList, axis=1)
	# 	for e in excludeList:
	# 		attributeLabel.pop(e)
	# except:
	# 	print('\nWARN: Can not drop the columns out of the attributeDict.')
	# 	pass
	# print(attributeDict.columns.values,attributeLabel)
	# pdb.set_trace()

	if debuggingMode:
		elapsed = str(round(time.time() - tSpatAgg, 2))
		print('***** Time elapsed during spatialAggregation: %ss' %elapsed)
	
	return attributeDict, attributeLabel

############################################################
############################################################

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
	

	########### attempt to improve speed

	# # For every player in the dataFrame
	# for idx,i in enumerate(pd.unique(rawDict['PlayerID'])):
	# 	curPlayer = rawDict[rawDict['PlayerID'] == i]
	# 	curPlayerDict = curPlayer.set_index('Ts')

	# 	if all(curPlayer['PlayerID'] == 'groupRow'):
	# 		# It's actually not a player, but a group, so skip it.
	# 		continue # do nothing
	# 	elif all(curPlayer['PlayerID'] == 'ball'):
	# 		# It's actually not a player, but the ball, so skip it.
	# 		continue # do nothing
	# 	elif all(curPlayer['TeamID'] == TeamAstring):
	# 		# Compute the distance to the centroid, NB: team specific!!
	# 		curPlayer_distToCent = np.sqrt((curPlayerDict['X'] - TeamVals['TeamCentXA'])**2 + (curPlayerDict['Y'] - TeamVals['TeamCentYA'])**2)
	# 	elif all(curPlayer['TeamID'] == TeamBstring):
	# 		# Compute the distance to the centroid, NB: team specific!!
	# 		curPlayer_distToCent = np.sqrt((curPlayerDict['X'] - TeamVals['TeamCentXB'])**2 + (curPlayerDict['Y'] - TeamVals['TeamCentYB'])**2)
	
	# ########### attempt 2 to improve speed
	# # Groupby player ID
	
	# # Groupby method:
	# tmp = dfA[['Ts','X','Y', 'PlayerID']]
	# data_perPlayer = tmp.groupby(['PlayerID'])

	# # for iterated_PLAYER, group in data_perPlayer:


	# # and then loop for time.

	# ########### / attempt to improve speed

	# Pivot X and Y dataframes for Team A
	try:
		# 	# Pivotting method:
		# Team_A_X_pivotted = dfA.pivot(columns='Ts', values='X')		

		# Groupby method:
		tmp = dfA[['Ts','X','Y']]
		# print(len(tmp['Ts'].unique()))
		Team_A_XY = tmp.groupby(['Ts'])
		
	except:
		Team_A_X = np.nanmean(dfA['X'])
		print(Team_A_X.shape)
		print(dfA['X'].shape)
		print(len(ind_groupRows))
		warn('Tried to pivot Team_A_X. but it didnt work. Find a different way, because dataset is too large for pivoting')
		pdb.set_trace()

	# try:
	# 	##Team_A_Y = dfA.pivot(columns='Ts', values='Y')
	# 	# Groupby method:
	# 	tmp = dfA[['Ts','Y']]
	# 	Team_A_Y = tmp.groupby(['Ts'])
		
	# except:
	# 	Team_A_Y = np.nanmean(dfA['Y'])
	# 	print(dfA.shape)
	# 	warn('Tried to pivot Team_A_Y. but it didnt work. Find a different way, because dataset is too large for pivoting')
	# 	pdb.set_trace()

	# Pivot X and Y dataframes for Team B
	try:
		##Team_B_X = dfB.pivot(columns='Ts', values='X')
		# Groupby method:
		tmp = dfB[['Ts','X','Y']]
		Team_B_XY = tmp.groupby(['Ts'])		
	except:
		print(dfB.shape)
		warn('Tried to pivot Team_B_X. but it didnt work. Find a different way, because dataset is too large for pivoting')
		pdb.set_trace()
	# try:
	# 	##Team_B_Y = dfB.pivot(columns='Ts', values='Y')   
	# 	# Groupby method:
	# 	tmp = dfB[['Ts','Y']]
	# 	Team_B_Y = tmp.groupby(['Ts'])
		
	# except:
	# 	print(dfB.shape)
	# 	warn('Tried to pivot Team_B_Y. but it didnt work. Find a different way, because dataset is too large for pivoting')
	# 	pdb.set_trace()

	# The warnings below were included to debug problems with different lengths per team and/or group row
	####### if len(ind_groupRows) != Team_A_X.shape[1]:
	if len(ind_groupRows) != Team_A_XY['X'].size:
		warn('\nWARNING: (potentially fatal) The number of groupRows does not correspond to the number of entries identified for <%s>.\nMOST LIKELY CAUSE: There are less ''groupRows'' than there are unique timeStamps\nAlternative reasons: This is either due to incorrect identification of groupRows (and subsequent allocation of the string <groupRow> in its TeamID).\nOr, this could be due to issues with identifying <%s>.\nUPDATE: Should now be cleaned in cleanupData.py in verifyGroupRows().\nALTERNATIVELY: As a result of a non-chronological dataset, the interpolation in cleanup doesnt work well.\n' %(TeamAstring,TeamAstring))
	####### if len(ind_groupRows) != Team_B_X.shape[1]:
	if len(ind_groupRows) != Team_B_XY['X'].size:		
		warn('\nWARNING: (potentially fatal) The number of groupRows does not correspond to the number of entries identified for <%s>.\nMOST LIKELY CAUSE: There are less ''groupRows'' than there are unique timeStamps\nAlternative reasons: This is either due to incorrect identification of groupRows (and subsequent allocation of the string <groupRow> in its TeamID).\nOr, this could be due to issues with identifying <%s>.\nUPDATE: Should now be cleaned in cleanupData.py in verifyGroupRows().\nALTERNATIVELY: As a result of a non-chronological dataset, the interpolation in cleanup doesnt work well.\n' %(TeamBstring,TeamBstring))
	##############if Team_A_X.shape[1] != Team_B_X.shape[1]:
	if Team_A_XY['X'].size != Team_B_XY['X'].size:
		warn('\nWARNING: (potentially fatal) \n\
	The number of rows (i.e., unique timestamps) identified for <%s> (n = %s frames) != <%s> (n = %s frames).\n\
	In other words. For some period of time, only players from one team were registered.\n\
	HERE I solved it by making the computation for each team separately.\n\
	Alternatively, we could: \n\
		1) clean up the data to include missing timestamps (or exclude timestamps that only occur for one team)\n\
		2) Write the analysis to only compute the spatial aggregate when there is data from both teams.\n\
	NB: As long as the difference in n is small (e.g., less than a second), the impact is minimal.\n\
#######################################################################################################'\
 %(TeamAstring,Team_A_XY['X'].size,TeamBstring,Team_B_XY['X'].size))
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
	pd.options.mode.chained_assignment = None  # default='warn' # NB: The code below gives a warning because it may be uncertain whether the right ind_groupRows are called. If you know a work-around, let me know.
		
	# For team A
	############## if Team_A_X.shape == (0,0):
	if Team_A_XY['X'].size == 0:		
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
	# print(ind_groupRowsA)
	# print(len(ind_groupRowsA))
	# print(Team_A_XY['X'].size)
	# print(len(uniqueTs_TeamA_Rows))

	# print(Team_A_XY['X'].mean())
	# print(any(Team_A_XY['X'].isnull()))
	newAttributesA['TeamCentXA'][ind_groupRowsA] = Team_A_XY['X'].mean()
	newAttributesA['TeamCentYA'][ind_groupRowsA] = Team_A_XY['Y'].mean()
	newAttributesA['LengthA'][ind_groupRowsA] = Team_A_XY['X'].max() - Team_A_XY['X'].min()
	newAttributesA['WidthA'][ind_groupRowsA] = Team_A_XY['Y'].max() - Team_A_XY['Y'].min()

	# And for team B
	###########if Team_B_X.shape == (0,0):
	if Team_B_XY['X'].size == 0:		
	  exit('\nFATAL WARNING: No data in TeamA\'s X positions.\nThis is likely because the TeamAstring and TeamBstring do not correspond with the strings in the dataset.\nThis may be because the user input is incorrect OR, because the Team strings are unsuccesfully derived from the filename (especialy when using dataType FDP for a new dataset):\nConsider writing a specific function in dissectFilename.py.\n')
	# print(Team_A_X.shape)  
	# print(len(ind_groupRowsA))
	# print(Team_B_X.shape) 
	# print(len(ind_groupRowsB))
	# pdb.set_trace()
	###########newAttributesB['TeamCentXB'][ind_groupRowsB] = Team_B_X.mean(axis=0, skipna=True)
	###########newAttributesB['TeamCentYB'][ind_groupRowsB] = Team_B_Y.mean(axis=0, skipna=True)
	###########newAttributesB['LengthB'][ind_groupRowsB] = Team_B_X.max(axis=0, skipna=True) - Team_B_X.min(axis=0, skipna=True)
	###########newAttributesB['WidthB'][ind_groupRowsB] = Team_B_Y.max(axis=0, skipna=True) - Team_B_Y.min(axis=0, skipna=True)
	
	newAttributesB['TeamCentXB'][ind_groupRowsB] = Team_B_XY['X'].mean()
	newAttributesB['TeamCentYB'][ind_groupRowsB] = Team_B_XY['Y'].mean()
	newAttributesB['LengthB'][ind_groupRowsB] = Team_B_XY['X'].max() - Team_B_XY['X'].min()
	newAttributesB['WidthB'][ind_groupRowsB] = Team_B_XY['Y'].max() - Team_B_XY['Y'].min()

	pd.options.mode.chained_assignment = 'warn'  # default='warn'

	warn('\nUnverified assumption: field width = X-axis, field length = Y-axis\n')

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributesA, newAttributesB], axis=1)
	
	return attributeDict,attributeLabel

def distanceToCentroid(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg):
	###############
	# Use this as an example to compute a PLAYER level variable. Pay attention to the indexing. Let me know if you have an easier way.
	###############

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	tmpDistToCentString = 'Player\'s distance to its team\'s centroid (m)'
	attributeLabel.update({'distToCent':tmpDistToCentString})
		
	if skipSpatAgg: # Return eary if spatial aggregation is being skipped
		return attributeDict,attributeLabel

	##### THE DATA #####
	# In this case, the new attribute will be computed based on a group (i.e., team) value
	TeamVals = attributeDict[rawDict['PlayerID'] == 'groupRow'].set_index('Ts')
	# Create empty DataFrame to store results, NB: columns need to be assigend beforehand.
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distToCent'], dtype = np.float64)
	
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

	return attributeDict,attributeLabel

def teamSpread_asPanda(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg):
	###############
	# another GROUP level variable
	###############

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	tmpSpreadAString = 'Average distance to center of %s (m)' %TeamAstring
	tmpStdSpreadAString = 'Standard deviation of distance to center of %s (m)' %TeamAstring
	tmpSpreadBString = 'Average distance to center of %s (m)' %TeamBstring
	tmpStdSpreadBString = 'Standard deviation of distance to center of %s (m)' %TeamBstring	
	tmpAtLa2 = {'SpreadA': tmpSpreadAString, 'SpreadB': tmpSpreadBString, 'stdSpreadA': tmpStdSpreadAString,'stdSpreadB': tmpStdSpreadBString}
	attributeLabel.update(tmpAtLa2)

	if skipSpatAgg: # Return eary if spatial aggregation is being skipped
		return attributeDict,attributeLabel

	##### THE DATA #####
	# Prepare the indices of the groupRows. This will be used to store the computed values in the index corresponding to attributeDict.
	ind_groupRows = attributeDict[rawDict['PlayerID'] == 'groupRow'].index
	ind_groupRowsA = ind_groupRows
	ind_groupRowsB = ind_groupRows

	# Separate the raw values per team
	dfA = attributeDict[rawDict['TeamID'] == TeamAstring]
	dfB = attributeDict[rawDict['TeamID'] == TeamBstring]
	# Pivot dataframes

	### Pivotting method:
	##Team_A_distToCent = dfA.pivot(columns='Ts', values='distToCent')
	##Team_B_distToCent = dfB.pivot(columns='Ts', values='distToCent')

	# Groupby method:
	tmp = dfA[['Ts','distToCent']]
	Team_A_distToCent = tmp.groupby(['Ts'])
	tmp = dfB[['Ts','distToCent']]
	Team_B_distToCent = tmp.groupby(['Ts'])


	####if Team_A_distToCent.shape[1] != Team_B_distToCent.shape[1]:
	if Team_A_distToCent['distToCent'].size != Team_B_distToCent['distToCent'].size:		
		warn('\nWARNING: Corrected groupRows per team. \nSee teamCentroid_panda() for more information.')
		# Index of the groupRows for every unique timestamp that exists for each team separately
		uniqueTs_TeamA_Rows = rawDict['Ts'][rawDict['TeamID'] == TeamAstring].unique()
		ind_groupRowsA = [i for i in ind_groupRows if np.isin(rawDict['Ts'][i],uniqueTs_TeamA_Rows)]
		
		uniqueTs_TeamB_Rows = rawDict['Ts'][rawDict['TeamID'] == TeamBstring].unique()
		ind_groupRowsB = [i for i in ind_groupRows if np.isin(rawDict['Ts'][i],uniqueTs_TeamB_Rows)]

	# Create empty DataFrame to store results, NB: columns need to be assigend beforehand.
	newAttributesA = pd.DataFrame(index = ind_groupRowsA,columns = ['SpreadA', 'stdSpreadA'])
	newAttributesB = pd.DataFrame(index = ind_groupRowsB,columns = ['SpreadB', 'stdSpreadB'])
		
	# Compute the new attributes
	pd.options.mode.chained_assignment = None  # default='warn' # NB: The code below gives a warning because it may be uncertain whether the right ind_groupRows are called. If you know a work-around, let me know.
	# newAttributesA['SpreadA'][ind_groupRowsA] = Team_A_distToCent.mean(axis=0, skipna=True)
	# newAttributesA['stdSpreadA'][ind_groupRowsA] = Team_A_distToCent.std(axis=0, skipna=True)
	# newAttributesB['SpreadB'][ind_groupRowsB] = Team_B_distToCent.mean(axis=0, skipna=True)
	# newAttributesB['stdSpreadB'][ind_groupRowsB] = Team_B_distToCent.std(axis=0, skipna=True)	
	newAttributesA['SpreadA'][ind_groupRowsA] = Team_A_distToCent['distToCent'].mean()
	newAttributesA['stdSpreadA'][ind_groupRowsA] = Team_A_distToCent['distToCent'].std()
	newAttributesB['SpreadB'][ind_groupRowsB] = Team_B_distToCent['distToCent'].mean()
	newAttributesB['stdSpreadB'][ind_groupRowsB] = Team_B_distToCent['distToCent'].std()

	pd.options.mode.chained_assignment = 'warn'  # default='warn'

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributesA, newAttributesB], axis=1)

	return attributeDict,attributeLabel

def teamSurface_asPanda(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg):
	###############
	# another GROUP level variable
	###############

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	tmpSurfaceAString = 'Surface area of %s ($m^2$)' %TeamAstring
	tmpSurfaceBString = 'Surface area of %s ($m^2$)' %TeamBstring
	tmpsumVerticesAString = 'Circumference of surface area of %s (m)' %TeamAstring
	tmpsumVerticesBString = 'Circumference of surface area of %s (m)' %TeamBstring
	tmpShapeRatioAString = 'Uniformity of surface area of %s (1 = uniform, closer to 0 = elongated)' %TeamAstring
	tmpShapeRatioBString = 'Uniformity of surface area of %s (1 = uniform, closer to 0 = elongated)' %TeamBstring

	tmpAtLa3 = {'SurfaceA': tmpSurfaceAString, 'SurfaceB': tmpSurfaceBString, \
	'SumVerticesA': tmpsumVerticesAString,'SumVerticesB': tmpsumVerticesBString, \
	'ShapeRatioA': tmpShapeRatioAString,'ShapeRatioB': tmpShapeRatioBString}	

	attributeLabel.update(tmpAtLa3)

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

	################################ Simulate the problem ##############################
	# # Simulate problems with missing values for a whole team
	# tmp = np.where(dfA['Ts'] == dfA.loc[205,'Ts'])
	# tmp1 = tmp[0].tolist()
	# dfA.drop(dfA.index[tmp1],inplace=True)
	################################ /Simulate the problem ##############################

	# ## Pivot dataframes
	# Team_AX = dfA.pivot(columns='Ts', values='X')
	# Team_AY = dfA.pivot(columns='Ts', values='Y')	
	# Team_BX = dfB.pivot(columns='Ts', values='X')
	# Team_BY = dfB.pivot(columns='Ts', values='Y')	

	# Groupby method:
	tmp = dfA[['Ts','X','Y']]
	Team_AXY = tmp.groupby(['Ts'])
	# tmp = dfA[['Ts','Y']]
	# Team_AY = tmp.groupby(['Ts'])
	tmp = dfB[['Ts','X','Y']]
	Team_BXY = tmp.groupby(['Ts'])
	# tmp = dfB[['Ts','Y']]
	# Team_BY = tmp.groupby(['Ts'])

	##### if Team_AX.shape[1] != Team_BX.shape[1]:
	if Team_AXY['X'].size != Team_BXY['X'].size:		
		warn('\nWARNING: Corrected groupRows per team. \nSee teamCentroid_panda() for more information.')
		# NB: I've adopted a slightly different solution compared to teamCentroid_panda() because of the structure of this function.
		# Index of the groupRows for every unique timestamp that exists for each team separately
	######uniqueTs_TeamA_Rows = rawDict['Ts'][rawDict['TeamID'] == TeamAstring].unique()
		# ind_groupRowsA = [i for i in ind_groupRows if np.isin(rawDict['Ts'][i],uniqueTs_TeamA_Rows)]
		
	#########uniqueTs_TeamB_Rows = rawDict['Ts'][rawDict['TeamID'] == TeamBstring].unique()
		# ind_groupRowsB = [i for i in ind_groupRows if np.isin(rawDict['Ts'][i],uniqueTs_TeamB_Rows)]

	# Compute the new attributes
	SurfaceA = []
	SumVerticesA = []
	ShapeRatioA = []
	SurfaceB = []
	SumVerticesB = []
	ShapeRatioB = []

	ind_groupRowsA_bygroups = []
	ind_groupRowsB_bygroups = []
	# For every time point in the dataFrame
	####	for idx,i in enumerate(pd.unique(rawDict.loc[ind_groupRows,'Ts'])):
	# i --> Ts value
	for Ts_name,XYvals in Team_AXY:

		curXPosA = XYvals['X'].as_matrix()
		curYPosA = XYvals['Y'].as_matrix()

		## I should write a warning for when there aren't the same number of players visible on court
		# I should either interpolate, or only export the surface when all players are visible
		# maxPlayersOfSurface = np.shape(curXPosA)

		curSurfaceA,curSumVerticesA,curShapeRatioA = groupSurface(curXPosA,curYPosA) 
		SurfaceA.append(curSurfaceA)
		SumVerticesA.append(curSumVerticesA)
		ShapeRatioA.append(curShapeRatioA)

		curTs_idx = ind_groupRowsA[rawDict.loc[ind_groupRowsA,'Ts'] == Ts_name].tolist()
		ind_groupRowsA_bygroups.append(curTs_idx[0])

	for Ts_name,XYvals in Team_BXY:

		curXPosB = XYvals['X'].as_matrix()
		curYPosB = XYvals['Y'].as_matrix()
		curSurfaceB,curSumVerticesB,curShapeRatioB = groupSurface(curXPosB,curYPosB) 
		SurfaceB.append(curSurfaceB)
		SumVerticesB.append(curSumVerticesB)
		ShapeRatioB.append(curShapeRatioB)

		curTs_idx = ind_groupRowsB[rawDict.loc[ind_groupRowsB,'Ts'] == Ts_name].tolist()
		ind_groupRowsB_bygroups.append(curTs_idx[0])



	# for idx,i in enumerate(pd.unique(rawDict.loc[ind_groupRows,'Ts'])):
	
	# 	skipA = False
	# 	skipB = False
	# 	if np.isin(i,uniqueTs_TeamA_Rows):
	# 		curXPosA = Team_AX[i][Team_AX[i].isnull() == False]
	# 		curYPosA = Team_AY[i][Team_AY[i].isnull() == False]
	# 	else:
	# 		SurfaceA.append(np.nan)
	# 		SumVerticesA.append(np.nan)
	# 		ShapeRatioA.append(np.nan)
	# 		skipA = True

	# 	if np.isin(i,uniqueTs_TeamB_Rows):
	# 		curXPosB = Team_BX[i][Team_BX[i].isnull() == False]
	# 		curYPosB = Team_BY[i][Team_BY[i].isnull() == False]
	# 	else:
	# 		SurfaceB.append(np.nan)
	# 		SumVerticesB.append(np.nan)
	# 		ShapeRatioB.append(np.nan)
	# 		skipB = True
	# 	# WARNING: groupSurface not equipped to deal with None and np.nan values.
	# 	# UPDATE: See stopping condition above (using skipA and skipB). It works when simple adding a nan to the array, rather than having it run through the functions.
	# 	# NB: Simply skipping it using an if-loop will be problematic with the current indexing method:
	# 	# using ind_groupRows assumes that there is a value for every timestep
	# 	if not skipA:
	# 		curXPosA = curXPosA.as_matrix()
	# 		curYPosA = curYPosA.as_matrix()	
	# 		curSurfaceA,curSumVerticesA,curShapeRatioA = groupSurface(curXPosA,curYPosA) 
	# 		SurfaceA.append(curSurfaceA)
	# 		SumVerticesA.append(curSumVerticesA)
	# 		ShapeRatioA.append(curShapeRatioA)

	# 	if not skipB:
	# 		curXPosB = curXPosB.as_matrix()
	# 		curYPosB = curYPosB.as_matrix()	
	# 		curSurfaceB,curSumVerticesB,curShapeRatioB = groupSurface(curXPosB,curYPosB)

	# 		SurfaceB.append(curSurfaceB)
	# 		SumVerticesB.append(curSumVerticesB)
	# 		ShapeRatioB.append(curShapeRatioB)

	## NB, using an old script for groupSurface(). So awkward way to re-convert to pandas
	if len(SurfaceA) != len(ind_groupRowsA):
		warn('\WARNING: some problems with less Ts in SurfaceA than in inds..\nSee <simulate the problem> above>\nShould have been solved by using <ind_groupRowsA_bygroups> instead of <ind_groupRowsA>')
	if len(SurfaceA) != len(ind_groupRowsA_bygroups):		
		warn('FATAL WARNING: The solution to deal with different indices lengths apparently didnt work...\n')
	dfSurfaceA = pd.DataFrame(data = SurfaceA,index = ind_groupRowsA_bygroups,columns = ['SurfaceA'])
	dfSumVerticesA = pd.DataFrame(data = SumVerticesA,index = ind_groupRowsA_bygroups,columns = ['SumVerticesA'])
	dfShapeRatioA = pd.DataFrame(data = ShapeRatioA,index = ind_groupRowsA_bygroups,columns = ['ShapeRatioA'])

	if len(SurfaceB) != len(ind_groupRowsB):
		warn('\nWARNING: some problems with less Ts in SurfaceA than in inds..\nSee <simulate the problem> above>\nShould have been solved by using <ind_groupRowsB_bygroups> instead of <ind_groupRowsB>')
	if len(SurfaceB) != len(ind_groupRowsB_bygroups):		
		warn('FATAL WARNING: The solution to deal with different indices lengths apparently didnt work...\n')
	# dfSurfaceB = pd.DataFrame(data = SurfaceB,index = ind_groupRowsB,columns = ['SurfaceB'])
	# dfSumVerticesB = pd.DataFrame(data = SumVerticesB,index = ind_groupRowsB,columns = ['SumVerticesB'])
	# dfShapeRatioB = pd.DataFrame(data = ShapeRatioB,index = ind_groupRowsB,columns = ['ShapeRatioB'])

	dfSurfaceB = pd.DataFrame(data = SurfaceB,index = ind_groupRowsB_bygroups,columns = ['SurfaceB'])
	dfSumVerticesB = pd.DataFrame(data = SumVerticesB,index = ind_groupRowsB_bygroups,columns = ['SumVerticesB'])
	dfShapeRatioB = pd.DataFrame(data = ShapeRatioB,index = ind_groupRowsB_bygroups,columns = ['ShapeRatioB'])

	# print('******* A *********')
	# print(dfSurfaceA.index)
	# print(dfSurfaceA.shape)
	# print(dfSurfaceA.keys())
	# print('******* B *********')
	# print(dfSurfaceB.index)
	# print(dfSurfaceB.shape)
	# print(dfSurfaceB.keys())
	# print('******* attributeDict *********')
	# print(attributeDict.index)
	# print(attributeDict.shape)
	# print(attributeDict.keys())

	# attributeDictB = pd.concat([attributeDict, \
	# 	dfSurfaceB,], axis=1)

	# attributeDictAB = pd.concat([attributeDictB, \
	# 	dfSurfaceA,], axis=1)
	attributeDict = pd.concat([attributeDict, \
		dfSurfaceA, dfSumVerticesA, dfShapeRatioA, \
		dfSurfaceB, dfSumVerticesB, dfShapeRatioB,], axis=1)

	return attributeDict,attributeLabel

#####################################################################################

def vNorm(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg):

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	attributeLabel_tmp = {'vNorm':'Speed (m/s)','distFrame':'Distance covered per frame (m)'}
	attributeLabel.update(attributeLabel_tmp)

	if skipSpatAgg: # Return eary if spatial aggregation is being skipped
		return attributeDict,attributeLabel

	##### THE DATA #####
	# In this case, the new attribute will be computed based on a group (i.e., team) value
	TeamVals = attributeDict[rawDict['PlayerID'] == 'groupRow'].set_index('Ts')
	# Create empty DataFrame to store results, NB: columns need to be assigend beforehand.
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['vNorm','distFrame'],dtype = np.float64)
	
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
		else:
			# Compute vNorm and distFrame for this player.
			vT = np.gradient(curPlayerDict.index)
			presumedFrameRate = np.median(vT)

			vX = np.gradient(curPlayerDict['X'],curPlayerDict.index)
			vY = np.gradient(curPlayerDict['Y'],curPlayerDict.index)
			vNorm = np.sqrt(vX**2 + vY**2)
			distFrame = vNorm * vT

			# A correction of vNorm for jumps in time that deviate from the expected measurement frequency.
			# Weakness: expected frequency (presumedFramerate) is based on the median of all frame rates. It only works if the majority of the timstamps are without jumps.
			if any(vT > 1.2*presumedFrameRate):
				# There seem to be 'jumps' in time.
				# NB: 1.2 is quite a strict cut-off. But the timeseries data can be expected to have a regular measurement frequency, so this strict cut-off should work.
				jumpsInTime = vT > 1.2*presumedFrameRate
				vNorm[jumpsInTime] = 0
				distFrame[jumpsInTime] = 0
				warn('\nWARNING: vNorm and distFrame were set to 0 around apparent jumps in time.\nIf vNorm is behaving oddly, check these parameter settings.')

				## I used to correct it using attributeDict, which should still work, but the above code is more generic.
				# def correctVNorm(rawDict,attributeDict):
				# 	if not 'Run' in attributeDict.keys(): # only normalize if 'runs' exists
				# 		return {'vNorm':attributeDict['vNorm']}
				# 	runs = np.array([i for i,val in enumerate(attributeDict['Run']) if val  != '' ])
				# 	if runs.size == 0:
				# 		warn('\n!!!!\nExisting attributes seem to be missing.\nCouldnt find runs to normalize velocity.\nVelocity not normalized.')
				# 		return {'vNorm':attributeDict['vNorm']}
				# 	runTimes = rawDict['Ts'][runs]
				# 	# output = attributeDict['vNorm']
				# 	output = {'vNorm':attributeDict['vNorm']}
				# 	for val in runTimes: # for every run time, make vNorm 0
				# 		for i,val2 in enumerate(rawDict['Ts']):
				# 			if val2 == val:
				# 				output['vNorm'][i] = 0
				# 	return output

			newAttributes['vNorm'][curPlayer.index] = vNorm
			newAttributes['distFrame'][curPlayer.index] = distFrame

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	return attributeDict,attributeLabel

#####################################################################################
#####################################################################################

# def teamCentroid(indsMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,TeamAstring,TeamBstring):

# 	print(np.shape(indsMatrix))
# 	print(np.shape(XpositionMatrix))
# 	print(np.shape(YpositionMatrix))
# 	# Compute the centroids
# 	CentXA = np.nanmean(XpositionMatrix[:,teamAcols],axis=1)
# 	CentXB = np.nanmean(XpositionMatrix[:,teamBcols],axis=1)
# 	CentYA = np.nanmean(YpositionMatrix[:,teamAcols],axis=1)
# 	CentYB = np.nanmean(YpositionMatrix[:,teamBcols],axis=1)

# 	# Return in format attributeDict
# 	dataShape = np.shape(XpositionMatrix) # Number of data entries

# 	tmpXA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
# 	tmpYA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
# 	tmpXB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
# 	tmpYB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan

# 	for idx,val in enumerate(indsMatrix): # indsMatrix are the team cells only
# 		tmpXA[int(val)] = CentXA[idx]
# 		tmpYA[int(val)] = CentXB[idx]
# 		tmpXB[int(val)] = CentXA[idx]
# 		tmpYB[int(val)] = CentYA[idx]
	
# 	print(tmpXA)
# 	pdb.set_trace()
# 	# the Data
# 	attributeDict_tmp = {'TeamCentXA': tmpXA, 'TeamCentYA': tmpYA, 'TeamCentXB': tmpXB,'TeamCentYB': tmpYB}

# 	# the Strings	
# 	tmpXAString = 'X-position of %s (m)' %TeamAstring
# 	tmpYAString = 'Y-position of %s (m)' %TeamAstring
# 	tmpXBString = 'X-position of %s (m)' %TeamBstring
# 	tmpYBString = 'Y-position of %s (m)' %TeamBstring			
# 	attributeLabel_tmp = {'TeamCentXA': tmpXAString, 'TeamCentYA': tmpYAString, 'TeamCentXB': tmpXBString,'TeamCentYB': tmpYBString}
# 	# return attributeDict_tmp,attributeLabel_tmp 
# 	return attributeDict_tmp,attributeLabel_tmp,tmpXA,tmpYA,tmpXB,tmpYB

# #####################################################################################

# def teamSpread(CentXA,CentYA,CentXB,CentYB,uniqueTsS,uniquePlayers,teamMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,indsMatrix,TeamAstring,TeamBstring):
# 	# Spread
# 	# (average) Distance of each player to center.
# 	# Dist to centre:
# 	distToTeamCent = np.ones((len(uniqueTsS),len(uniquePlayers)),dtype='float64')*-1	
# 	for idx,val in enumerate(teamMatrix):
# 		for i,v in enumerate(val):
# 			if v == 0:
# 				distToTeamCent[idx,i] = np.sqrt( (CentXA[idx] - XpositionMatrix[idx,i])**2 + (CentYA[idx] - YpositionMatrix[idx,i])**2)			
# 			elif v == 1:
# 				distToTeamCent[idx,i] = np.sqrt( (CentXB[idx] - XpositionMatrix[idx,i])**2 + (CentYB[idx] - YpositionMatrix[idx,i])**2)

# 	# Aggregate to team level
# 	SpreadA = np.nanmean(distToTeamCent[:,teamAcols],axis=1)
# 	SpreadB = np.nanmean(distToTeamCent[:,teamBcols],axis=1)

# 	stdSpreadA = np.nanstd(distToTeamCent[:,teamAcols],axis=1)
# 	stdSpreadB = np.nanstd(distToTeamCent[:,teamBcols],axis=1)

# 	# Other ideas:
# 	# (min or avg or max)Distance to closest teammate

# 	# Return in format attributeDict
# 	dataShape = np.shape(XpositionMatrix) # Number of data entries

# 	tmpSpreadA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
# 	tmpSpreadB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
# 	tmpStdSpreadA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
# 	tmpStdSpreadB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
# 	print(SpreadA)
# 	pdb.set_trace()

# 	for idx,val in enumerate(indsMatrix): # indsMatrix are the team cells only
# 		tmpSpreadA[int(val)] = SpreadA[idx]
# 		tmpSpreadB[int(val)] = SpreadB[idx]

# 		tmpStdSpreadA[int(val)] = stdSpreadA[idx]
# 		tmpStdSpreadB[int(val)] = stdSpreadB[idx]

# 	# The data
# 	tmpAtDi2 = {'SpreadA': tmpSpreadA, 'SpreadB': tmpSpreadB, 'stdSpreadA': tmpStdSpreadA,'stdSpreadB': tmpStdSpreadB}
# 	# The labels
# 	tmpSpreadAString = 'Average distance to center of %s (m)' %TeamAstring
# 	tmpStdSpreadAString = 'Standard deviation of distance to center of %s (m)' %TeamAstring
# 	tmpSpreadBString = 'Average distance to center of %s (m)' %TeamBstring
# 	tmpStdSpreadBString = 'Standard deviation of distance to center of %s (m)' %TeamBstring	
# 	tmpAtLa2 = {'SpreadA': tmpSpreadAString, 'SpreadB': tmpSpreadBString, 'stdSpreadA': tmpStdSpreadAString,'stdSpreadB': tmpStdSpreadBString}
# 	return tmpAtDi2,tmpAtLa2

# #####################################################################################

# def teamSurface(indsMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,TeamAstring,TeamBstring):

# 	# Return in format attributeDict
# 	dataShape = np.shape(XpositionMatrix) # Number of data entries

# 	tmpSurfaceA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
# 	tmpsumVerticesA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
# 	tmpShapeRatioA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
# 	tmpWidthA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
# 	tmpLengthA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan

# 	tmpSurfaceB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
# 	tmpsumVerticesB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
# 	tmpShapeRatioB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
# 	tmpWidthB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
# 	tmpLengthB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan

# 	for idx,val in enumerate(indsMatrix):
# 		SurfaceA,sumVerticesA,ShapeRatioA = groupSurface(XpositionMatrix[idx,teamAcols],YpositionMatrix[idx,teamAcols])
# 		SurfaceB,sumVerticesB,ShapeRatioB = groupSurface(XpositionMatrix[idx,teamBcols],YpositionMatrix[idx,teamBcols])		

# 		# Width ************** ASSUMPTION --> field width = X-axis, field length = Y-axis
# 		WidthA = max(XpositionMatrix[idx,teamAcols])-min(XpositionMatrix[idx,teamAcols])
# 		WidthB = max(XpositionMatrix[idx,teamBcols])-min(XpositionMatrix[idx,teamBcols])
# 		# Length
# 		LengthA = max(YpositionMatrix[idx,teamAcols])-min(YpositionMatrix[idx,teamAcols])
# 		LengthB = max(YpositionMatrix[idx,teamBcols])-min(YpositionMatrix[idx,teamBcols])
# 		warn('\nUnverified assumption: field width = X-axis, field length = Y-axis\n')

# 		# Store immediately
# 		tmpSurfaceA[int(val)] = SurfaceA
# 		tmpsumVerticesA[int(val)] = sumVerticesA
# 		tmpShapeRatioA[int(val)] = ShapeRatioA
# 		tmpWidthA[int(val)] = WidthA
# 		tmpLengthA[int(val)] = LengthA

# 		tmpSurfaceB[int(val)] = SurfaceB
# 		tmpsumVerticesB[int(val)] = sumVerticesB
# 		tmpShapeRatioB[int(val)] = ShapeRatioB
# 		tmpWidthB[int(val)] = WidthB
# 		tmpLengthB[int(val)] = LengthB

# 	# Export these new values to temporary dictionary
# 	tmpAtDi3 = {'SurfaceA': tmpSurfaceA, 'SurfaceB': tmpSurfaceB, \
# 	'sumVerticesA': tmpsumVerticesA,'sumVerticesB': tmpsumVerticesB, \
# 	'ShapeRatioA': tmpShapeRatioA,'ShapeRatioB': tmpShapeRatioB, \
# 	'WidthA': tmpWidthA,'WidthB': tmpWidthB, \
# 	'LengthA': tmpLengthA,'LengthB': tmpLengthB }

# 	tmpSurfaceAString = 'Surface area of %s (m^2)' %TeamAstring
# 	tmpSurfaceBString = 'Surface area of %s (m^2)' %TeamBstring
# 	tmpsumVerticesAString = 'Circumference of surface area of %s (m)' %TeamAstring
# 	tmpsumVerticesBString = 'Circumference of surface area of %s (m)' %TeamBstring
# 	tmpShapeRatioAString = 'Uniformity of surface area of %s (1 = uniform, closer to 0 = elongated)' %TeamAstring
# 	tmpShapeRatioBString = 'Uniformity of surface area of %s (1 = uniform, closer to 0 = elongated)' %TeamBstring
# 	tmpWidthAString = 'Distance along X-axis %s (m)' %TeamAstring
# 	tmpWidthBString = 'Distance along X-axis %s (m)' %TeamBstring
# 	tmpLengthAString = 'Distance along Y-axis %s (m)' %TeamAstring
# 	tmpLengthBString = 'Distance along Y-axis %s (m)' %TeamBstring

# 	# Export labels
# 	tmpAtLa3 = {'SurfaceA': tmpSurfaceAString, 'SurfaceB': tmpSurfaceBString, \
# 	'sumVerticesA': tmpsumVerticesAString,'sumVerticesB': tmpsumVerticesBString, \
# 	'ShapeRatioA': tmpShapeRatioAString,'ShapeRatioB': tmpShapeRatioBString, \
# 	'WidthA': tmpWidthAString,'WidthB': tmpWidthBString, \
# 	'LengthA': tmpLengthAString,'LengthB': tmpLengthBString }	
# 	return tmpAtDi3,tmpAtLa3

# #####################################################################################

def groupSurface(X,Y):

	if any(np.isnan(X)):
		warn('\nWARNING: Found a NaN in groupSurface. Normally, these should not occur in the groupSurface function.\nOmitted them, but not sure what the consequence may be for the rest of the pipeline.')
		if not any(np.isnan(Y)):
			warn('\nWARNING: Even stranger, the NaN only existed for the X value.')
		Y = Y[~np.isnan(X)]
		X = X[~np.isnan(X)]


	if any(np.isnan(Y)):
		warn('\nWARNING: Found a NaN in groupSurface. Normally, these should not occur in the groupSurface function.\nOmitted them, but not sure what the consequence may be for the rest of the pipeline.')
		warn('\nWARNING: Even stranger, the NaN only existed for the Y value.')
		X = X[~np.isnan(Y)]
		Y = Y[~np.isnan(Y)]

	if len(X) < 3:
		# Can't compute a surface with only 1 or 2 players
		warn('\nWARNING: Less than two players detected for a single timestamp. No surface measures computed.')
		return np.nan,np.nan,np.nan

	def cart2pol(x, y):
		rho = np.sqrt(x**2 + y**2)
		phi = np.arctan2(y, x)/math.pi
		for idx,val in enumerate(phi):
			if val < 0:
				phi[idx] = val + 2
		return(rho,phi) # in multples of pi

	def pol2cart(rho, phi):
	    x = rho * np.cos(phi)
	    y = rho * np.sin(phi)
	    return(x, y)

	# Using the shoelace formula https://en.wikipedia.org/wiki/Shoelace_formula
	# (which skips the necessity of an arbitrary reference point, unlike Frencken et al., 2011)		
	def PolyArea(x,y):
	    return 0.5*np.abs(np.dot(x,np.roll(y,1))-np.dot(y,np.roll(x,1)))

	def Circumference(VerticesX,VerticesY):
		dVX = [j-i for i, j in zip(VerticesX[:-1], VerticesX[1:])] # Diference in Vertices X
		dVY = [j-i for i, j in zip(VerticesY[:-1], VerticesY[1:])]
		return sum([math.sqrt(i**2+j**2) for i,j in zip(dVX,dVY)])		
	
	def RibRatio(VXa,VXy): # Note that the last coordinates should be of the starting point
		ribDist = []
		for i in range(math.ceil(len(VXa)/2)):
			dX = [abs(VXa[i] - VXa[j+i+1]) for j in range(len(VXa) - i-2)] # -2 because the last value is the same as the first
			dY = [abs(VXy[i] - VXy[j+i+1]) for j in range(len(VXy) - i-2)]
			
			ribDist.extend([math.sqrt(i**2+j**2) for i,j in zip(dX,dY)])
		if min(ribDist) == 0:
			warn('\nWARNING: Unknown issue with ribs. \nOne or multiple vertices have no length.\nProblem with duplicate timestamp? Lack of position data?')
			return 999
		else:
			return max(ribDist) / min(ribDist) 

	# pre-first, change dataframes into np arrays
	# X = X.as_matrix()
	# Y = Y.as_matrix()	
	# X = np.array([-10.717,	4.551,	1.023,	-5.168,	-49.105,	-8.758,	-18.722,	0.427,	0.842])
	# Y = np.array([	-22.45,	-20.663,	10.458,	22.755,	1.325,	7.319,	-11.606,	-0.407,	0.589])
	dataShapeX = np.shape(X) # Number of data entries
	dataShapeY = np.shape(Y)
	if dataShapeX != dataShapeY:
		warn('\nWARNING: different number of data points for X and Y coordinates.\nThis should not be possible...\n ')
	dataShape = dataShapeX
	# print(type(X))
	# print(np.shape(X))
	# print(type(Y))
	# print(np.shape(Y))

	# first, find lowest yvalue (and highest X if equal)
	indStartY = np.where(Y == np.min(Y))

	# ########################### recreate the problem #######################
	# Ytmp = Y#np.append(Y,np.min(Y))
	# Ytmp[Ytmp == np.max(Ytmp)] = np.min(Ytmp)

	# # problem occurs when there is no min Y..	
	# indStartY = np.where(Ytmp == 123456789123456789)
	# # Perhaps only because there are no players at that timepoint?

	# # X[indStartY] = np.max(X)
	# # indStartY = np.array([])
	# ########################### / recreate the problem ######################

	if np.size(indStartY) != 1:
		
		try:
			# If multiple smallest Y, select highest X
			indStartXY = np.where(X[indStartY] == np.max(X[indStartY]))
		except:
			warn('\nWARNING: Problem with finding the starting point to create the polygon that covers the surface.\n')
			print('For <ValueError: zero-size array to reduction operation maximum which has no identity>,')
			print('The solution was that there were nans....')
			print('See also: recreate the problem above.')
			print('0000000000000000000000000000000000000000000000')
			print('dataShapeX = %s' %dataShapeX)
			print('dataShapeY = %s' %dataShapeY)

			print('are there any values in Y:')
			print(Y)
			print('are there any values in X:')
			print(X)
			print('indStartY should contain multiple inds:')
			print(indStartY)

			print('\ndoes this work:')
			print(X[indStartY])
			print('\ndoes this work:')
			print(np.max(X[indStartY]))
			pdb.set_trace()

		indStart = indStartY[0][indStartXY[0]][0]
	else:
		indStart = indStartY[0][0]	

	StartX = X[indStart]
	StartY = Y[indStart]

	indNext =[-1]
	countIteration = 0

	xremaining = X.copy()
	xremaining[indStart] = np.nan

	curInd = indStart

	VerticesX = [StartX]
	VerticesY = [StartY]

	while indNext != indStart:
		countIteration = countIteration + 1

		CurX = X[curInd]
		CurY = Y[curInd]
		# print('CurX = %s' %CurX)

		# then, compute the angles with all players towards an arbitrary point (the centroid)
		# print('x_in = %s' %(xremaining - CurX))
		# print('y_in = %s' %(Y-CurY))
		rho,phi = cart2pol(xremaining - CurX,Y-CurY)
		# rho,phi = cart2pol(CurX - xremaining,CurY-Y)

		# # Predictable Rho
		# x = np.array([0,-10,0,10,0])
		# y = np.array([-10,0,10,0,0])
		# rhos,phis = cart2pol(x,y)
		# bottom = phis[0]
		# left = phis[1]
		# top = phis[2]
		# right = phis[3]
		# rho0 = phis[4]
		# # # Improvised correction after difficulty with finding next point.
		# # # I'm guessing it's because the starting point is different than expected.
		# # # First, correct phi with 1/4*pi
		# # phiAccent = phi + 1
		# # # Then, check if any value below 0, which should be 2*pi higher.
		# # phiAccent[phiAccent<0] = [i + 2*math.pi for i in phiAccent if i < 0]			
		# # phi = phiAccent

		# print('bottom = %s' %bottom)
		# print('right = %s' %right)
		# print('top = %s' %top)
		# print('left = %s' %left)
		# print('rho0 = %s' %rho0)
		# pdb.set_trace()

		# select the smallest angle as the next point
		indNextPhi = np.where(phi == np.nanmin(phi))

		if np.size(indNextPhi) != 1:
			# If multiple smallest phi, select highet rho	
			indNextPhiRho = np.where(rho[indNextPhi] == np.nanmax(rho[indNextPhi]))
			indNext = indNextPhi[0][indNextPhiRho[0]][0]
		else:
			indNext = indNextPhi[0][0]

		NextX = X[indNext]
		NextY = Y[indNext]
		# if countIteration == 5:
		# 	print('---')
		# 	print('phi = %s' %phi)
		# 	print('X = %s' %X)
		# 	print('Y = %s' %Y)
		# 	print('---')
		# 	print('NextPhi = %s' %phi[indNext])
		# 	print('NextX = %s' %NextX)
		# 	print('NextY = %s' %NextY)
		# 	pdb.set_trace()



		if countIteration > dataShape[0]: # use the number of columns as the maximum (= max number of vertices)
			# Stopping criterium reached
			warn('\nDid not finish drawing polygon. Maximum number of expected iterations reached..\n')
			break

		if countIteration == 1: # The start is replaced only at the first time as it is used as the stopping criterium\
			# Simpelere alternative: add != 1 to while statement
			xremaining[indStart] = StartX
		
		# Exclude all points that lie in-between the next and the current point
		# I THINK it's only necessary to do this for the Y-axis
		# Debugging idea: see if the same rule needs to apply on the X-axis?
		statement = (Y > Y[indNext]) & (Y < Y[curInd])
		# print('statement = %s' %statement)
		xremaining[statement] = np.nan
		xremaining[indNext] = np.nan
		curInd = indNext


		VerticesX.append(NextX)
		VerticesY.append(NextY)	

	# print(VerticesX)
	# print(VerticesY)
	# pdb.set_trace()
	Surface = PolyArea(VerticesX,VerticesY)
	sumVertices = Circumference(VerticesX,VerticesY)
	ShapeRatio = RibRatio(VerticesX,VerticesY)

	return Surface,sumVertices,ShapeRatio

def obtainIndices(rawDict,TeamAstring,TeamBstring):
	X = rawDict['X']
	Y = rawDict['Y']
	PlayerID = rawDict['PlayerID']
	TeamID = rawDict['TeamID']
	TsS = rawDict['Ts']
	uniqueTsS,tmp = np.unique(TsS,return_counts=True)
	uniquePlayers = pd.unique(PlayerID)

	indsMatrix = np.ones((len(uniqueTsS),1),dtype='int')*999
	teamMatrix = np.ones((len(uniqueTsS),len(uniquePlayers)),dtype='float64')*999
	XpositionMatrix = np.ones((len(uniqueTsS),len(uniquePlayers)),dtype='float64')*999
	YpositionMatrix = np.ones((len(uniqueTsS),len(uniquePlayers)),dtype='float64')*999	

	teamAcols = []
	teamBcols = []
	ballCols = []
	groupCols = []
	for idx,val in enumerate(TsS):
		row = np.where(val == uniqueTsS)[0]
		col = np.where(PlayerID[idx] == uniquePlayers)[0]
		
		XpositionMatrix[row,col] = X[idx]
		YpositionMatrix[row,col] = Y[idx]		

		if TeamID[idx] == TeamAstring:
			teamMatrix[row,col] = 0
			if not col in teamAcols:
				teamAcols.append(int(col))
		elif TeamID[idx] == TeamBstring:
			teamMatrix[row,col] = 1
			if not col in teamBcols:
				teamBcols.append(int(col))
		elif TeamID[idx] == '' or PlayerID[idx] == 'groupRow':
			# Store indices, as we're computing team values
			indsMatrix[row] = idx

		else:
			if PlayerID[idx] == 'ball':
				ballCols.append(int(col))
			elif np.isnan(TeamID[idx]):
				groupCols.append(int(col))
			else:
				warn('\nDid not recoganize Team ID string: <%s>' %TeamID[idx])	
	
	# NB: Could still export groupCols and ballCols
	return indsMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,uniqueTsS,uniquePlayers,teamMatrix

def ballPossession(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring,skipSpatAgg):
	tDistToBall = time.time()

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	inPossessionString = '0 = not in possession, 1 = in possession'
	attributeLabel.update({'inPossession':inPossessionString}) ##################################### andere afgeleiden nog toevoegen
		
	if skipSpatAgg: # Return eary if spatial aggregation is being skipped
		return attributeDict,attributeLabel

	##### THE DATA #####
	# In this case, the new attribute will be computed based on a group (i.e., team) value
	ballVals = rawDict[rawDict['PlayerID'] == 'ball'].set_index('Ts')
	# Create empty DataFrame to store results, NB: columns need to be assigend beforehand.
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['distToBall','velRelToBall'], dtype = np.float64)
	
	# 1) Compute the X and Y velocities of the ball
	curBall = rawDict[rawDict['PlayerID'] == 'ball']

	# using np's gradient.
	dt = 0.1
	Ball_VX = np.gradient(curBall['X'],dt)
	Ball_VY = np.gradient(curBall['Y'],dt)

	# based on the difference between frames
	dt = curBall['Ts'] - curBall['Ts'].shift(1)
	dX = curBall['X'] - curBall['X'].shift(1)
	dY = curBall['Y'] - curBall['Y'].shift(1)
	# The second method
	Ball_VX2 = dX / dt
	Ball_VY2 = dY / dt

	# Compute the direction of the ball
	# Based on the difference in Y and X position
	ballDir = np.arctan2(dY,dX)
	# Based on the difference in Y and X velocity (method 1)
	ballDir_vel = np.arctan2(Ball_VY,Ball_VX)
	# Based on the difference in Y and X velocity (method 2)
	ballDir_vel2 = np.arctan2(Ball_VY2,Ball_VX2)

	newBallAttributes1 = pd.DataFrame(Ball_VX, index = curBall['Ts'], columns = ['Ball_VX'], dtype = np.float64)
	newBallAttributes2 = pd.DataFrame(Ball_VY, index = curBall['Ts'], columns = ['Ball_VY'], dtype = np.float64)
	# newBallAttributes = pd.DataFrame([Ball_VX,Ball_VY], index = curBall['Ts'].index, columns = ['Ball_VX', 'Ball_VY'], dtype = np.float64)

	ballVals = pd.concat([ballVals,newBallAttributes1,newBallAttributes2], axis = 1)
	print(ballVals)

	# # Plot to verify
	# plt.figure()
	# plt.plot(curBall['Ts'],ballDir_vel)
	# plt.show()
	# pdb.set_trace()

	# 2) compute the distance of each player to the ball
	# and
	# 3) compute the relative X and Y velocities

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
		else: # either a TeamAstring or a TeamBstring

			curPlayer_distToBall = np.sqrt((curPlayerDict['X'] - ballVals['X'])**2 + (curPlayerDict['Y'] - ballVals['Y'])**2)

			dt = 0.1
			curPlayer_VX = np.gradient(curPlayerDict['X'],dt)
			curPlayer_VY = np.gradient(curPlayerDict['Y'],dt)

			curPlayer_VX_asPanda = pd.DataFrame(curPlayer_VX,columns = ['VX'],index = curPlayer['Ts'])
			curPlayer_VY_asPanda = pd.DataFrame(curPlayer_VX,columns = ['VY'],index = curPlayer['Ts'])

			# Compute relative velocity
			curPlayer_velRelToBall = np.sqrt((curPlayer_VX_asPanda['VX'] - ballVals['Ball_VX'])**2 + (curPlayer_VY_asPanda['VY'] - ballVals['Ball_VY'])**2)

		# Put compute values in the right place in the dataFrame
		newAttributes['distToBall'][curPlayer.index] = curPlayer_distToBall[curPlayerDict.index]
		newAttributes['velRelToBall'][curPlayer.index] = curPlayer_velRelToBall[curPlayerDict.index]


		

	# IDEAS:
	# Duration threshold?

	# absolute threshold (<3m?)

	# velocity (same direction?)

	# Prioritization?
	# Gain based on duration (cv. gaze studies)

	# Discrete?
	# Every change of direction of the ball means possession has been had. Take whoever was closest.

		


	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	elapsed = str(round(time.time() - tDistToBall, 2))
	print('*****----- Time elapsed during determining distance to ball.vNorm(): %ss' %elapsed)

	altogether = pd.concat([rawDict,attributeDict], axis=1)
	altogether.to_csv('D:\\KNVB\\test.csv')

	pdb.set_trace()
	return attributeDict,attributeLabel
