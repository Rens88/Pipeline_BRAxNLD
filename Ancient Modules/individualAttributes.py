# 09-02-2018 Rens Meerhoff
# Code has now been merged into spatialAggregation.py

# 30-11-2017 Rens Meerhoff
# Function to compute INDIVIDUAL timeseries attributes of a range of players in the same dataset.
# Data should be organized with all players under each other, corresponding to their X and Y positions and Timestamp (in seconds) (TsS).
# frameRateData should be given to make sure units are /second.
#
# yDist ==> 
# PlayerInds => indices corresponding to player and window
# vNorm => normalized velocity

import pdb; #pdb.set_trace()
from warnings import warn
import numpy as np
from os.path import isfile, join, isdir
from os import listdir
import CSVexcerpt


if __name__ == '__main__':
	# Compute normalized velocity
	distToGoal(rawDict) # NOT FINISHED
	PlayerInds(rawDict,firstFrameTimeseries,windowTimeseries)	
	TeamInds(rawDict,firstFrameTimeseries,windowTimeseries)
	vNorm(rawDict, frameRateData)
	
	# Nonlinear pedagogy only:
	# Every new Run (Nonlinear pedagogy data only) has a jump in time (and position), for which velocity is set to 0.
	correctVNorm(rawDict,attributeDict)

def distToGoal(rawDict):
	centerGoalNeg = [-50,0.1]
	centerGoalPos = [50,0.1]
	# Choose side based on average distance of ?
	
###########################
	## Continue with commented code by changing dX to dXtoGoalNeg (and pos)
###########################

	# PlayerID = rawDict['PlayerID']
	# TsS = rawDict['TsS']
	# X = rawDict['X']
	# Y = rawDict['Y']

	# curPlayer = []
	# dX = []
	# dY = []
	# dTarray = []#np.array([])
	# firstFramePlayers = []
	# for idx, val in enumerate(PlayerID):
	# 	if val == '':
	# 		# Team level idx
	# 		dX.append(np.nan) # TO DO: ?? replace these with the team averages ??
	# 		dY.append(np.nan)
	# 		dTarray.append(np.nan)
	# 	elif val == curPlayer:
	# 		# Still the same player
	# 		# --> Continue
	# 		if firstFramePlayers[-1] == idx-1: # This should mean that it's the second frame of this player
	# 			dTarray[idx-1] = TsS[idx] - TsS[idx-1]
	# 		dX.append(X[idx] - X[idx-1])
	# 		dY.append(Y[idx] - Y[idx-1])
	# 		dTarray.append(TsS[idx] - TsS[idx-1])		
			
	# 		if not (TsS[idx] - dTarray[-1]) == prevTime:
	# 			warn('\nPANICK, time not consequetive\n')
	# 			break
	# 		prevTime = TsS[idx]
	# 	else:
	# 		# FirstFrame of next player
	# 		# --> reset
	# 		firstFramePlayers.append(idx) # not necessary for computing speed, but possibly useful later
	# 		curPlayer = val
	# 		dX.append(0)
	# 		dY.append(0)
	# 		dTarray.append(0)# first just make it zero, in case only one frame is available
	# 		prevTime = TsS[idx]


	
	output = {distToGoal:{'ownGoal':ownGoal,'oppGoal':oppGoal,'goalA':goalA,'goalB':goalB}}
	return output

def PlayerInds(rawDict,firstFrameTimeseries,windowTimeseries):
	PlayerID = rawDict['Entity']['PlayerID']
	TsMS = rawDict['Time']['TsMS']	

	# inds: List of lists with 1) PlayerID, 2) first frame of that player, 3) last frame of that player
	# Assumptions: data sorted by player
	IDs = list(set(PlayerID))
	IDs.remove('')

	prevVal = []
	inds = np.ones((len(IDs),3),dtype='int')*-1
	countX = 0

	for idx,val in enumerate(PlayerID):
		if countX == 0 and val != '': # first player
			inds[countX,0] = PlayerID[idx] # ID
			inds[countX,1] = idx # Start ind
			prevVal = val
			countX = countX + 1
		elif not prevVal == val and val != '': # New ID
			inds[countX-1,2] = idx-1 # End of previous
			if countX == len(IDs):
				warn('\n\nThis should lead to an error: \nData input should be ordered by athlete. Either \n1) add <sort> in function, or \n2) sort your dataset before loading it in the pipeline]\n\n')
			inds[countX,0] = PlayerID[idx] #
			inds[countX,1] = idx
			prevVal = val
			countX = countX + 1
		elif countX == len(IDs) and val != '':# Last one
			# print('and now im here2')
			# print('val = ',val)

			inds[countX-1,2] = idx # End of previous
	if inds[-1][-1] == -1:
		warn('\n\n Not all inds were assigned. This may lead to an error.')
		# Only one known scenario could cause this: 
		# If the last player only has one frame and after that player there is only one team variable.

	# BASED on the assumptions that TO DO: BUILD SAFETYWARNING HERE
	# 1) the data is ordered by time
	# 2) each player has the same number of frames
	# timeSeriesInd = np.ones((len(IDs),len(framesTimeseries)),dtype='int')*-1

	tmp = TsMS[list(range(inds[0][1],inds[0][2]+1,1))] # TsS --> Time in seconds
	# NB: the '+1' is added to make the indices until and including the last frame

	startCorrection = []
	if not windowTimeseries == None:
		if firstFrameTimeseries	< tmp [0]:
			warn('\n\nSelected timeframe outside data time.\n')
		for idx,val in enumerate(tmp):
			if val >= firstFrameTimeseries and startCorrection == []:
				startCorrection = idx
			if val >= firstFrameTimeseries + windowTimeseries:
				endCorrection = idx
				break

	else:
		endCorrection = 0
		startCorrection = 0
	for i in range(len(inds)):
		inds[i][2] = inds[i][2] + endCorrection # NB: order is important, because I'm referring to the variable that Im changing within this loop
		inds[i][1] = inds[i][1] + startCorrection
	return inds

def TeamInds(rawDict,firstFrameTimeseries,windowTimeseries): ######## DOESNT WORK AT THE MOMENT
	PlayerID = rawDict['Entity']['PlayerID']
	TsMS = rawDict['Time']['TsMS']	

	# inds: List of lists with 1) PlayerID, 2) first frame of that player, 3) last frame of that player
	# Assumptions: data sorted by player
	IDs = ''

	prevVal = []
	inds = np.ones((len(IDs),3),dtype='int')*-1
	countX = 0

	for idx,val in enumerate(PlayerID):
		if countX == 0 and val != '': # first player
			inds[countX,0] = PlayerID[idx] # ID
			inds[countX,1] = idx # Start ind
			prevVal = val
			countX = countX + 1
		elif not prevVal == val and val != '': # New ID
			inds[countX-1,2] = idx-1 # End of previous
			if countX == len(IDs):
				warn('\n\nThis should lead to an error: \nData input should be ordered by athlete. Either \n1) add <sort> in function, or \n2) sort your dataset before loading it in the pipeline]\n\n')
			inds[countX,0] = PlayerID[idx] #
			inds[countX,1] = idx
			prevVal = val
			countX = countX + 1
		elif countX == len(IDs) and val != '':# Last one
			# print('and now im here2')
			# print('val = ',val)

			inds[countX-1,2] = idx # End of previous
	if inds[-1][-1] == -1:
		warn('\n\n Not all inds were assigned. This may lead to an error.')
		# Only one known scenario could cause this: 
		# If the last player only has one frame and after that player there is only one team variable.

	# BASED on the assumptions that TO DO: BUILD SAFETYWARNING HERE
	# 1) the data is ordered by time
	# 2) each player has the same number of frames
	# timeSeriesInd = np.ones((len(IDs),len(framesTimeseries)),dtype='int')*-1

	tmp = TsMS[list(range(inds[0][1],inds[0][2]+1,1))] # TsS --> Time in seconds
	# NB: the '+1' is added to make the indices until and including the last frame

	startCorrection = []
	if not windowTimeseries == None:
		if firstFrameTimeseries	< tmp [0]:
			warn('\n\nSelected timeframe outside data time.\n')
		for idx,val in enumerate(tmp):
			if val >= firstFrameTimeseries and startCorrection == []:
				startCorrection = idx
			if val >= firstFrameTimeseries + windowTimeseries:
				endCorrection = idx
				break

	else:
		endCorrection = 0
		startCorrection = 0
	for i in range(len(inds)):
		inds[i][2] = inds[i][2] + endCorrection # NB: order is important, because I'm referring to the variable that Im changing within this loop
		inds[i][1] = inds[i][1] + startCorrection
	return inds	

############################################################
############################################################
def correctVNorm(rawDict,attributeDict):
	runs = np.array([i for i,val in enumerate(attributeDict['Run']) if val  != '' ])

	if runs.size == 0:
		warn('\n!!!!\nExisting attributes seem to be missing.\nCouldnt find runs to normalize velocity.\nVelocity not normalized.')
		return attributeDict
	runTimes = rawDict['Time']['TsS'][runs]

	for val in runTimes: # for every run time, make vNorm 0
		for i,val2 in enumerate(rawDict['Time']['TsS']):
			if val2 == val:
				attributeDict['vNorm'][i] = 0

	return attributeDict


def vNorm(rawDict):
	PlayerID = rawDict['Entity']['PlayerID']
	TsS = rawDict['Time']['TsS']	

	X = rawDict['Location']['X']
	Y = rawDict['Location']['Y']

	curPlayer = []
	dX = []
	dY = []
	dTarray = []#np.array([])
	firstFramePlayers = []
	for idx, val in enumerate(PlayerID):
		if val == '':
			# Team level idx
			dX.append(np.nan) # TO DO: ?? replace these with the team averages ??
			dY.append(np.nan)
			dTarray.append(np.nan)
		elif val == curPlayer:
			# Still the same player
			# --> Continue
			if firstFramePlayers[-1] == idx-1: # This should mean that it's the second frame of this player
				dTarray[idx-1] = TsS[idx] - TsS[idx-1]
			dX.append(X[idx] - X[idx-1])
			dY.append(Y[idx] - Y[idx-1])
			dTarray.append(TsS[idx] - TsS[idx-1])		
			
			if not (TsS[idx] - dTarray[-1]) == prevTime:
				warn('\nPANICK, time not conseCutive\n')
				break
			prevTime = TsS[idx]
		else:
			# FirstFrame of next player
			# --> reset
			firstFramePlayers.append(idx) # not necessary for computing speed, but possibly useful later
			curPlayer = val
			dX.append(0)
			dY.append(0)
			dTarray.append(0)# first just make it zero, in case only one frame is available
			prevTime = TsS[idx]

	# Covered distance on x- and y- axis
	dX = np.array(dX)
	dY = np.array(dY)

	dTarray = np.array(dTarray)
	# dt_fromUserInput = 1 / frameRateData
	# dt = dt_fromUserInput
	# if not np.round(np.nanmean(dTarray),decimals = 4) == np.round(dt_fromUserInput,decimals = 4):
	# 	warn('\nTime in data does not correspond with user time input.\n\
	# 		Analysis continued with data-based dt\n')
	# 	dt = dTarray

	distFrame = np.sqrt(dX**2 + dY**2) # distance covered per frame
	vNorm = distFrame / dTarray[0][0] #*100000
	#print('WARNING: Arbitrary distance unit used.')
	# IDEA: Could add distSummed

	# if 'Run' in attributeDict.keys():
	# 	runs = np.array([i for i,val in enumerate(attributeDict['Run']) if val  != '' ])
	# 	runTimes = rawDict['Time']['TsS'][runs]

	# 	for val in runTimes: # for every run time, make vNorm 0
	# 		for i,val2 in enumerate(rawDict['Time']['TsS']):
	# 			if val2 == val:
	# 				attributeDict['vNorm'][i] = 0

	output = {'vNorm':vNorm,'distFrame':distFrame}
	return output