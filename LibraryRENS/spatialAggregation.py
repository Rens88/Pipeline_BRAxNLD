# 08-12-2017 Rens Meerhoff

import csv
import pdb; #pdb.set_trace()
import numpy as np
from os.path import isfile, join, isdir
from os import listdir, path, startfile
from warnings import warn
import math
# From my own library:
import plotSnapshot
import CSVexcerpt
import CSVimportAsColumns
import identifyDuplHeader
import LoadOrCreateCSVexcerpt
import individualAttributes
import plotTimeseries
import dataToDict
import dataToDict2
import safetyWarning
import exportCSV

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

def process(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	# Per Match (i.e., file)
	# Per Team and for both teams
	indsMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,uniqueTsS,uniquePlayers,teamMatrix = \
	obtainIndices(rawDict,TeamAstring,TeamBstring) # NB: These indices could be useful for different purposes as well

	tmpAtDi1,tmpAtLa1,CentXA,CentYA,CentXB,CentYB = teamCentroid(indsMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,TeamAstring,TeamBstring)
	attributeDict.update(tmpAtDi1)
	attributeLabel.update(tmpAtLa1)

	tmpAtDi2,tmpAtLa2 = teamSpread(CentXA,CentYA,CentXB,CentYB,uniqueTsS,uniquePlayers,teamMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,indsMatrix,TeamAstring,TeamBstring)
	attributeDict.update(tmpAtDi2)
	attributeLabel.update(tmpAtLa2)

	tmpAtDi3,tmpAtLa3 = teamSurface(indsMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,TeamAstring,TeamBstring)
	attributeDict.update(tmpAtDi3)
	attributeLabel.update(tmpAtLa3)

	tmpAtDi4,tmpAtLa4 = vNorm(rawDict)
	attributeDict.update(tmpAtDi4)
	attributeLabel.update(tmpAtLa4)

	# Nonlinear pedagogy only:
	# Every new Run (Nonlinear pedagogy data only) has a jump in time (and position), for which velocity is set to 0.
	tmpAtDi5 = correctVNorm(rawDict,attributeDict) # Correction only, doesn't need new label.
	attributeDict.update(tmpAtDi5)

	return attributeDict, attributeLabel

############################################################
############################################################

def correctVNorm(rawDict,attributeDict):
	if not 'Run' in attributeDict.keys(): # only normalize if 'runs' exists
		return {'vNorm':attributeDict['vNorm']}
	runs = np.array([i for i,val in enumerate(attributeDict['Run']) if val  != '' ])

	if runs.size == 0:
		warn('\n!!!!\nExisting attributes seem to be missing.\nCouldnt find runs to normalize velocity.\nVelocity not normalized.')
		return {'vNorm':attributeDict['vNorm']}
	runTimes = rawDict['Time']['TsS'][runs]
	# output = attributeDict['vNorm']
	output = {'vNorm':attributeDict['vNorm']}

	for val in runTimes: # for every run time, make vNorm 0
		for i,val2 in enumerate(rawDict['Time']['TsS']):
			if val2 == val:
				output['vNorm'][i] = 0

	return output

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
				warn('\nPANIC, time not consecutive\n')
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

	distFrame = np.sqrt(dX**2 + dY**2) # distance covered per frame
	vNorm = distFrame / dTarray[0][0] #*100000
	# IDEA: Could add distSummed

	output = {'vNorm':vNorm,'distFrame':distFrame}
	labels = {'vNorm':'Speed (m/s)','distFrame':'Distance covered (m)'}
	return output,labels

	#####################################################################################
#####################################################################################

def teamCentroid(indsMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,TeamAstring,TeamBstring):
	## IDEA: I could add the player's distance to the centroid here
	
	# Compute the centroids
	CentXA = np.nanmean(XpositionMatrix[:,teamAcols],axis=1)
	CentXB = np.nanmean(XpositionMatrix[:,teamBcols],axis=1)
	CentYA = np.nanmean(YpositionMatrix[:,teamAcols],axis=1)
	CentYB = np.nanmean(YpositionMatrix[:,teamBcols],axis=1)

	# Return in format attributeDict
	dataShape = np.shape(XpositionMatrix) # Number of data entries

	tmpXA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
	tmpYA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
	tmpXB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
	tmpYB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan

	for idx,val in enumerate(indsMatrix): # indsMatrix are the team cells only
		tmpXA[int(val)] = CentXA[idx]
		tmpYA[int(val)] = CentXB[idx]
		tmpXB[int(val)] = CentXA[idx]
		tmpYB[int(val)] = CentYA[idx]
	# the Data
	tmpAtDi1 = {'TeamCentXA': tmpXA, 'TeamCentYA': tmpYA, 'TeamCentXB': tmpXB,'TeamCentYB': tmpYB}
	# the Strings	
	tmpXAString = 'X-position of %s (m)' %TeamAstring
	tmpYAString = 'Y-position of %s (m)' %TeamAstring
	tmpXBString = 'X-position of %s (m)' %TeamBstring
	tmpYBString = 'Y-position of %s (m)' %TeamBstring			
	tmpAtLa1 = {'TeamCentXA': tmpXAString, 'TeamCentYA': tmpYAString, 'TeamCentXB': tmpXBString,'TeamCentYB': tmpYBString}
	return tmpAtDi1,tmpAtLa1,CentXA,CentYA,CentXB,CentYB # Export dictionary to CSV (after simple temporal aggregation)

#####################################################################################

def teamSpread(CentXA,CentYA,CentXB,CentYB,uniqueTsS,uniquePlayers,teamMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,indsMatrix,TeamAstring,TeamBstring):
	# Spread
	# (average) Distance of each player to center.
	# Dist to centre:
	distToTeamCent = np.ones((len(uniqueTsS),len(uniquePlayers)),dtype='float64')*-1	
	for idx,val in enumerate(teamMatrix):
		for i,v in enumerate(val):
			if v == 0:
				distToTeamCent[idx,i] = np.sqrt( (CentXA[idx] - XpositionMatrix[idx,i])**2 + (CentYA[idx] - YpositionMatrix[idx,i])**2)			
			elif v == 1:
				distToTeamCent[idx,i] = np.sqrt( (CentXB[idx] - XpositionMatrix[idx,i])**2 + (CentYB[idx] - YpositionMatrix[idx,i])**2)

	# Aggregate to team level
	SpreadA = np.nanmean(distToTeamCent[:,teamAcols],axis=1)
	SpreadB = np.nanmean(distToTeamCent[:,teamBcols],axis=1)

	stdSpreadA = np.nanstd(distToTeamCent[:,teamAcols],axis=1)
	stdSpreadB = np.nanstd(distToTeamCent[:,teamBcols],axis=1)

	# Other ideas:
	# (min or avg or max)Distance to closest teammate

	# Return in format attributeDict
	dataShape = np.shape(XpositionMatrix) # Number of data entries

	tmpSpreadA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
	tmpSpreadB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
	tmpStdSpreadA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
	tmpStdSpreadB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan

	for idx,val in enumerate(indsMatrix): # indsMatrix are the team cells only
		tmpSpreadA[int(val)] = SpreadA[idx]
		tmpSpreadB[int(val)] = SpreadB[idx]

		tmpStdSpreadA[int(val)] = stdSpreadA[idx]
		tmpStdSpreadB[int(val)] = stdSpreadB[idx]

	# The data
	tmpAtDi2 = {'SpreadA': tmpSpreadA, 'SpreadB': tmpSpreadB, 'stdSpreadA': tmpStdSpreadA,'stdSpreadB': tmpStdSpreadB}
	# The labels
	tmpSpreadAString = 'Average distance to center of %s (m)' %TeamAstring
	tmpStdSpreadAString = 'Standard deviation of distance to center of %s (m)' %TeamAstring
	tmpSpreadBString = 'Average distance to center of %s (m)' %TeamBstring
	tmpStdSpreadBString = 'Standard deviation of distance to center of %s (m)' %TeamBstring	
	tmpAtLa2 = {'SpreadA': tmpSpreadAString, 'SpreadB': tmpSpreadBString, 'stdSpreadA': tmpStdSpreadAString,'stdSpreadB': tmpStdSpreadBString}
	return tmpAtDi2,tmpAtLa2

#####################################################################################

def teamSurface(indsMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,TeamAstring,TeamBstring):

	# Return in format attributeDict
	dataShape = np.shape(XpositionMatrix) # Number of data entries

	tmpSurfaceA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
	tmpsumVerticesA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
	tmpShapeRatioA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
	tmpWidthA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
	tmpLengthA = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan

	tmpSurfaceB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
	tmpsumVerticesB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
	tmpShapeRatioB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
	tmpWidthB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan
	tmpLengthB = np.zeros((dataShape[1]*dataShape[0],1),dtype='float64')* np.nan

	for idx,val in enumerate(indsMatrix):
		SurfaceA,sumVerticesA,ShapeRatioA = groupSurface(XpositionMatrix[idx,teamAcols],YpositionMatrix[idx,teamAcols])
		SurfaceB,sumVerticesB,ShapeRatioB = groupSurface(XpositionMatrix[idx,teamBcols],YpositionMatrix[idx,teamBcols])		

		# Width ************** ASSUMPTION --> field width = X-axis, field length = Y-axis
		WidthA = max(XpositionMatrix[idx,teamAcols])-min(XpositionMatrix[idx,teamAcols])
		WidthB = max(XpositionMatrix[idx,teamBcols])-min(XpositionMatrix[idx,teamBcols])
		# Length
		LengthA = max(YpositionMatrix[idx,teamAcols])-min(YpositionMatrix[idx,teamAcols])
		LengthB = max(YpositionMatrix[idx,teamBcols])-min(YpositionMatrix[idx,teamBcols])
		warn('\nUnverified assumption: field width = X-axis, field length = Y-axis\n')

		# Store immediately
		tmpSurfaceA[int(val)] = SurfaceA
		tmpsumVerticesA[int(val)] = sumVerticesA
		tmpShapeRatioA[int(val)] = ShapeRatioA
		tmpWidthA[int(val)] = WidthA
		tmpLengthA[int(val)] = LengthA

		tmpSurfaceB[int(val)] = SurfaceB
		tmpsumVerticesB[int(val)] = sumVerticesB
		tmpShapeRatioB[int(val)] = ShapeRatioB
		tmpWidthB[int(val)] = WidthB
		tmpLengthB[int(val)] = LengthB

	# Export these new values to temporary dictionary
	tmpAtDi3 = {'SurfaceA': tmpSurfaceA, 'SurfaceB': tmpSurfaceB, \
	'sumVerticesA': tmpsumVerticesA,'sumVerticesB': tmpsumVerticesB, \
	'ShapeRatioA': tmpShapeRatioA,'ShapeRatioB': tmpShapeRatioB, \
	'WidthA': tmpWidthA,'WidthB': tmpWidthB, \
	'LengthA': tmpLengthA,'LengthB': tmpLengthB }

	tmpSurfaceAString = 'Surface area of %s (m^2)' %TeamAstring
	tmpSurfaceBString = 'Surface area of %s (m^2)' %TeamBstring
	tmpsumVerticesAString = 'Circumference of surface area of %s (m)' %TeamAstring
	tmpsumVerticesBString = 'Circumference of surface area of %s (m)' %TeamBstring
	tmpShapeRatioAString = 'Uniformity of surface area of %s (1 = uniform, closer to 0 = elongated)' %TeamAstring
	tmpShapeRatioBString = 'Uniformity of surface area of %s (1 = uniform, closer to 0 = elongated)' %TeamBstring
	tmpWidthAString = 'Distance along X-axis %s (m)' %TeamAstring
	tmpWidthBString = 'Distance along X-axis %s (m)' %TeamBstring
	tmpLengthAString = 'Distance along Y-axis %s (m)' %TeamAstring
	tmpLengthBString = 'Distance along Y-axis %s (m)' %TeamBstring

	# Export labels
	tmpAtLa3 = {'SurfaceA': tmpSurfaceAString, 'SurfaceB': tmpSurfaceBString, \
	'sumVerticesA': tmpsumVerticesAString,'sumVerticesB': tmpsumVerticesBString, \
	'ShapeRatioA': tmpShapeRatioAString,'ShapeRatioB': tmpShapeRatioBString, \
	'WidthA': tmpWidthAString,'WidthB': tmpWidthBString, \
	'LengthA': tmpLengthAString,'LengthB': tmpLengthBString }	
	return tmpAtDi3,tmpAtLa3

#####################################################################################

def groupSurface(X,Y):
	dataShape = np.shape(X) # Number of data entries

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

	# first, find lowest yvalue (and highest X if equal)
	indStartY = np.where(Y == np.min(Y))

	if np.size(indStartY) != 1:
		# If multiple smallest Y, select highest X
		indStartXY = np.where(X[indStartY] == np.max(X[indStartY]))
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
		# then, compute the angles with all players towards an arbitrary point (the centroid)
		rho,phi = cart2pol(xremaining - CurX,Y-CurY)

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

		if countIteration > dataShape[0]: # use the number of columns as the maximum (= max number of vertices)
			# Stopping criterium reached
			warn('\nDid not finish drawing polygon. Maximum number of expected iterations reached..\n')
			break

		if countIteration == 1: # The start is replaced only at the first time as it is used as the stopping criterium\
			# Simpelere alternative: add != 1 to while statement
			xremaining[indStart] = StartX
		
		xremaining[indNext] = np.nan
		curInd = indNext

		VerticesX.append(NextX)
		VerticesY.append(NextY)	

	Surface = PolyArea(VerticesX,VerticesY)
	sumVertices = Circumference(VerticesX,VerticesY)
	ShapeRatio = RibRatio(VerticesX,VerticesY)

	return Surface,sumVertices,ShapeRatio

def obtainIndices(rawDict,TeamAstring,TeamBstring):
	X = rawDict['Location']['X']
	Y = rawDict['Location']['Y']
	PlayerID = rawDict['Entity']['PlayerID']
	TeamID = rawDict['Entity']['TeamID']
	TsS = rawDict['Time']['TsS']
	uniqueTsS,tmp = np.unique(TsS,return_counts=True)
	uniquePlayers = np.unique(PlayerID)

	indsMatrix = np.ones((len(uniqueTsS),1),dtype='int')*999
	teamMatrix = np.ones((len(uniqueTsS),len(uniquePlayers)),dtype='float64')*999
	XpositionMatrix = np.ones((len(uniqueTsS),len(uniquePlayers)),dtype='float64')*999
	YpositionMatrix = np.ones((len(uniqueTsS),len(uniquePlayers)),dtype='float64')*999	

	teamAcols = []
	teamBcols = []
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
		elif TeamID[idx] == '':
			# Store indices, as we're computing team values
			indsMatrix[row] = idx
		else:
			warn('\nDid not recoganize Team ID string: <%s>' %TeamID[idx])	

	return indsMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,uniqueTsS,uniquePlayers,teamMatrix

