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
import countExistingAttributes
import exportCSV

if __name__ == '__main__':
	process(rawDict,TeamAstring,TeamBstring)
	teamCentroid(indsMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols)
	teamSpread(attributeDIct,uniqueTsS,uniquePlayers,teamMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,indsMatrix)
	groupSurface(X,Y)

#####################################################################################

def process(rawDict,TeamAstring,TeamBstring):
	# Per Match
	# Per Team and for both teams

	X = rawDict['Location']['X']
	Y = rawDict['Location']['Y']
	PlayerID = rawDict['Entity']['PlayerID']
	TeamID = rawDict['Entity']['TeamID']
	TsS = rawDict['Time']['TsS']

	uniqueTsS = np.unique(TsS)
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
		# indsMatrix[row,col] = idx
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

	tmpAtDi1,CentXA,CentYA,CentXB,CentYB = teamCentroid(indsMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols)
	tmpAtDi2 = teamSpread(CentXA,CentYA,CentXB,CentYB,uniqueTsS,uniquePlayers,teamMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,indsMatrix)
	tmpAtDi3 = teamSurface(indsMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols)

	attrDictOut = tmpAtDi1
	attrDictOut.update(tmpAtDi2)
	attrDictOut.update(tmpAtDi3)	

	return attrDictOut

#####################################################################################
#####################################################################################

def teamCentroid(indsMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols):
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

	tmpAtDi1 = {'TeamCentXA': tmpXA, 'TeamCentYA': tmpYA, 'TeamCentXB': tmpXB,'TeamCentYB': tmpYB}

	return tmpAtDi1,CentXA,CentYA,CentXB,CentYB # Export dictionary to CSV (after simple temporal aggregation)

#####################################################################################

def teamSpread(CentXA,CentYA,CentXB,CentYB,uniqueTsS,uniquePlayers,teamMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols,indsMatrix):
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

	tmpAtDi2 = {'SpreadA': tmpSpreadA, 'SpreadB': tmpSpreadB, 'stdSpreadA': tmpStdSpreadA,'stdSpreadB': stdSpreadB}

	return tmpAtDi2

#####################################################################################

def teamSurface(indsMatrix,XpositionMatrix,YpositionMatrix,teamAcols,teamBcols):

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

	return tmpAtDi3

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

	# print('Number of iterations required = ',countIteration)
	Surface = PolyArea(VerticesX,VerticesY)
	sumVertices = Circumference(VerticesX,VerticesY)
	ShapeRatio = RibRatio(VerticesX,VerticesY)

	return Surface,sumVertices,ShapeRatio