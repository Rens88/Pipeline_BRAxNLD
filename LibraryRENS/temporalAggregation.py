# 08-12-2017 Rens Meerhoff

import csv
import pdb; #pdb.set_trace()
import numpy as np
from os.path import isfile, join, isdir
from os import listdir, path
from warnings import warn

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
	plotSpeed(rawDict, attributeDict, targetEvents,firstFrameTimeseries,windowTimeseries,stringOut)
	simple(rawData,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring)

def simple(rawData,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring):
	# Per Match
	# Per Team and for both teams (for vNorm, the team aggregate -techinically spatial aggregation - still has to be made)
	vel = [val for idx,val in enumerate(attributeDict['vNorm'])]
	velA = [val for idx,val in enumerate(attributeDict['vNorm']) if rawData['Entity']['TeamID'][idx] == TeamAstring]
	velB = [val for idx,val in enumerate(attributeDict['vNorm']) if rawData['Entity']['TeamID'][idx] == TeamBstring]
	
	# Average
	avgSpeed = np.nanmean(vel)
	avgSpeedA = np.nanmean(velA)
	avgSpeedB = np.nanmean(velB)

	# Std	
	stdSpeed = np.nanstd(vel)
	stdSpeedA = np.nanstd(velA)
	stdSpeedB = np.nanstd(velB)

	# Min	
	minSpeed = np.nanmin(vel)
	minSpeedA = np.nanmin(velA)
	minSpeedB = np.nanmin(velB)

	# Max	
	maxSpeed = np.nanmax(vel)
	maxSpeedA = np.nanmax(velA)
	maxSpeedB = np.nanmax(velB)

	###########################################################
	# Export
	# Averages
	exportDataString.append('avgSpeed')
	exportData.append(avgSpeed)
	exportFullExplanation.append('Average speed (in m/s) of all players per match.')

	exportDataString.append('avgSpeedA')
	exportData.append(avgSpeedA)
	exportFullExplanation.append('Average speed (in m/s) of all players from %s per match.' %TeamAstring)


	exportDataString.append('avgSpeedB')
	exportData.append(avgSpeedB)
	exportFullExplanation.append('Average speed (in m/s) of all players from %s per match.' %TeamBstring)

	# Stds	
	exportDataString.append('stdSpeed')
	exportData.append(stdSpeed)
	exportFullExplanation.append('Standard deviation speed (in m/s) of all players per match.')

	exportDataString.append('stdSpeedA')
	exportData.append(stdSpeedA)
	exportFullExplanation.append('Standard deviation speed (in m/s) of all players from %s per match.' %TeamAstring)

	exportDataString.append('stdSpeedB')
	exportData.append(stdSpeedB)
	exportFullExplanation.append('Standard deviation speed (in m/s) of all players from %s per match.' %TeamBstring)

	# minmax
	exportDataString.append('minSpeed')
	exportData.append(minSpeed)
	exportFullExplanation.append('Minimum speed (in m/s) of all players per match.')

	exportDataString.append('minSpeedA')
	exportData.append(minSpeedA)
	exportFullExplanation.append('Minimum speed (in m/s) of all players from %s per match.' %TeamAstring)

	exportDataString.append('minSpeedB')
	exportData.append(minSpeedB)
	exportFullExplanation.append('Minimum speed (in m/s) of all players from %s per match.' %TeamBstring)

	exportDataString.append('maxSpeed')
	exportData.append(maxSpeed)
	exportFullExplanation.append('Maximum speed (in m/s) of all players per match.')

	exportDataString.append('maxSpeedA')
	exportData.append(maxSpeedA)
	exportFullExplanation.append('Maximum speed (in m/s) of all players from %s per match.' %TeamAstring)

	exportDataString.append('maxSpeedB')
	exportData.append(maxSpeedB)
	exportFullExplanation.append('Maximum speed (in m/s) of all players from %s per match.' %TeamBstring)


	#########################################

	exportDataString.append('avgSpreadA')
	exportData.append(np.nanmean(attributeDict['SpreadA']))
	exportFullExplanation.append('The average spread (= averge player distance to team centroid) of %s per match.' %TeamAstring)

	exportDataString.append('avgSpreadB')
	exportData.append(np.nanmean(attributeDict['SpreadB']))
	exportFullExplanation.append('The average spread (= averge player distance to team centroid) of %s per match.' %TeamBstring)

	exportDataString.append('stdSpreadA')
	exportData.append(np.nanstd(attributeDict['SpreadA']))
	exportFullExplanation.append('The standard deviation of the spread (= averge player distance to team centroid) of %s per match.' %TeamAstring)

	exportDataString.append('stdSpreadB')
	exportData.append(np.nanstd(attributeDict['SpreadB']))
	exportFullExplanation.append('The standard deviation of the spread (= averge player distance to team centroid) of %s per match.' %TeamBstring)


	exportDataString.append('avg_stdSpreadA')
	exportData.append(np.nanmean(attributeDict['stdSpreadA']))
	exportFullExplanation.append('The average uniformity around center (= std player distance to team centroid) of %s per match.' %TeamAstring)

	exportDataString.append('avg_stdSpreadB')
	exportData.append(np.nanmean(attributeDict['stdSpreadB']))
	exportFullExplanation.append('The average uniformity around center (= std player distance to team centroid) of %s per match.' %TeamBstring)

	exportDataString.append('std_stdSpreadA')
	exportData.append(np.nanstd(attributeDict['stdSpreadA']))
	exportFullExplanation.append('The standard deviation of the uniformity around center (= std player distance to team centroid) of %s per match.' %TeamAstring)

	exportDataString.append('std_stdSpreadB')
	exportData.append(np.nanstd(attributeDict['stdSpreadB']))
	exportFullExplanation.append('The standard deviation of the uniformity around center (= std player distance to team centroid) of %s per match.' %TeamBstring)

	
	exportDataString.append('avgWidthA')
	exportData.append(np.nanmean(attributeDict['WidthA']))
	exportFullExplanation.append('The average width (= widest X distance between players) of %s per match.' %TeamAstring)

	exportDataString.append('avgWidthB')
	exportData.append(np.nanmean(attributeDict['WidthB']))
	exportFullExplanation.append('The average width (= widest X distance between players) of %s per match.' %TeamBstring)

	exportDataString.append('stdWidthA')
	exportData.append(np.nanstd(attributeDict['WidthA']))
	exportFullExplanation.append('The standard deviation of the width (= widest X distance between players) of %s per match.' %TeamAstring)

	exportDataString.append('stdWidthB')
	exportData.append(np.nanstd(attributeDict['WidthB']))
	exportFullExplanation.append('The standard deviation of the width (= widest X distance between players) of %s per match.' %TeamBstring)


	exportDataString.append('avgLengthA')
	exportData.append(np.nanmean(attributeDict['LengthA']))
	exportFullExplanation.append('The average length (= widest Y distance between players) of %s per match.' %TeamAstring)

	exportDataString.append('avgLengthB')
	exportData.append(np.nanmean(attributeDict['LengthB']))
	exportFullExplanation.append('The average length (= widest Y distance between players) of %s per match.' %TeamBstring)

	exportDataString.append('stdLengthA')
	exportData.append(np.nanstd(attributeDict['LengthA']))
	exportFullExplanation.append('The standard deviation of the length (= widest Y distance between players) of %s per match.' %TeamAstring)

	exportDataString.append('stdLengthB')
	exportData.append(np.nanstd(attributeDict['LengthB']))
	exportFullExplanation.append('The standard deviation of the length (= widest Y distance between players) of %s per match.' %TeamBstring)
	

	exportDataString.append('avgSurfaceA')
	exportData.append(np.nanmean(attributeDict['SurfaceA']))
	exportFullExplanation.append('The average surface area (in unknown units) of %s per match.' %TeamAstring)

	exportDataString.append('avgSurfaceB')
	exportData.append(np.nanmean(attributeDict['SurfaceB']))
	exportFullExplanation.append('The average surface area (in unknown units) of %s per match.' %TeamBstring)

	exportDataString.append('stdSurfaceA')
	exportData.append(np.nanstd(attributeDict['SurfaceA']))
	exportFullExplanation.append('The standard deviation of the surface area (in unknown units) of %s per match.' %TeamAstring)

	exportDataString.append('stdSurfaceB')
	exportData.append(np.nanstd(attributeDict['SurfaceB']))
	exportFullExplanation.append('The standard deviation of the surface area (in unknown units) of %s per match.' %TeamBstring)


	exportDataString.append('avgsumVerticesA')
	exportData.append(np.nanmean(attributeDict['sumVerticesA']))
	exportFullExplanation.append('The average circumference (in unknown units) of %s per match.' %TeamAstring)

	exportDataString.append('avgsumVerticesB')
	exportData.append(np.nanmean(attributeDict['sumVerticesB']))
	exportFullExplanation.append('The average circumference (in unknown units) of %s per match.' %TeamBstring)

	exportDataString.append('stdsumVerticesA')
	exportData.append(np.nanstd(attributeDict['sumVerticesA']))
	exportFullExplanation.append('The standard deviation of the circumference (in unknown units) of %s per match.' %TeamAstring)

	exportDataString.append('stdsumVerticesB')
	exportData.append(np.nanstd(attributeDict['sumVerticesB']))
	exportFullExplanation.append('The standard deviation of the circumference (in unknown units) of %s per match.' %TeamBstring)


	exportDataString.append('avgShapeRatioA')
	exportData.append(np.nanmean(attributeDict['ShapeRatioA']))
	exportFullExplanation.append('The average shape ratio (longest rib / shortest rib) (values < 1 should not be possible) (the closer to 1, the more uniformly spread out) of %s per match (NB: this is a measure of spread and should correlate with stdSpreadA).' %TeamAstring)

	exportDataString.append('avgShapeRatioB')
	exportData.append(np.nanmean(attributeDict['ShapeRatioB']))
	exportFullExplanation.append('The average shape ratio (longest rib / shortest rib) (values < 1 should not be possible) (the closer to 1, the more uniformly spread out) of %s per match (NB: this is a measure of spread and should correlate with stdSpreadA).' %TeamBstring)

	exportDataString.append('stdShapeRatioA')
	exportData.append(np.nanstd(attributeDict['ShapeRatioA']))
	exportFullExplanation.append('The standard deviation of the shape ratio (longest rib / shortest rib) (values < 1 should not be possible) (the closer to 1, the more uniformly spread out) of %s per match (NB: this is a measure of spread and should correlate with stdSpreadA).' %TeamAstring)

	exportDataString.append('stdShapeRatioB')
	exportData.append(np.nanstd(attributeDict['ShapeRatioB']))
	exportFullExplanation.append('The standard deviation of the shape ratio (longest rib / shortest rib) (values < 1 should not be possible) (the closer to 1, the more uniformly spread out) of %s per match (NB: this is a measure of spread and should correlate with stdSpreadA).' %TeamBstring)

	# Unused:
	# attributeDict['Possession/Turnover ']	attributeDict['Pass']	attributeDict['distFrame']	attributeDict['Goal ']	attributeDict['currentPossession'] attributeDict['TeamCentXA']	attributeDict['TeamCentYA']	attributeDict['TeamCentXB']	attributeDict['TeamCentYB']

	return 	exportData,exportDataString,exportFullExplanation

def plotSpeed(rawDict, attributeDict, targetEvents,tmpFigFolder,firstFrameTimeseries,windowTimeseries,stringOut):

	inds = individualAttributes.PlayerInds(rawDict,firstFrameTimeseries,windowTimeseries)
	xstring = 'Time (s)'
	ystring = 'Speed (m/s)'
	if not firstFrameTimeseries == None:
		tmin = str(firstFrameTimeseries)
	else:
		firstFrameTimeseries = 0
		tmin = 'start'
	if not windowTimeseries == None:
		tmax = str(firstFrameTimeseries+windowTimeseries)
	else:
		tmax = 'End'

	plotTimeseries.PerPlayer(tmin,tmax,inds,rawDict['Time']['TsS'],attributeDict['vNorm'],xstring,ystring,tmpFigFolder,stringOut)

	return rawDict, attributeDict, targetEvents