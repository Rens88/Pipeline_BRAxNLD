# 08-12-2017 Rens Meerhoff
# NB: From the raw data, this script should only access the always available Time, Entity, and Location.
# The script should not use the attributeDict directly, as this may differ depending on the data source.
# Instead, use targetEvents in the main process file where you can include the characteristics of the event either
# automatically, or manually (and always make the same format)

import csv
import pdb; #pdb.set_trace()
import numpy as np
from os.path import isfile, join, isdir
from os import listdir, path
from warnings import warn

# From my own library:
import plotSnapshot
# import CSVexcerpt
# import CSVimportAsColumns
# import identifyDuplHeader
# import LoadOrCreateCSVexcerpt
# import individualAttributes
import plotTimeseries
# import dataToDict
# import dataToDict2
import safetyWarning
# import exportCSV
# import countEvents2

if __name__ == '__main__':
	# First try, should be removed later:
	simple(rawData,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring)

	process(targetEvents,aggregateLevel,rawDict,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring)
	# Aggregates a range of pre-defined features over a specific window:
	specific(rowswithinrange,aggregateString,rawDict,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring)	

def process(targetEvents,aggregateLevel,rawDict,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring):
	if len(targetEvents[aggregateLevel[0]]) == 0:
		warn('\nWARNING: No targetevents detected. \nCouldnt aggregate temporally. \nNo Data exported.\n')
		return exportData,exportDataString,exportFullExplanation
	exportMatrix = []
	exportDataString.append('temporalAggregate')
	exportFullExplanation.append('Level of temporal aggregation, based on <<%s>> event and counted chronologically' %aggregateLevel[0])
	## FYI:
	# aggregateLevel[0] --> event ID
	# aggregateLevel[1] --> window (s)
	# aggregateLevel[2] --> lag (s)
	specialCase = False # when there's only one event (which is annoying when accessing it as a list instead of a list of lists)
	for idx,currentEvent in enumerate(targetEvents[aggregateLevel[0]]):
		if type(targetEvents[aggregateLevel[0]]) != list:
			# A special case: there was only one event identified, which means that <enumerate> now 
			# goes through the contents of that specific event, rather than iterating over the events.
			# Improvised solution is to overwrite currentEvent and subsequently terminate early.
			currentEvent = targetEvents[aggregateLevel[0]]
			specialCase = True
		if aggregateLevel[0] == 'Possession' or aggregateLevel[0] == 'Full':
			
			tStart = currentEvent[0]
			tEnd = currentEvent[1]
		else:
			tEnd = currentEvent[0] - aggregateLevel[2]
			tStart = tEnd - aggregateLevel[1]
			if aggregateLevel[0] != 'Possession' or aggregateLevel[0] != 'Full':
				warn('\nCode not yet adjusted to have an unspecified period for any other event than <Possession> and <Full>.\nMay lead to an error.\n')

		# Ideas:
		# 	- in 'currentEvent', use value with teamstring to double check if the right time was selected
		#	- negative lags could be used to indicate time after the event (not sure why that's useful though)
		#	- check if overlap with the next occurrence of the event, this needs to be signalled and possibly banned
		#	- Make generic / call-able from for example plotTimeseries.py
		#	- make it possible to have possession analyzed as window / lag as well (now it's automatically analyzed as a complete possession)
		# Determine the rows corresponding to the current event.
		if tEnd == None or tStart == None: # Check if both end and start are allocated
			warn('\nEvent %d skipped because tStart = <<%s>> and tEnd = <<%s>>.\n' %(idx,tStart,tEnd))
			continue
		TsS = rawDict['Time']['TsS']
		rowswithinrange = [idx2 for idx2,i in enumerate(TsS) if i >= tStart and i <= tEnd]
		tmp = [rawDict['Entity']['TeamID'][i] for i in rowswithinrange]
		rowswithinrangePlayers = [rowswithinrange[idx] for idx,val in enumerate(tmp) if val != '']
		rowswithinrangeTeam = [rowswithinrange[idx] for idx,val in enumerate(tmp) if val == '']		
		if round(tStart,2) <= round(tEnd,2):
			window = (tStart,tEnd)
		else:
			window = (tEnd,tStart)
			warn('\nSTRANGE: tStart <%s> was bigger than tEnd <%s>.\nSwapped them to determine window' %(tStart,tEnd))
	
		exportCurrentData = exportData.copy()
		overallString = exportDataString.copy()
		overallExplanation = exportFullExplanation.copy()
		
		# Create the output string that identifies the current event
		aggregateString = '%03d_%s' %(idx,aggregateLevel[0])		
		exportCurrentData.append(aggregateString) # NB: String and explanation are defined before the for loop
		## Count exsiting events (goals, possession and passes)
		# Ideas:
		# - Should include a code for identifying whether information is available.
		# - Should only rely on events as made available through targetEvents (rather than obtain it from attributeDict)

		exportCurrentData,overallString,overallExplanation =\
		countEvents2.goals(window,aggregateLevel[0],targetEvents,TeamAstring,TeamBstring,exportCurrentData,overallString,overallExplanation)
		
		exportCurrentData,overallString,overallExplanation =\
		countEvents2.turnovers(window,aggregateLevel[0],targetEvents,TeamAstring,TeamBstring,exportCurrentData,overallString,overallExplanation)

		exportCurrentData,overallString,overallExplanation =\
		countEvents2.possessions(window,aggregateLevel[0],targetEvents,TeamAstring,TeamBstring,exportCurrentData,overallString,overallExplanation)
	
		exportCurrentData,overallString,overallExplanation =\
		countEvents2.passes(window,aggregateLevel[0],targetEvents,TeamAstring,TeamBstring,exportCurrentData,overallString,overallExplanation)
	
		## Aggregate timeseries variables for the given time-period
		## By default, it uses (where easily possible) mean (avg), standard deviation (std), minimum (min), maximum (max), sum
		# Ideas:
		# - something to think about: the aggregation method (avg, std, min, max, sum) should also be data-driven. Perhaps it needs to be placed outside of this script?
		tmp,overallString,overallExplanation = \
		specific(rowswithinrange,aggregateLevel[0],rawDict,attributeDict,exportCurrentData,overallString,overallExplanation,TeamAstring,TeamBstring)
		exportMatrix.append(tmp)

		if specialCase:
			break

	return exportMatrix,overallString,overallExplanation

def specific(rowswithinrange,aggregateString,rawData,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring):
	# Per event
	# Per Team and for both teams (for vNorm, the team aggregate -techinically spatial aggregation - still has to be made)
	vel = [val for idx,val in enumerate(attributeDict['vNorm'][rowswithinrange])]
	velA = [val for idx,val in enumerate(attributeDict['vNorm'][rowswithinrange]) if rawData['Entity']['TeamID'][rowswithinrange[idx]] == TeamAstring]
	velB = [val for idx,val in enumerate(attributeDict['vNorm'][rowswithinrange]) if rawData['Entity']['TeamID'][rowswithinrange[idx]] == TeamBstring]

	# pritn(vel)
	# print(velA)
	# print(velB)
	# pdb.set_trace()

	
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
	
	exportFullExplanation.append('Average speed (in m/s) of all players per %s.' %aggregateString)

	exportDataString.append('avgSpeedA')
	exportData.append(avgSpeedA)
	exportFullExplanation.append('Average speed (in m/s) of all players from %s per %s.' %(TeamAstring,aggregateString))


	exportDataString.append('avgSpeedB')
	exportData.append(avgSpeedB)
	exportFullExplanation.append('Average speed (in m/s) of all players from %s per %s.' %(TeamBstring,aggregateString))

	# Stds	
	exportDataString.append('stdSpeed')
	exportData.append(stdSpeed)
	exportFullExplanation.append('Standard deviation speed (in m/s) of all players per %s.' %aggregateString)

	exportDataString.append('stdSpeedA')
	exportData.append(stdSpeedA)
	exportFullExplanation.append('Standard deviation speed (in m/s) of all players from %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('stdSpeedB')
	exportData.append(stdSpeedB)
	exportFullExplanation.append('Standard deviation speed (in m/s) of all players from %s per %s.' %(TeamBstring,aggregateString))

	# minmax
	exportDataString.append('minSpeed')
	exportData.append(minSpeed)
	exportFullExplanation.append('Minimum speed (in m/s) of all players per %s.' %aggregateString)

	exportDataString.append('minSpeedA')
	exportData.append(minSpeedA)
	exportFullExplanation.append('Minimum speed (in m/s) of all players from %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('minSpeedB')
	exportData.append(minSpeedB)
	exportFullExplanation.append('Minimum speed (in m/s) of all players from %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('maxSpeed')
	exportData.append(maxSpeed)
	exportFullExplanation.append('Maximum speed (in m/s) of all players per %s.' %aggregateString)

	exportDataString.append('maxSpeedA')
	exportData.append(maxSpeedA)
	exportFullExplanation.append('Maximum speed (in m/s) of all players from %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('maxSpeedB')
	exportData.append(maxSpeedB)
	exportFullExplanation.append('Maximum speed (in m/s) of all players from %s per %s.' %(TeamBstring,aggregateString))


	#########################################

	exportDataString.append('avgSpreadA')
	exportData.append(np.nanmean(attributeDict['SpreadA'][rowswithinrange]))
	exportFullExplanation.append('The average spread (= averge player distance to team centroid) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('avgSpreadB')
	exportData.append(np.nanmean(attributeDict['SpreadB'][rowswithinrange]))
	exportFullExplanation.append('The average spread (= averge player distance to team centroid) of %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('stdSpreadA')
	exportData.append(np.nanstd(attributeDict['SpreadA'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the spread (= averge player distance to team centroid) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('stdSpreadB')
	exportData.append(np.nanstd(attributeDict['SpreadB'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the spread (= averge player distance to team centroid) of %s per %s.' %(TeamBstring,aggregateString))


	exportDataString.append('avg_stdSpreadA')
	exportData.append(np.nanmean(attributeDict['stdSpreadA'][rowswithinrange]))
	exportFullExplanation.append('The average uniformity around center (= std player distance to team centroid) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('avg_stdSpreadB')
	exportData.append(np.nanmean(attributeDict['stdSpreadB'][rowswithinrange]))
	exportFullExplanation.append('The average uniformity around center (= std player distance to team centroid) of %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('std_stdSpreadA')
	exportData.append(np.nanstd(attributeDict['stdSpreadA'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the uniformity around center (= std player distance to team centroid) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('std_stdSpreadB')
	exportData.append(np.nanstd(attributeDict['stdSpreadB'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the uniformity around center (= std player distance to team centroid) of %s per %s.' %(TeamBstring,aggregateString))

	
	exportDataString.append('avgWidthA')
	exportData.append(np.nanmean(attributeDict['WidthA'][rowswithinrange]))
	exportFullExplanation.append('The average width (= widest X distance between players) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('avgWidthB')
	exportData.append(np.nanmean(attributeDict['WidthB'][rowswithinrange]))
	exportFullExplanation.append('The average width (= widest X distance between players) of %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('stdWidthA')
	exportData.append(np.nanstd(attributeDict['WidthA'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the width (= widest X distance between players) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('stdWidthB')
	exportData.append(np.nanstd(attributeDict['WidthB'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the width (= widest X distance between players) of %s per %s.' %(TeamBstring,aggregateString))


	exportDataString.append('avgLengthA')
	exportData.append(np.nanmean(attributeDict['LengthA'][rowswithinrange]))
	exportFullExplanation.append('The average length (= widest Y distance between players) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('avgLengthB')
	exportData.append(np.nanmean(attributeDict['LengthB'][rowswithinrange]))
	exportFullExplanation.append('The average length (= widest Y distance between players) of %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('stdLengthA')
	exportData.append(np.nanstd(attributeDict['LengthA'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the length (= widest Y distance between players) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('stdLengthB')
	exportData.append(np.nanstd(attributeDict['LengthB'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the length (= widest Y distance between players) of %s per %s.' %(TeamBstring,aggregateString))
	

	exportDataString.append('avgSurfaceA')
	exportData.append(np.nanmean(attributeDict['SurfaceA'][rowswithinrange]))
	exportFullExplanation.append('The average surface area (in unknown units) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('avgSurfaceB')
	exportData.append(np.nanmean(attributeDict['SurfaceB'][rowswithinrange]))
	exportFullExplanation.append('The average surface area (in unknown units) of %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('stdSurfaceA')
	exportData.append(np.nanstd(attributeDict['SurfaceA'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the surface area (in unknown units) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('stdSurfaceB')
	exportData.append(np.nanstd(attributeDict['SurfaceB'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the surface area (in unknown units) of %s per %s.' %(TeamBstring,aggregateString))


	exportDataString.append('avgsumVerticesA')
	exportData.append(np.nanmean(attributeDict['sumVerticesA'][rowswithinrange]))
	exportFullExplanation.append('The average circumference (in unknown units) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('avgsumVerticesB')
	exportData.append(np.nanmean(attributeDict['sumVerticesB'][rowswithinrange]))
	exportFullExplanation.append('The average circumference (in unknown units) of %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('stdsumVerticesA')
	exportData.append(np.nanstd(attributeDict['sumVerticesA'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the circumference (in unknown units) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('stdsumVerticesB')
	exportData.append(np.nanstd(attributeDict['sumVerticesB'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the circumference (in unknown units) of %s per %s.' %(TeamBstring,aggregateString))


	exportDataString.append('avgShapeRatioA')
	exportData.append(np.nanmean(attributeDict['ShapeRatioA'][rowswithinrange]))
	exportFullExplanation.append('The average shape ratio (longest rib / shortest rib) (values < 1 should not be possible) (the closer to 1, the more uniformly spread out) of %s per %s (NB: this is a measure of spread and should correlate with stdSpreadA).' %(TeamAstring,aggregateString))

	exportDataString.append('avgShapeRatioB')
	exportData.append(np.nanmean(attributeDict['ShapeRatioB'][rowswithinrange]))
	exportFullExplanation.append('The average shape ratio (longest rib / shortest rib) (values < 1 should not be possible) (the closer to 1, the more uniformly spread out) of %s per %s (NB: this is a measure of spread and should correlate with stdSpreadA).' %(TeamBstring,aggregateString))

	exportDataString.append('stdShapeRatioA')
	exportData.append(np.nanstd(attributeDict['ShapeRatioA'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the shape ratio (longest rib / shortest rib) (values < 1 should not be possible) (the closer to 1, the more uniformly spread out) of %s per %s (NB: this is a measure of spread and should correlate with stdSpreadA).' %(TeamAstring,aggregateString))

	exportDataString.append('stdShapeRatioB')
	exportData.append(np.nanstd(attributeDict['ShapeRatioB'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the shape ratio (longest rib / shortest rib) (values < 1 should not be possible) (the closer to 1, the more uniformly spread out) of %s per %s (NB: this is a measure of spread and should correlate with stdSpreadA).' %(TeamBstring,aggregateString))

	# Unused:
	# attributeDict['Possession/Turnover ']	attributeDict['Pass']	attributeDict['distFrame']	attributeDict['Goal ']	attributeDict['currentPossession'] attributeDict['TeamCentXA']	attributeDict['TeamCentYA']	attributeDict['TeamCentXB']	attributeDict['TeamCentYB']

	return 	exportData,exportDataString,exportFullExplanation

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