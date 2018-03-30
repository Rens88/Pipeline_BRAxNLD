# 08-12-2017 Rens Meerhoff
# NB: From the raw data, this script should only access the always available Time, Entity, and Location.
# The script should not use the attributeDict directly, as this may differ depending on the data source.
# Instead, use targetEvents in the main process file where you can include the characteristics of the event either
# automatically, or manually (and always make the same format)

import csv
import pdb; #pdb.set_trace()
import numpy as np
import pandas as pd
from os.path import isfile, join, isdir
from os import listdir, path
from warnings import warn
import time
# From my own library:
import plotSnapshot
# import CSVexcerpt
# import CSVimportAsColumns
# import identifyDuplHeader
# import LoadOrCreateCSVexcerpt
# import individualAttributes
# import plotTimeseries
# import dataToDict
# import dataToDict2
import safetyWarning
# import exportCSV
import countEvents2

if __name__ == '__main__':
	# First try, should be removed later:
	simple(rawData,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring)

	process(targetEvents,aggregateLevel,rawDict,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring)
	# Aggregates a range of pre-defined features over a specific window:
	specific(rowswithinrange,aggregateString,rawDict,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring)	

def process(targetEvents,aggregateLevel,rawDict,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring,debuggingMode,tobeDeletedWithWarning,skipEventAgg_curFile):
	tTempAgg = time.time()

	# Create an empty to export when there are no events??
	eventExcerptPanda = pd.DataFrame()
	if aggregateLevel[0] == 'None':
		warn('\nWARNING: No temporal aggregate level indicated. \nNo temporally aggregated data exported.\nChange aggregateEvent = <%s> in USER INPUT.\n' %aggregateLevel[0])
		return exportData,exportDataString,exportFullExplanation,eventExcerptPanda

	elif len(targetEvents[aggregateLevel[0]]) == 0:
		warn('\nWARNING: No targetevents detected. \nCouldnt aggregate temporally. \nNo Data exported.\n')
		return exportData,exportDataString,exportFullExplanation,eventExcerptPanda

	exportMatrix = []
	exportDataString.append('temporalAggregate')
	exportFullExplanation.append('Level of temporal aggregation, based on <<%s>> event and counted chronologically' %aggregateLevel[0])
	
	attrDictCols = [i for i in attributeDict.columns if not i in ['Ts','TeamID','PlayerID','X','Y']]
	## FYI:
	# aggregateLevel[0] --> event ID
	# aggregateLevel[1] --> window (s)
	# aggregateLevel[2] --> lag (s)
	eventExcerptPanda = pd.DataFrame()
	for idx,currentEvent in enumerate(targetEvents[aggregateLevel[0]]):
		window,rowswithinrange,rowswithinrangeTeam,rowswithinrangeBall,rowswithinrangePlayer,rowswithinrangePlayerA,rowswithinrangePlayerB,specialCase,skipCurrentEvent = \
		findRows(aggregateLevel,targetEvents,rawDict,TeamAstring,TeamBstring,currentEvent)
		
		if skipCurrentEvent:
			warn('\nEvent %d skipped because tStart = <<%s>> and tEnd = <<%s>>.\n' %(idx,tStart,tEnd))
			continue
		
		exportCurrentData = exportData.copy()
		overallString = exportDataString.copy()
		overallExplanation = exportFullExplanation.copy()
		
		# Create the output string that identifies the current event
		aggregateString = '%03d_%s' %(idx,aggregateLevel[0])		
		exportCurrentData.append(aggregateString) # NB: String and explanation are defined before the for loop
		
		# All data in exportCurrentData are trial identifiers. Create a column in a new panda copying these:
		currentEventID = pd.DataFrame([],columns = [exportDataString],index = [rawDict['Ts'][rowswithinrange].index])
		for i,val in enumerate(exportCurrentData):
			currentEventID[exportDataString[i]] = val
		
		if not skipEventAgg_curFile:
			# Create a new panda that has the identifiers of the current event and the rawdata
			curEventExcerptPanda = pd.concat([currentEventID, rawDict.loc[rowswithinrange], attributeDict.loc[rowswithinrange, attrDictCols]], axis=1) # Skip the duplicate 'Ts' columns

			# Create an index that restarts per event
			times = curEventExcerptPanda['Ts'].unique()
			timesSorted = np.sort(times)
			newColumn = pd.DataFrame([], columns = ['newIndex', 'eventTime'])
			for i,val in np.ndenumerate(timesSorted):
				oldIndex = curEventExcerptPanda[curEventExcerptPanda['Ts'] == val].index
				eventTime = val - max(times)
				# It seems that the index can actually be negative.
				# If problematic, change the '0' below to e.g. '1000'
				newIndex = 0 - len(times) + i[0] + 1

				tmp = pd.DataFrame([], columns = ['newIndex','eventTime'], index = oldIndex)
				tmp['newIndex'] = newIndex
				tmp['eventTime'] = eventTime
				newColumn = newColumn.append(tmp)

			# Add the new index
			curEventExcerptPanda = pd.concat([newColumn,curEventExcerptPanda], axis=1) # Skip the duplicate 'Ts' columns			
			# Use the new index as the index
			curEventExcerptPanda = curEventExcerptPanda.set_index('newIndex', drop=True, append=False, inplace=False, verify_integrity=False)
			eventExcerptPanda = eventExcerptPanda.append(curEventExcerptPanda)
		else:
			warn('\nContinued with previously aggregated event data.\nIf you want to add new (or revised) spatial aggregates, change <skipEventAgg> into <False>.\n')
		# eventExcerptPanda.to_csv('C:\\Users\\rensm\\Documents\\PostdocLeiden\\NP repository\\test.csv')

		## Count exsiting events (goals, possession and passes)
		# Ideas:
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
		specific(rowswithinrange,aggregateLevel[0],rawDict,attributeDict,exportCurrentData,overallString,overallExplanation,TeamAstring,TeamBstring,tobeDeletedWithWarning)
		
		exportMatrix.append(tmp)

		# Create / append / exporteer een dataframe met alle windowsinrange (evt met huidige index, maar die moet later gereset)
		# NB: use eventExcerptPanda (which already has the trial characteristics)

		if specialCase:
			break
	if debuggingMode:
		elapsed = str(round(time.time() - tTempAgg, 2))
		print('Time elapsed during temporalAggregation: %ss' %elapsed)
	return exportMatrix,overallString,overallExplanation,eventExcerptPanda

def specific(rowswithinrange,aggregateString,rawData,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring,tobeDeletedWithWarning):
	# Per event
	# Per Team and for both teams (for vNorm, the team aggregate -techinically spatial aggregation - still has to be made)
	vel = [val for idx,val in enumerate(attributeDict['vNorm'][rowswithinrange])]
	velA = [val for idx,val in enumerate(attributeDict['vNorm'][rowswithinrange]) if rawData['TeamID'][rowswithinrange[idx]] == TeamAstring]
	velB = [val for idx,val in enumerate(attributeDict['vNorm'][rowswithinrange]) if rawData['TeamID'][rowswithinrange[idx]] == TeamBstring]

	if velA == []:
		warn('\nWARNING: No values found for <%s>\nCould be a result of an incomplete dataset.\nConsider cleaning the data again and/or running the spatial aggregation again.\nOr, you could delete the spatially aggregated file:\n<%s>\n!!!!!!!!!!!!!!!!!!!!\n' %(TeamAstring,tobeDeletedWithWarning))
	if velB == []:
		warn('\nWARNING: No values found for <%s>\nCould be a result of an incomplete dataset.\nConsider cleaning the data again and/or running the spatial aggregation again.\nOr, you could delete the spatially aggregated file:\n<%s>\n!!!!!!!!!!!!!!!!!!!!\n' %(TeamBstring,tobeDeletedWithWarning))
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
	exportFullExplanation.append('The average surface area (in m^2) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('avgSurfaceB')
	exportData.append(np.nanmean(attributeDict['SurfaceB'][rowswithinrange]))
	exportFullExplanation.append('The average surface area (in m^2) of %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('stdSurfaceA')
	exportData.append(np.nanstd(attributeDict['SurfaceA'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the surface area (in m^2) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('stdSurfaceB')
	exportData.append(np.nanstd(attributeDict['SurfaceB'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the surface area (in m^2) of %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('avgsumVerticesA')
	exportData.append(np.nanmean(attributeDict['SumVerticesA'][rowswithinrange]))
	exportFullExplanation.append('The average circumference (in m) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('avgsumVerticesB')
	exportData.append(np.nanmean(attributeDict['SumVerticesB'][rowswithinrange]))
	exportFullExplanation.append('The average circumference (in m) of %s per %s.' %(TeamBstring,aggregateString))

	exportDataString.append('stdsumVerticesA')
	exportData.append(np.nanstd(attributeDict['SumVerticesA'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the circumference (in m) of %s per %s.' %(TeamAstring,aggregateString))

	exportDataString.append('stdsumVerticesB')
	exportData.append(np.nanstd(attributeDict['SumVerticesB'][rowswithinrange]))
	exportFullExplanation.append('The standard deviation of the circumference (in m) of %s per %s.' %(TeamBstring,aggregateString))


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
	velA = [val for idx,val in enumerate(attributeDict['vNorm']) if rawData['TeamID'][idx] == TeamAstring]
	velB = [val for idx,val in enumerate(attributeDict['vNorm']) if rawData['TeamID'][idx] == TeamBstring]
	
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
	exportFullExplanation.append('The average surface area (in m^2) of %s per match.' %TeamAstring)

	exportDataString.append('avgSurfaceB')
	exportData.append(np.nanmean(attributeDict['SurfaceB']))
	exportFullExplanation.append('The average surface area (in m^2) of %s per match.' %TeamBstring)

	exportDataString.append('stdSurfaceA')
	exportData.append(np.nanstd(attributeDict['SurfaceA']))
	exportFullExplanation.append('The standard deviation of the surface area (in m^2) of %s per match.' %TeamAstring)

	exportDataString.append('stdSurfaceB')
	exportData.append(np.nanstd(attributeDict['SurfaceB']))
	exportFullExplanation.append('The standard deviation of the surface area (in m^2) of %s per match.' %TeamBstring)


	exportDataString.append('avgsumVerticesA')
	exportData.append(np.nanmean(attributeDict['sumVerticesA']))
	exportFullExplanation.append('The average circumference (in m) of %s per match.' %TeamAstring)

	exportDataString.append('avgsumVerticesB')
	exportData.append(np.nanmean(attributeDict['sumVerticesB']))
	exportFullExplanation.append('The average circumference (in m) of %s per match.' %TeamBstring)

	exportDataString.append('stdsumVerticesA')
	exportData.append(np.nanstd(attributeDict['sumVerticesA']))
	exportFullExplanation.append('The standard deviation of the circumference (in m) of %s per match.' %TeamAstring)

	exportDataString.append('stdsumVerticesB')
	exportData.append(np.nanstd(attributeDict['sumVerticesB']))
	exportFullExplanation.append('The standard deviation of the circumference (in m) of %s per match.' %TeamBstring)


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

def findRows(aggregateLevel,targetEvents,rawDict,TeamAstring,TeamBstring,currentEvent):

	specialCase = False
	skipCurrentEvent = False
	if type(targetEvents[aggregateLevel[0]]) != list:			
		# A special case: there was only one event identified, which means that <enumerate> now 
		# goes through the contents of that specific event, rather than iterating over the events.
		# Improvised solution is to overwrite currentEvent and subsequently terminate early.
		#
		# necessary to adjust input style of aggregateLevel[0] (which determines currentEvent)
		# find a better way to store aggregateLevel ?
		# Gebeurt alleen bij full?
		currentEvent = targetEvents[aggregateLevel[0]]
		# fileAggregateID = aggregateString + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')'
		specialCase = True
	# Determine tStart and tEnd
	if aggregateLevel[0] == 'Possession' or aggregateLevel[0] == 'Full' or aggregateLevel[0] == 'Run':
		# These are the events that have a fixed window. Technically it combines 2 events. The start of a targetEvent and the end of a targetEvent.
		# E.g., from possession start to possession end.
		# E.g., from start of the file to the end.
		# E.g., from the start of an attack to the end.
		# In general terms:
		# Anything that has a start and an end time should be captured here.
		tStart = currentEvent[0]
		tEnd = currentEvent[1]
		# fileAggregateID = aggregateString + '_' 'window(all)_lag(none)'
	else:
		# And these are the remaining ones. The ones for which the ijk-algorithm should be designed.
		# Here, the window is determined by the user input: window-size and lag (and possibly in the future aggregation method)

		tEnd = currentEvent[0] - aggregateLevel[2]
		tStart = tEnd - aggregateLevel[1]
		# fileAggregateID = aggregateString + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')'

	# Determine the rows corresponding to the current event.
	if tEnd == None or tStart == None: # Check if both end and start are allocated
		skipCurrentEvent = True
		return None,None,None,None,None,None,None,skipCurrentEvent
	# Find index of rows within tStart and tEnd
	if round(tStart,2) <= round(tEnd,2):
		window = (tStart,tEnd)
	else:
		window = (tEnd,tStart)
		warn('\nSTRANGE: tStart <%s> was bigger than tEnd <%s>.\nSwapped them to determine window' %(tStart,tEnd))
	
	tmp = rawDict[rawDict['Ts'] > window[0]]
	rowswithinrange = tmp[tmp['Ts'] <= window[1]].index
	del(tmp)

	rowswithinrangeTeam = rawDict['Ts'][rowswithinrange].index[rawDict['PlayerID'][rowswithinrange] == 'groupRow']
	rowswithinrangeBall = rawDict['Ts'][rowswithinrange].index[rawDict['PlayerID'][rowswithinrange] == 'ball']
	rowswithinrangePlayer = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'][rowswithinrange] != '']
	rowswithinrangePlayerA = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'][rowswithinrange] == TeamAstring]
	rowswithinrangePlayerB = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'][rowswithinrange] == TeamBstring]

	return window,rowswithinrange,rowswithinrangeTeam,rowswithinrangeBall,rowswithinrangePlayer,rowswithinrangePlayerA,rowswithinrangePlayerB,specialCase,skipCurrentEvent
