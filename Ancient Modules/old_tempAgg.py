######## 
# OLD AND GUMBY. DELETE LATER.
def specific(rowswithinrange,aggregateString,rawData,attributeDict,exportData,exportDataString,exportFullExplanation,TeamAstring,TeamBstring,tobeDeletedWithWarning):
	# Per Team and for both teams (for vNorm, the team aggregate -techinically spatial aggregation - still has to be made)
	vel = [val for idx,val in enumerate(attributeDict['vNorm'][rowswithinrange])]
	velA = [val for idx,val in enumerate(attributeDict['vNorm'][rowswithinrange]) if rawData['TeamID'][rowswithinrange[idx]] == TeamAstring]
	velB = [val for idx,val in enumerate(attributeDict['vNorm'][rowswithinrange]) if rawData['TeamID'][rowswithinrange[idx]] == TeamBstring]

	if velA == []:
		warn('\nWARNING: No values found for <%s>\nCould be a result of an incomplete dataset.\nConsider cleaning the data again and/or running the spatial aggregation again.\nOr, you could delete the spatially aggregated file:\n<%s>\n!!!!!!!!!!!!!!!!!!!!\n' %(TeamAstring,tobeDeletedWithWarning))
	if velB == []:
		warn('\nWARNING: No values found for <%s>\nCould be a result of an incomplete dataset.\nConsider cleaning the data again and/or running the spatial aggregation again.\nOr, you could delete the spatially aggregated file:\n<%s>\n!!!!!!!!!!!!!!!!!!!!\n' %(TeamBstring,tobeDeletedWithWarning))
	
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

# Redundant? delete?
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

# Redundant? delete?
def determineWindow(aggregateLevel,targetEvents,rawDict,TeamAstring,TeamBstring,currentEvent):

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
		return None,None,skipCurrentEvent
	# Find index of rows within tStart and tEnd
	if round(tStart,2) <= round(tEnd,2):
		window = (tStart,tEnd)
	else:
		window = (tEnd,tStart)
		warn('\nSTRANGE: tStart <%s> was bigger than tEnd <%s>.\nSwapped them to determine window' %(tStart,tEnd))

	# # Determine reference team
	# print(targetEvents)
	# pdb.set_trace()
	
	# tmp = rawDict[rawDict['Ts'] > window[0]]
	# rowswithinrange = tmp[tmp['Ts'] <= window[1]].index
	# del(tmp)

	# rowswithinrangeTeam = rawDict['Ts'][rowswithinrange].index[rawDict['PlayerID'][rowswithinrange] == 'groupRow']
	# rowswithinrangeBall = rawDict['Ts'][rowswithinrange].index[rawDict['PlayerID'][rowswithinrange] == 'ball']
	# rowswithinrangePlayer = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'][rowswithinrange] != '']
	# rowswithinrangePlayerA = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'][rowswithinrange] == TeamAstring]
	# rowswithinrangePlayerB = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'][rowswithinrange] == TeamBstring]

	return window,specialCase,skipCurrentEvent