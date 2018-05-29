# 09-02-2018 Rens Meerhoff
# In process. Continue with PairwisePerTeam2() 
# --> which will be edited to automatically identify whether a variable is a team or individual variable.
#
# 30-11-2017 Rens Meerhoff
# Function to plot timeseries variables.
#
# vNorm => normalized velocity

import pylab
import pdb; #pdb.set_trace()
from warnings import warn
import numpy as np
from os.path import isfile, join, isdir, exists
from os import listdir, makedirs
# import CSVexcerpt
import matplotlib.pyplot as plt
from matplotlib import colors as mcolors
import time
import math
import pandas as pd

if __name__ == '__main__':		

	# 09-02-2018 Rens Meerhoff
	# The plot new style
	process(plotTheseAttributes,aggregateLevel,targetEvents,rawDict,attributeDict,attributeLabel,tmpFigFolder,fname,TeamAstring,TeamBstring)

def process(plotTheseAttributes,aggregateLevel,targetEvents,rawDict,attributeDict,attributeLabel,tmpFigFolder,fname,TeamAstring,TeamBstring,debuggingMode):
	# Idea: add overview of positions on the court
	tPlot = time.time()	# do stuff
	
	xLabel = attributeLabel['Ts']
	
	# Put TeamID and PlayerID in attributeDict for pivoting
	if 'PlayerID' not in attributeDict.keys():
		attributeDict = pd.concat([attributeDict, rawDict['PlayerID']], axis = 1) # Skip the duplicate 'Ts' columns
	if 'TeamID' not in attributeDict.keys():
		attributeDict = pd.concat([attributeDict, rawDict['TeamID']], axis = 1) # Skip the duplicate 'Ts' columns		
	# attributeDict.drop('Ts', axis = 1, inplace = True)
	
	for idx,currentEvent in enumerate(targetEvents[aggregateLevel[0]]):
		# Find rows
		# Idea: Similar to tempAgg. Maybe write a generic module?
		fileAggregateID,rowswithinrangeTeam,rowswithinrangeBall,rowswithinrangePlayer,rowswithinrangePlayerA,rowswithinrangePlayerB,specialCase,skipCurrentEvent = \
		findRows(idx,aggregateLevel,targetEvents,rawDict,TeamAstring,TeamBstring,currentEvent)
		
		if skipCurrentEvent:
			continue
		
		for plotThisAttribute in plotTheseAttributes:

			plt.figure(num=None, figsize=(3.8*5,3*5), dpi=300, facecolor='w', edgecolor='k')


			if type(plotThisAttribute) == tuple: # pairwise comparison of team
				# Pairwise per team
				yLabel = findYlabel(plotThisAttribute,attributeLabel,TeamAstring,TeamBstring) 
				# Plot it
				pairwisePerTeam(plotThisAttribute,attributeDict,rowswithinrangeTeam,rawDict,TeamAstring,TeamBstring)
				varName = plotThisAttribute[0]
				if plotThisAttribute[0][-1] == 'A' or plotThisAttribute[0][-1] == 'B':
					varName = varName[:-1]
				outputFilename = tmpFigFolder + fname + '_' + varName + '_' + fileAggregateID + '.jpg'
			else:
				# Plot it
				plotPerPlayerPerTeam(plotThisAttribute,attributeDict,rowswithinrangePlayerA,rowswithinrangePlayerB,TeamAstring,TeamBstring)
				labelProvided = [True for j in attributeLabel.keys() if plotThisAttribute == j]
				if labelProvided:
					yLabel = attributeLabel[plotThisAttribute]
				else:
					yLabel = 'Unknown'

				outputFilename = tmpFigFolder + fname + '_' + plotThisAttribute + '_' + fileAggregateID + '.jpg'

			plt.title(fileAggregateID)
			plt.xlabel(xLabel)
			plt.ylabel(yLabel)
			plt.savefig(outputFilename, figsize=(1,1), dpi = 300, bbox_inches='tight')
			print('EXPORTED: <%s>' %outputFilename)
			plt.close()

		if specialCase:
			break
	if debuggingMode:
		elapsed = str(round(time.time() - tPlot, 2))
		print('Time elapsed during plotTimeseries: %ss' %elapsed)

def findRows(idx,aggregateLevel,targetEvents,rawDict,TeamAstring,TeamBstring,currentEvent):
	# Create the output string that identifies the current event
	aggregateString = '%03d_%s' %(idx,aggregateLevel[0])	
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
		fileAggregateID = aggregateString + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')'
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
		fileAggregateID = aggregateString + '_' 'window(all)_lag(none)'
	else:
		# And these are the remaining ones. The ones for which the ijk-algorithm should be designed.
		# Here, the window is determined by the user input: window-size and lag (and possibly in the future aggregation method)

		tEnd = currentEvent[0] - aggregateLevel[2]
		tStart = tEnd - aggregateLevel[1]
		fileAggregateID = aggregateString + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')'

	# Determine the rows corresponding to the current event.
	if tEnd == None or tStart == None: # Check if both end and start are allocated
		warn('\nEvent %d skipped because tStart = <<%s>> and tEnd = <<%s>>.\n' %(idx,tStart,tEnd))
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
	## The old notation.
	rowswithinrangeTeam = rawDict['Ts'][rowswithinrange].index[rawDict['PlayerID'][rowswithinrange] == 'groupRow']
	rowswithinrangeBall = rawDict['Ts'][rowswithinrange].index[rawDict['PlayerID'][rowswithinrange] == 'ball']
	rowswithinrangePlayer = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'][rowswithinrange] != '']
	rowswithinrangePlayerA = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'][rowswithinrange] == TeamAstring]
	rowswithinrangePlayerB = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'][rowswithinrange] == TeamBstring]
	
	# rowswithinrangeTeam = rawDict['Ts'][rowswithinrange].index[rawDict['PlayerID'].loc[rowswithinrange] == 'groupRow']
	# rowswithinrangeBall = rawDict['Ts'][rowswithinrange].index[rawDict['PlayerID'].loc[rowswithinrange] == 'ball']
	# rowswithinrangePlayer = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'].loc[rowswithinrange] != '']
	# rowswithinrangePlayerA = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'].loc[rowswithinrange] == TeamAstring]
	# rowswithinrangePlayerB = rawDict['Ts'][rowswithinrange].index[rawDict['TeamID'].loc[rowswithinrange] == TeamBstring]

	# # rowswithinrangeTeam = spatAggPanda['PlayerID'].loc[rowswithinrange] == 'groupRow'.index
	# rowswithinrangeTeam = rawDict['PlayerID'].loc[rowswithinrange] == 'groupRow'.index
	# rowswithinrangeBall = rawDict['PlayerID'].loc[rowswithinrange] == 'ball'.index
	# rowswithinrangePlayer = rawDict['TeamID'].loc[rowswithinrange] != ''.index
	# rowswithinrangePlayerA = [rawDict['TeamID'].loc[rowswithinrange] == TeamAstring].index
	# rowswithinrangePlayerB = rawDict['TeamID'].loc[rowswithinrange] == TeamBstring .index

	return fileAggregateID,rowswithinrangeTeam,rowswithinrangeBall,rowswithinrangePlayer,rowswithinrangePlayerA,rowswithinrangePlayerB,specialCase,skipCurrentEvent

def findYlabel(plotThisAttribute,attributeLabel,TeamAstring,TeamBstring):
	
	tmp = []
	for itmp in [0,1]:
		labelProvided = [True for j in attributeLabel.keys() if plotThisAttribute[itmp] == j]
		
		if labelProvided:
			tmp.append(attributeLabel[plotThisAttribute[itmp]]) # take the label as provided
			if TeamAstring in tmp[itmp]:
				ofTeamAstring = ' of %s' %TeamAstring
				if ofTeamAstring in tmp[itmp]:
					tmp[itmp] = tmp[itmp].replace(ofTeamAstring,'')	
				else:
					tmp[itmp] = tmp[itmp].replace(TeamAstring,'both teams')

			if TeamBstring in tmp[itmp]:
				ofTeamBstring = ' of %s' %TeamBstring
				if ofTeamBstring in tmp[itmp]:
					tmp[itmp] = tmp[itmp].replace(ofTeamBstring,'')	
				else:
					tmp[itmp] = tmp[itmp].replace(TeamBstring,'both teams')

		else:
			tmp.append('Unknown')
			warn('\nWARNING: y-label not specified.\nPlease provide y-label in <attributeLabel> for <%s>.' %plotThisAttribute[itmp])
	yLabel = tmp[0]

	if tmp[0] != tmp[1]:
		# To do: find a way to generalize labels (i.e., take out team bit)
		warn('\nWARNING: y-labels not identical:\n<%s>\nand\n<%s>' %(tmp[0],tmp[1]))

	return yLabel

def pairwisePerTeam(plotThisAttribute,attributeDict,rowswithinrangeTeam,rawDict,TeamAstring,TeamBstring):
	Y1 = attributeDict[plotThisAttribute[0]][rowswithinrangeTeam]
	Y2 = attributeDict[plotThisAttribute[1]][rowswithinrangeTeam]

	X1 = rawDict['Ts'][rowswithinrangeTeam]
	X2 = rawDict['Ts'][rowswithinrangeTeam]
	
	# Look for gaps in time:
	# Idea: could separate this per team. But with the current definitions, Values for one team should occur equally often as for any other team
	t0 = X1[:-1].reset_index(level=None, drop=False, inplace=False)
	t1 = X1[1:].reset_index(level=None, drop=False, inplace=False)

	dt = t1-t0

	# Find the dt where it is more than 1.5 times the median
	jumps = dt['Ts'][dt['Ts']>(np.median(dt['Ts'])*1.5)]
	jumpStarts = t0['Ts'][dt['Ts']>(np.median(dt['Ts'])*1.5)]

	if jumps.empty:
		# no jumps detected, plot as normal
		pltA = plt.plot(X1,Y1,color='red',linestyle='-',label = TeamAstring) ##### I should use code below to make it stick to the window
		pltB = plt.plot(X2,Y2,color='blue',linestyle='--',label= TeamBstring)
		plt.legend()
	else:
		# Special case, don't connect the line where there are jumps
		nextStart = int(X1.index[0])
		for jumpStart in jumpStarts:
			curStart = nextStart
			curEnd = int(X1[X1 == jumpStart].index[0])

			pltA = plt.plot(X1.loc[curStart:curEnd],Y1.loc[curStart:curEnd],color='red', linestyle='-') ##### I should use code below to make it stick to the window
			pltB = plt.plot(X2.loc[curStart:curEnd],Y2.loc[curStart:curEnd],color='blue',linestyle='--')

			nextStart = curEnd + 2
		plt.legend([pltA[0], pltB[0]], [TeamAstring,TeamBstring])

def plotPerPlayerPerTeam(plotThisAttribute,attributeDict,rowswithinrangePlayerA,rowswithinrangePlayerB,TeamAstring,TeamBstring):

	# Team_AX = dfA.pivot(columns='Ts', values='X')
	Y1 = attributeDict.loc[rowswithinrangePlayerA].pivot(columns='PlayerID',values=plotThisAttribute)
	Y2 = attributeDict.loc[rowswithinrangePlayerB].pivot(columns='PlayerID',values=plotThisAttribute)
	X1 = attributeDict.loc[rowswithinrangePlayerA].pivot(columns='PlayerID',values='Ts')
	X2 = attributeDict.loc[rowswithinrangePlayerB].pivot(columns='PlayerID',values='Ts')

	# # Sort colors by hue, saturation, value and name.
	colors = dict(mcolors.BASE_COLORS, **mcolors.CSS4_COLORS)
	by_hsv = sorted((tuple(mcolors.rgb_to_hsv(mcolors.to_rgba(color)[:3])), name)
	                for name, color in colors.items())
	sorted_names = [name for hsv, name in by_hsv]
	[sorted_names.remove(i) for i in ['gainsboro', 'whitesmoke', 'w', 'white', 'snow']] # remove some silly colors
	
	# Use this to make code more generic (and allow user to specify color)
	refColorA = 'red'
	refColorB = 'blue'
	

	# Colors A
	refIndA = [i for i,v in enumerate(sorted_names) if v == refColorA]
	if refIndA == []:
		warn('\nWARNING: Specified reference color not found.\nConsider specifying a different color.')

	indA = Y1.shape[1]
	startColor = math.floor(refIndA[0] - .5 * indA)
	endColor = math.floor(refIndA[0] + .5 * indA)
	dC = round((endColor - startColor) / indA)
	colorPickerA = np.arange(startColor,endColor,dC)
	colorPickerA[round(len(colorPickerA)/2)], colorPickerA[-1] = colorPickerA[-1], colorPickerA[round(len(colorPickerA)/2)]

	# Colors B
	refIndB = [i for i,v in enumerate(sorted_names) if v == refColorB]
	if refIndB == []:
		warn('\nWARNING: Specified reference color not found.\nConsider specifying a different color.')

	indB = Y2.shape[1]
	startColor = math.floor(refIndB[0] - .5 * indB)
	endColor = math.floor(refIndB[0] + .5 * indB)
	dC = round((endColor - startColor) / indB)
	colorPickerB = np.arange(startColor,endColor,dC)
	colorPickerB[round(len(colorPickerB)/2)], colorPickerB[-1] = colorPickerB[-1], colorPickerB[round(len(colorPickerB)/2)]

	for ix,player in enumerate(X1.keys()):
		curColor = sorted_names[colorPickerA[ix]]
		pltA = plt.plot(X1[player],Y1[player],color=curColor,linestyle='-') 

	for ix,player in enumerate(X2.keys()):
		curColor = sorted_names[colorPickerB[ix]]
		pltB = plt.plot(X2[player],Y2[player],color=curColor,linestyle='-')

	plt.legend([pltA[0], pltB[0]], [TeamAstring,TeamBstring])	