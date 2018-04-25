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
from scipy.interpolate import InterpolatedUnivariateSpline


if __name__ == '__main__':		

	# 09-02-2018 Rens Meerhoff
	# The plot new style
	process(plotTheseAttributes,aggregateLevel,rawDict,attributeDict,attributeLabel,tmpFigFolder,fname,TeamAstring,TeamBstring)

def process(plotTheseAttributes,aggregateLevel,eventExcerptPanda,attributeLabel,tmpFigFolder,fname,TeamAstring,TeamBstring,debuggingMode):
	# process(plotTheseAttributes,aggregateLevel,targetEvents,rawDict,attributeDict,attributeLabel,tmpFigFolder,fname,TeamAstring,TeamBstring,debuggingMode):
	

	# Idea: add overview of positions on the court
	tPlot = time.time()	# do stuff
	
	xLabel = attributeLabel['Ts']
	
	# # Put TeamID and PlayerID in attributeDict for pivoting
	# if 'PlayerID' not in attributeDict.keys():
	# 	attributeDict = pd.concat([attributeDict, rawDict['PlayerID']], axis = 1) # Skip the duplicate 'Ts' columns
	# if 'TeamID' not in attributeDict.keys():
	# 	attributeDict = pd.concat([attributeDict, rawDict['TeamID']], axis = 1) # Skip the duplicate 'Ts' columns		
	# attributeDict.drop('Ts', axis = 1, inplace = True)
	
	if not 'temporalAggregate' in eventExcerptPanda.keys():
		# No events detected
		warn('\nWARNING: No temporalAggregate detected. \nCouldnt plot any data.\nUse <Random> to create random events, or <full> to plot the whole file.\n')
		return
	# for idx,currentEvent in enumerate(targetEvents[aggregateLevel[0]]):
	# for i in pd.unique(eventExcerptPanda['temporalAggregate']):

		# Find rows
		# Idea: Similar to tempAgg. Maybe write a generic module?

		# fileAggregateID,rowswithinrangeTeam,rowswithinrangeBall,rowswithinrangePlayer,rowswithinrangePlayerA,rowswithinrangePlayerB,specialCase,skipCurrentEvent = \
		# findRows(idx,aggregateLevel,targetEvents,rawDict,TeamAstring,TeamBstring,currentEvent)
		
		# if skipCurrentEvent:
		# 	continue

		# A bit of an elaborate way of finding the rows, but that's a leftover of converting an old script that was based on rawDict and attributeDict
	# currentEvent = eventExcerptPanda.loc[eventExcerptPanda['temporalAggregate'] == i]
	# currentEvent = currentEvent.reset_index()

	tmp = eventExcerptPanda.loc[eventExcerptPanda['PlayerID'] == 'groupRow']
	rowswithinrangeTeam = tmp.index
	tmp = eventExcerptPanda.loc[eventExcerptPanda['TeamID'] == TeamAstring]#.index
	rowswithinrangePlayerA = tmp.index
	tmp = eventExcerptPanda.loc[eventExcerptPanda['TeamID'] == TeamBstring]#.index
	rowswithinrangePlayerB = tmp.index

	fileAggregateID = 'window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')'
	
	# print(plotTheseAttributes)

	for plotThisAttribute in plotTheseAttributes:
		# print(plotThisAttribute)
		plt.figure(num=None, figsize=(3.8*5,3*5), dpi=300, facecolor='w', edgecolor='k')


		if type(plotThisAttribute) == tuple: # pairwise comparison of team
			# Pairwise per team
			yLabel = findYlabel(plotThisAttribute,attributeLabel,TeamAstring,TeamBstring) 

			# Plot it
			plotPerTeamPerEvent(plotThisAttribute,eventExcerptPanda,rowswithinrangeTeam,TeamAstring,TeamBstring)

			varName = plotThisAttribute[0]
			if plotThisAttribute[0][-1] == 'A' or plotThisAttribute[0][-1] == 'B':
				varName = varName[:-1]
			if plotThisAttribute[0][-4:] == '_oth' or plotThisAttribute[0][-4:] == '_ref':
				varName = varName[:-4]
			outputFilename = tmpFigFolder + fname + '_' + varName + '_' + fileAggregateID + '.jpg'
		else:
			# # Plot it
			# plotPerPlayerPerTeam(plotThisAttribute,currentEvent,rowswithinrangePlayerA,rowswithinrangePlayerB,TeamAstring,TeamBstring)
			labelProvided = [True for j in attributeLabel.keys() if plotThisAttribute == j]
			if labelProvided:
				yLabel = attributeLabel[plotThisAttribute][0] # because here it's a pandaaaa
			else:
				yLabel = 'Unknown'

			outputFilename = tmpFigFolder + fname + '_' + plotThisAttribute + '_' + fileAggregateID + '.jpg'

		plt.title(fileAggregateID)
		plt.xlabel(xLabel)
		plt.ylabel(yLabel)
		plt.savefig(outputFilename, figsize=(1,1), dpi = 300, bbox_inches='tight')
		print('EXPORTED: <%s>' %outputFilename)
		plt.close()
		# pdb.set_trace()
		# if specialCase:
		# 	break
	if debuggingMode:
		elapsed = str(round(time.time() - tPlot, 2))
		print('Time elapsed during plotTimeseries: %ss' %elapsed)


def findYlabel(plotThisAttribute,attributeLabel,TeamAstring,TeamBstring):
	# Unlike trialVisualization, this function is panda based.

	if plotThisAttribute[0] in attributeLabel.keys():
		tmpA = attributeLabel[plotThisAttribute[0]][0]
	else:
		tmpA = 'Unknown'
	
	if plotThisAttribute[1] in attributeLabel.keys():
		tmpB = attributeLabel[plotThisAttribute[1]][0]
	else:
		tmpB = 'Unknown'

	tmp = []
	for i in [tmpA,tmpB]:
		if TeamAstring in i:
			ofTeamAstring = ' of %s' %TeamAstring
			if ofTeamAstring in i:
				i = i.replace(ofTeamAstring,'')	
			else:
				i = i.replace(TeamAstring,'both teams')

		elif TeamBstring in i:
			ofTeamBstring = ' of %s' %TeamBstring
			if ofTeamBstring in i:
				i = i.replace(ofTeamBstring,'')	
			else:
				i = i.replace(TeamBstring,'both teams')

		tmp.append(i)
	
	yLabel = tmp[0]

	if tmp[0] != tmp[1]:
		# To do: find a way to generalize labels (i.e., take out team bit)
		warn('\nWARNING: y-labels not identical:\n<%s>\nand\n<%s>' %(tmp[0],tmp[1]))
		
	return yLabel

# def pairwisePerTeam(plotThisAttribute,eventExcerptPanda,rowswithinrangeTeam,TeamAstring,TeamBstring):
# 	Y1 = eventExcerptPanda[plotThisAttribute[0]][rowswithinrangeTeam]
# 	Y2 = eventExcerptPanda[plotThisAttribute[1]][rowswithinrangeTeam]

# 	X1 = eventExcerptPanda['eventTime'][rowswithinrangeTeam]
# 	X2 = eventExcerptPanda['eventTime'][rowswithinrangeTeam]
	
# 	# Look for gaps in time:
# 	# Idea: could separate this per team. But with the current definitions, Values for one team should occur equally often as for any other team
# 	t0 = X1[:-1].reset_index(level=None, drop=False, inplace=False)
# 	t1 = X1[1:].reset_index(level=None, drop=False, inplace=False)

# 	dt = t1-t0

# 	# Find the dt where it is more than 1.5 times the median
# 	# print(max(dt['eventTime']))
# 	jumps = dt['eventTime'][dt['eventTime']>(np.median(dt['eventTime'])*1.5)]
# 	jumpStarts = t0['eventTime'][dt['eventTime']>(np.median(dt['eventTime'])*1.5)]


# 	if jumps.empty:
# 		print('empty')
# 		# no jumps detected, plot as normal
# 		pltA = plt.plot(X1,Y1,color='red',linestyle='-',label = TeamAstring) ##### I should use code below to make it stick to the window
# 		pltB = plt.plot(X2,Y2,color='blue',linestyle='--',label= TeamBstring)
# 		plt.legend()
# 	else:
# 		# print('jumps')
# 		# Special case, don't connect the line where there are jumps
# 		nextStart = int(X1.index[0])
# 		for jumpStart in jumpStarts:
# 			curStart = nextStart
# 			curEnd = int(X1[X1 == jumpStart].index[0])

# 			pltA = plt.plot(X1.loc[curStart:curEnd],Y1.loc[curStart:curEnd],color='red', linestyle='-') ##### I should use code below to make it stick to the window
# 			pltB = plt.plot(X2.loc[curStart:curEnd],Y2.loc[curStart:curEnd],color='blue',linestyle='--')

# 			nextStart = curEnd + 2
# 		plt.legend([pltA[0], pltB[0]], [TeamAstring,TeamBstring])

# def plotPerPlayerPerTeam(plotThisAttribute,eventExcerptPanda,rowswithinrangePlayerA,rowswithinrangePlayerB,TeamAstring,TeamBstring,rowswithinrangeTeam):
def plotPerTeamPerEvent(plotThisAttribute,eventExcerptPanda,rowswithinrangeTeam,TeamAstring,TeamBstring):
	# eventExcerptPanda.set_index('eventTime', drop=True, append=False, inplace=True, verify_integrity=False)
	# # eventExcerptPanda.set_index('eventTimeIndex', drop=True, append=False, inplace=True, verify_integrity=False)

	# print(plotThisAttribute[0])
	Y1 = eventExcerptPanda.loc[rowswithinrangeTeam].pivot(columns='EventUID',values=plotThisAttribute[0])
	Y2 = eventExcerptPanda.loc[rowswithinrangeTeam].pivot(columns='EventUID',values=plotThisAttribute[1])
	X1 = eventExcerptPanda.loc[rowswithinrangeTeam].pivot(columns='EventUID',values='eventTime')
	X2 = X1#eventExcerptPanda.loc[rowswithinrangeTeam].pivot(columns='EventUID',values='eventTime')

	# interpolate events to have the same timestamp (necessary for averaging and comparisons)
	# first, just do it in the plotting procedcure.
	# Later, you could already interpolate before exporting the eventAggregate file.
	
	# The interpolated time
	X_int = np.arange(round(min(eventExcerptPanda['eventTime']),1),max(eventExcerptPanda['eventTime'] +0.1),0.1)
	# print(X_int)
	valColsA = pd.DataFrame([])
	valColsB = pd.DataFrame([])

	interpolatedVals = pd.DataFrame([])
	for ix,event in enumerate(X1.keys()): # per player/ball/groupRow
		# interpolate the Y1 corresponding to X1
		curIdx = np.where(pd.notnull(Y1[event]))
		curX1 = X1[event].iloc[curIdx]
		curY1 = Y1[event].iloc[curIdx]
		
		curIdx = np.where(pd.notnull(Y2[event]))
		curX2 = X2[event].iloc[curIdx]
		curY2 = Y2[event].iloc[curIdx]

		order = 3 # cubic spline
		s = InterpolatedUnivariateSpline(curX1, curY1, k=order)
		Y1_int = s(X_int)
		s = InterpolatedUnivariateSpline(curX2, curY2, k=order)
		Y2_int = s(X_int)

		# Maybe take the first occurring time for this event as the start time to avoid interpolating beyond the data.
		# replace with nans until the first occurring time?
		# first occurring time:
		firstIdx = np.where(abs(X_int - min(curX1)) == min(abs(X_int - min(curX1))))
		# Replace any interpolated values from before the first time occurred (to avoid weird interpolations)
		replaceThese = np.where(X_int < X_int[firstIdx])
		Y1_int[replaceThese] = np.nan

		# and the same for y2
		firstIdx = np.where(abs(X_int - min(curX2)) == min(abs(X_int - min(curX2))))
		replaceThese = np.where(X_int < X_int[firstIdx])
		Y2_int[replaceThese] = np.nan

		# Either create a dataset with every event in a new column:
		curVals_without_A = pd.DataFrame(Y1_int,columns = [event], index = X_int)
		valColsA = pd.concat([valColsA,curVals_without_A], axis = 1)
		
		curVals_without_B = pd.DataFrame(Y2_int,columns = [event], index = X_int)
		valColsB = pd.concat([valColsB,curVals_without_B], axis = 1)
		
		# Or create a dataset that has the column EventUID (and others, if necessary)
		# --> allow you to add multiple characteristics that could be selected for plotting (Team / Goal scored / Experimental group etc..)
		# --> but to plot, it will need to be pivotted (and it'll end up like < valColsA > )
		ID = [event] * len(X_int)
		cur_UID = pd.DataFrame(ID,columns = ['EventUID'], index = X_int)
		cur_Y1 = pd.DataFrame(Y1_int,columns = ['Y1'], index = X_int)
		cur_Y2 = pd.DataFrame(Y2_int,columns = ['Y2'], index = X_int)

		curVals = pd.concat([cur_UID,cur_Y1,cur_Y2])
		valRows = interpolatedVals.append(curVals)

		# # why does this not work
		# tmp = pd.DataFrame([ID, Y1_int.tolist()],columns = ['EventUID','Y1'], index = X_int)

	
	# avgY1 = valRows['Y1'].mean()
	avgY1 = np.mean(valColsA, axis = 1)
	avgY2 = np.mean(valColsB, axis = 1)

	# # Sort colors by hue, saturation, value and name.
	colors = dict(mcolors.BASE_COLORS, **mcolors.CSS4_COLORS)
	by_hsv = sorted((tuple(mcolors.rgb_to_hsv(mcolors.to_rgba(color)[:3])), name)
	                for name, color in colors.items())
	sorted_names = [name for hsv, name in by_hsv]
	[sorted_names.remove(i) for i in ['gainsboro', 'whitesmoke', 'w', 'white', 'snow','salmon']] # remove some hard to see colors
	
	# Use this to make code more generic (and allow user to specify color)
	refColorA = 'red'
	refColorB = 'blue'

	# Colors A
	refIndA = [i for i,v in enumerate(sorted_names) if v == refColorA]
	if refIndA == []:
		warn('\nWARNING: Specified reference color not found.\nConsider specifying a different color.')

	# A little trick to make the colors - if there's only a few players - a bit more distinct
	rangeMultiplier = .5
	if valColsA.shape[1] < 8:
		rangeMultiplier = 2

	indA = valColsA.shape[1]
	startColor = math.floor(refIndA[0] - rangeMultiplier * indA)
	endColor = math.floor(refIndA[0] + rangeMultiplier * indA)
	dC = round((endColor - startColor) / indA)
	colorPickerA = np.arange(startColor,endColor,dC)
	colorPickerA[round(len(colorPickerA)/2)], colorPickerA[-1] = colorPickerA[-1], colorPickerA[round(len(colorPickerA)/2)]

	# Colors B
	refIndB = [i for i,v in enumerate(sorted_names) if v == refColorB]
	if refIndB == []:
		warn('\nWARNING: Specified reference color not found.\nConsider specifying a different color.')

	indB = valColsB.shape[1]
	startColor = math.floor(refIndB[0] - rangeMultiplier * indB)
	endColor = math.floor(refIndB[0] + rangeMultiplier * indB)
	dC = round((endColor - startColor) / indB)
	colorPickerB = np.arange(startColor,endColor,dC)
	colorPickerB[round(len(colorPickerB)/2)], colorPickerB[-1] = colorPickerB[-1], colorPickerB[round(len(colorPickerB)/2)]

	for ix,event in enumerate(valColsA.keys()):
		curColorA = sorted_names[colorPickerA[ix]]
		curColorB = sorted_names[colorPickerB[ix]]

		pltA = plt.plot(X_int,valColsA[event],color=curColorA,linestyle='-') 
		pltB = plt.plot(X_int,valColsB[event],color=curColorB,linestyle='--')

	pltAvgA = plt.plot(X_int,avgY1, color = refColorA, linestyle = '-', linewidth = 4)
	pltAvgB = plt.plot(X_int,avgY2, color = refColorB, linestyle = '--', linewidth = 4)
	# for ix,player in enumerate(X1.keys()):
	# 	curColor = sorted_names[colorPickerA[ix]]
	# 	pltA = plt.plot(X_int[player],valColsA[player],color=curColor,linestyle='-') 

	# for ix,player in enumerate(X2.keys()):
	# 	curColor = sorted_names[colorPickerB[ix]]
	# 	pltB = plt.plot(X_int[player],valColsB[player],color=curColor,linestyle='-')


	# plt.legend([pltA[0], pltB[0]], [TeamAstring,TeamBstring])	
	plt.legend([pltA[0], pltB[0]], [TeamAstring,TeamBstring])	











	# # make a new column that makes a unique identifier of each event


	# Y1 = eventExcerptPanda[plotThisAttribute[0]][rowswithinrangeTeam]
	# Y2 = eventExcerptPanda[plotThisAttribute[1]][rowswithinrangeTeam]

	# X = eventExcerptPanda['eventTime'][rowswithinrangeTeam]
	



	# # X2 = eventExcerptPanda['eventTime'][rowswithinrangeTeam]

	# # For var 1 and 2 the same
	# # 0.0s indicates the end of an event. By definition. Right?

	# startInd = 0
	# endInd = (X[X == 0.0].index)
	# for i in endInd:
	# 	idx = np.arange(float(startInd),float(endInd),1)
	# 	pltA = plt.plot(X[idx],Y1[idx],color='salmon',linestyle='-')
	# 	pltB = plt.plot(X[idx],Y2[idx],color='lightblue',linestyle='--')
		
	# 	startInd = i + 1
	# plt.show()

	# pdb.set_trace()
	

	# pltA = plt.plot(X1[player],Y1[player],color=curColor,linestyle='-') 


	# # Look for gaps in time:
	# # Idea: could separate this per team. But with the current definitions, Values for one team should occur equally often as for any other team
	# t0 = X1[:-1].reset_index(level=None, drop=False, inplace=False)
	# t1 = X1[1:].reset_index(level=None, drop=False, inplace=False)

	# dt = t1-t0

	# # Find the dt where it is more than 1.5 times the median
	# print(max(dt['eventTime']))
	# jumps = dt['eventTime'][dt['eventTime']>(np.median(dt['eventTime'])*1.5)]
	# jumpStarts = t0['eventTime'][dt['eventTime']>(np.median(dt['eventTime'])*1.5)]


	# pdb.set_trace()
	# print('hi')
	# Y1 = eventExcerptPanda.loc[rowswithinrangeTeam].pivot(columns='eventTime',values=plotThisAttribute[0])
	# Y2 = eventExcerptPanda.loc[rowswithinrangeTeam].pivot(columns='eventTime',values=plotThisAttribute[1])

	# X1 = eventExcerptPanda.loc[rowswithinrangeTeam].pivot(columns='TeamID',values='eventTime')
	# X2 = eventExcerptPanda.loc[rowswithinrangeTeam].pivot(columns='TeamID',values='eventTime')

	# time = eventExcerptPanda['eventTime'].unique()
	# print(max(time))
	# print(Y1.shape)
	# pdb.set_trace()

	# # # Team_AX = dfA.pivot(columns='eventTime', values='X')
	# # EventUID
	# # Y1 = eventExcerptPanda.loc[rowswithinrangePlayerA].pivot(columns='PlayerID',values=plotThisAttribute)
	# # Y2 = eventExcerptPanda.loc[rowswithinrangePlayerB].pivot(columns='PlayerID',values=plotThisAttribute)
	# # X1 = eventExcerptPanda.loc[rowswithinrangePlayerA].pivot(columns='PlayerID',values='eventTime')
	# # X2 = eventExcerptPanda.loc[rowswithinrangePlayerB].pivot(columns='PlayerID',values='eventTime')

	# # # Sort colors by hue, saturation, value and name.
	# colors = dict(mcolors.BASE_COLORS, **mcolors.CSS4_COLORS)
	# by_hsv = sorted((tuple(mcolors.rgb_to_hsv(mcolors.to_rgba(color)[:3])), name)
	#                 for name, color in colors.items())
	# sorted_names = [name for hsv, name in by_hsv]
	# [sorted_names.remove(i) for i in ['gainsboro', 'whitesmoke', 'w', 'white', 'snow','salmon']] # remove some hard to see colors
	
	# # Use this to make code more generic (and allow user to specify color)
	# refColorA = 'red'
	# refColorB = 'blue'

	# # Colors A
	# refIndA = [i for i,v in enumerate(sorted_names) if v == refColorA]
	# if refIndA == []:
	# 	warn('\nWARNING: Specified reference color not found.\nConsider specifying a different color.')

	# # A little trick to make the colors - if there's only a few players - a bit more distinct
	# rangeMultiplier = .5
	# if Y1.shape[1] < 8:
	# 	rangeMultiplier = 2

	# indA = Y1.shape[1]
	# startColor = math.floor(refIndA[0] - rangeMultiplier * indA)
	# endColor = math.floor(refIndA[0] + rangeMultiplier * indA)
	# dC = round((endColor - startColor) / indA)
	# colorPickerA = np.arange(startColor,endColor,dC)
	# colorPickerA[round(len(colorPickerA)/2)], colorPickerA[-1] = colorPickerA[-1], colorPickerA[round(len(colorPickerA)/2)]

	# # Colors B
	# refIndB = [i for i,v in enumerate(sorted_names) if v == refColorB]
	# if refIndB == []:
	# 	warn('\nWARNING: Specified reference color not found.\nConsider specifying a different color.')

	# indB = Y2.shape[1]
	# startColor = math.floor(refIndB[0] - rangeMultiplier * indB)
	# endColor = math.floor(refIndB[0] + rangeMultiplier * indB)
	# dC = round((endColor - startColor) / indB)
	# colorPickerB = np.arange(startColor,endColor,dC)
	# colorPickerB[round(len(colorPickerB)/2)], colorPickerB[-1] = colorPickerB[-1], colorPickerB[round(len(colorPickerB)/2)]

	# for ix,player in enumerate(X1.keys()):
	# 	curColor = sorted_names[colorPickerA[ix]]
	# 	pltA = plt.plot(X1[player],Y1[player],color=curColor,linestyle='-') 

	# for ix,player in enumerate(X2.keys()):
	# 	curColor = sorted_names[colorPickerB[ix]]
	# 	pltB = plt.plot(X2[player],Y2[player],color=curColor,linestyle='-')

	# plt.legend([pltA[0], pltB[0]], [TeamAstring,TeamBstring])	
	# AS FUNCTION
def resampleData(x,y,xnew,order): # order 1) = linear, 2) = quadratic, 3) = cubic
	f = InterpolatedUnivariateSpline(x, y, k=order)
	ynew = f(xnew)   # use interpolation function returned by `interp1d`
	# limit ynew to values around original x
	# next(x[0] for x in enumerate(L) if x[1] > 0.7)
	return[ynew]