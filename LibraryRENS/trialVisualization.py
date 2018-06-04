# 09-02-2018 Rens Meerhoff

# import pylab
import pdb; #pdb.set_trace()
from warnings import warn
import numpy as np
from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs, sep
# import CSVexcerpt
import matplotlib
matplotlib.use('Agg') #https://stackoverflow.com/questions/37604289/tkinter-tclerror-no-display-name-and-no-display-environment-variable?utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa
import matplotlib.pyplot as plt
from matplotlib import colors as mcolors
import matplotlib.patches as patches
import matplotlib.lines as mlines
import time
import math
import pandas as pd
import time
import imp
# import plotSnapshot

if __name__ == '__main__':

	# 09-02-2018 Rens Meerhoff
	# The plot new style
	process(plotTheseAttributes,aggregateLevel,rawDict,attributeDict,attributeLabel,tmpFigFolder,fname,TeamAstring,TeamBstring)

def process(plotTheseAttributes,aggregateLevel,eventExcerptPanda,attributeLabel,tmpFigFolder,fname,TeamAstring,TeamBstring,debuggingMode,dataType,fieldDimensions):
	tTrialVisualization = time.time()

	# An attempt to avoid the error 'Fail to allocate bitmap'
	# imp.reload(plt)
	# imp.reload(mcolors)
	# imp.reload(patches)
	# imp.reload(mlines)
	# imp.reload(matplotlib)
	# matplotlib.use('Agg')

	tmpFigFolder = tmpFigFolder + 'trialVisualization' + sep

	xLabel = attributeLabel['Ts']

	if not 'temporalAggregate' in eventExcerptPanda.keys():
		# No events detected
		warn('\nWARNING: No temporalAggregate detected. \nCouldnt plot any data.\nUse <Random> to create random events, or <full> to plot the whole file.\nOr change <skipEventAgg> to True to run it at the trial level.')

		if debuggingMode:
			elapsed = str(round(time.time() - tTrialVisualization, 2))
			print('***** Time elapsed during trialVisualization: %ss' %elapsed)
		return

	# For every unique event
	for i in pd.unique(eventExcerptPanda['temporalAggregate']):

		# Find rows
		# Idea: Similar to tempAgg. Maybe write a generic module?

		# fileAggregateID,rowswithinrangeTeam,rowswithinrangeBall,rowswithinrangePlayer,rowswithinrangePlayerA,rowswithinrangePlayerB,specialCase,skipCurrentEvent = \
		# findRows(idx,aggregateLevel,targetEvents,rawDict,TeamAstring,TeamBstring,currentEvent)

		# if skipCurrentEvent:
		# 	continue

		# A bit of an elaborate way of finding the rows, but that's a leftover of converting an old script that was based on rawDict and attributeDict
		currentEvent = eventExcerptPanda.loc[eventExcerptPanda['temporalAggregate'] == i]
		currentEvent = currentEvent.reset_index()
		# currentEvent.set_index('eventTimeIndex', drop=True, append=False, inplace=True, verify_integrity=False)

		tmp = currentEvent.loc[currentEvent['PlayerID'] == 'groupRow']
		rowswithinrangeTeam = tmp.index
		tmp = currentEvent.loc[currentEvent['TeamID'] == TeamAstring]#.index
		rowswithinrangePlayerA = tmp.index
		tmp = currentEvent.loc[currentEvent['TeamID'] == TeamBstring]#.index
		rowswithinrangePlayerB = tmp.index

		fileAggregateID = i + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')'
		teamStrings = findTeamString(currentEvent,TeamAstring,TeamBstring)

		for plotThisAttribute in plotTheseAttributes:

			plt.figure(num=None, figsize=(3.8*5,3*5), dpi=300, facecolor='w', edgecolor='k')

			if type(plotThisAttribute) == tuple: # pairwise comparison of team
				# Pairwise per team
				yLabel = findYlabel(plotThisAttribute,attributeLabel,TeamAstring,TeamBstring)
				# Plot it
				pairwisePerTeam(plotThisAttribute,currentEvent,rowswithinrangeTeam,teamStrings)

				varName = plotThisAttribute[0]
				if plotThisAttribute[0][-1] == 'A' or plotThisAttribute[0][-1] == 'B':
					varName = varName[:-1]
				if plotThisAttribute[0][-4:] == '_oth' or plotThisAttribute[0][-4:] == '_ref':
					varName = varName[:-4]

				outputFilename = tmpFigFolder + fname + '_' + varName + '_' + fileAggregateID + '.png'
			else:
				# Plot it
				plotPerPlayerPerTeam(plotThisAttribute,currentEvent,rowswithinrangePlayerA,rowswithinrangePlayerB,TeamAstring,TeamBstring)
				labelProvided = [True for j in attributeLabel.keys() if plotThisAttribute == j]
				if labelProvided:
					yLabel = attributeLabel[plotThisAttribute]
				else:
					yLabel = 'Unknown'

				outputFilename = tmpFigFolder + fname + '_' + plotThisAttribute + '_' + fileAggregateID + '.png'

			plt.title(fileAggregateID)
			plt.xlabel(xLabel)
			plt.ylabel(yLabel)
			plt.savefig(outputFilename, figsize=(1,1), dpi = 300, bbox_inches='tight')
			if debuggingMode:
				print('EXPORTED: <%s>' %outputFilename)
			plt.close()
		# Plot a snapshot of the positions
		outputFilename = tmpFigFolder + fname + '_Snapshot_' + fileAggregateID + '.png'
		plotSnapshot(outputFilename,currentEvent,rowswithinrangePlayerA,rowswithinrangePlayerB,teamStrings,fileAggregateID,dataType,fieldDimensions)
		if debuggingMode:
			print('EXPORTED: <%s>' %outputFilename)

		# if specialCase:
		# 	break
	if debuggingMode:
		elapsed = str(round(time.time() - tTrialVisualization, 2))
		print('***** Time elapsed during trialVisualization: %ss' %elapsed)
	return

def	plotSnapshot(outputFilename,currentEvent,rowswithinrangePlayerA,rowswithinrangePlayerB,teamStrings,fileAggregateID,dataType,fieldDimensions):

	# # Use this to only plot the last frame
	# tmp = currentEvent.loc[currentEvent['TeamID'] == TeamAstring]#.index
	# rowswithinrangePlayerA = tmp[tmp['Ts'] == max(currentEvent['Ts'])].index
	# tmp = currentEvent.loc[currentEvent['TeamID'] == TeamBstring]#.index
	# rowswithinrangePlayerB = tmp[tmp['Ts'] == max(currentEvent['Ts'])].index

	# do something with ref Team? In title?
	plt.figure(num=None, figsize=(3.8,3), dpi=300, facecolor='w', edgecolor='k')
	fig, ax = plt.subplots() # note we must use plt.subplots, not plt.subplot

	plotPerPlayerPerTeam('Y',currentEvent,rowswithinrangePlayerA,rowswithinrangePlayerB,teamStrings[0],teamStrings[1], x_value = 'X')
	plt.title(fileAggregateID)
	plt.xlabel('X-position (m)')
	plt.ylabel('Y-position (m)')

	borderColor = 'k'

	if not fieldDimensions['foundIt']:
		borderColor = 'r'
		# Come up with the best values possible
		if dataType == 'FDP':
			fieldDimensions.update({'X_bot_left': -50,'X_top_left': -50,'X_bot_right': 50,'X_top_right': 50,'Y_bot_left': -32.5,'Y_bot_right': -32.5,'Y_top_left': 32.7,'Y_top_right': 32.7}) # Values are given in meters
			warn('\nWARNING: no field dimensions found. If plot looks weird, include field dimensions (see importFieldDimensions.py).\nPlotted the borders in red to indicate they are estimations.')
		elif dataType == 'NP':
			# create data based field parameters...
			fieldDimensions.update({'X_bot_left': min(currentEvent['X']),'X_top_left': min(currentEvent['X']),'X_bot_right': max(currentEvent['X']),'X_top_right': max(currentEvent['X']),'Y_bot_left': min(currentEvent['Y']),'Y_bot_right': min(currentEvent['Y']),'Y_top_left': max(currentEvent['Y']),'Y_top_right': max(currentEvent['Y'])}) # Values are given in meters
			warn('\nWARNING: no field dimensions found. If plot looks weird, include field dimensions (see importFieldDimensions.py).\nPlotted the borders in red to indicate they are estimations.')
		else:
			fieldDimensions.update({'X_bot_left': -50,'X_top_left': -50,'X_bot_right': 50,'X_top_right': 50,'Y_bot_left': -32.5,'Y_bot_right': -32.5,'Y_top_left': 32.7,'Y_top_right': 32.7}) # Values are given in meters
			warn('\nWARNING: no field dimensions found. If plot looks weird, include field dimensions (see importFieldDimensions.py).\nPlotted the borders in red to indicate they are estimations.')

	XBL = fieldDimensions['X_bot_left']
	XBR = fieldDimensions['X_bot_right']
	XTR = fieldDimensions['X_top_right']
	XTL = fieldDimensions['X_top_left']

	YBL = fieldDimensions['Y_bot_left']
	YBR = fieldDimensions['Y_bot_right']
	YTR = fieldDimensions['Y_top_right']
	YTL = fieldDimensions['Y_top_left']

	X_mid_bot = ( (XBR - XBL) / 2 ) + XBL
	X_mid_top = ( (XTR - XTL) / 2 ) + XTL

	Y_mid_left = ( (YBR - YBL) / 2 ) + YBL
	Y_mid_right = ( (YTR - YTL) / 2 ) + YTL

	X_mid_mid = ( (X_mid_top - X_mid_bot) / 2 ) + X_mid_bot
	Y_mid_mid = ( (Y_mid_right - Y_mid_left) / 2 ) + Y_mid_left

	widthOfField = np.sqrt( (XBR - XTR)**2 + (YBR - YTR)**2 )
	lengthOfField = np.sqrt( (XBR - XBL)**2 + (YBR - YBL)**2 )
	MidCircleRadius = lengthOfField * 9.15/100

	# Plot the basic field
	plt.plot([XBL, XBR, XTR, XTL, XBL], [YBL, YBR, YTR, YTL, YBL], color=borderColor, linestyle='-', linewidth=1) # Field outline
	plt.plot([X_mid_bot, X_mid_top],[Y_mid_left, Y_mid_right], color=borderColor, linestyle='-', linewidth=1) # middle line
	plt.plot(X_mid_mid,Y_mid_mid,".", color = borderColor, markersize=3) # centre spot
	MidCircle = plt.Circle((X_mid_mid,Y_mid_mid), MidCircleRadius, color=borderColor, fill=False, linewidth=1) # middle circle
	ax.add_artist(MidCircle)

	# For FDP, plot the full field (could be rotated as well?)
	if dataType == 'FDP':
		x1 = XBL
		x2 = XBR
		x0 = X_mid_bot
		y1 = YBL
		y2 = YTL
		y0 = Y_mid_mid
		# Plot the whole field (including penalty box etc.)
		plt.plot([x1,x1+16.5,x1+16.5,x1], [y0-20,y0-20,y0+20,y0+20], color='k', linestyle='-', linewidth=1) # penalty box
		plt.plot([x2,x2-16.5,x2-16.5,x2], [y0-20,y0-20,y0+20,y0+20], color='k', linestyle='-', linewidth=1) # penalty box
		plt.plot([x1+11,x2-11],[y0,y0],".", color = 'k', markersize=2) # penalty mark
		plt.plot([x1,x1+5.5,x1+5.5,x1], [y0-9.12,y0-9.12,y0+9.12,y0+9.12], color='k', linestyle='-', linewidth=1) # keeper box
		plt.plot([x2,x2-5.5,x2-5.5,x2], [y0-9.12,y0-9.12,y0+9.12,y0+9.12], color='k', linestyle='-', linewidth=1) # keeper box
		ax.add_patch(patches.Arc((x1+11,y0), 9.15*2,9.15*2, 0,127+180,233+180)) # stupid semi-circle
		ax.add_patch(patches.Arc((x2-11,y0), 9.15*2,9.15*2, 0,127,233)) # stupid semi-circle
		plt.plot([x1,x1-.5,x1-.5,x1], [y0-3.62,y0-3.62,y0+3.62,y0+3.62], color='k', linestyle='-', linewidth=1) # goal
		plt.plot([x2,x2+.5,x2+.5,x2], [y0-3.62,y0-3.62,y0+3.62,y0+3.62], color='k', linestyle='-', linewidth=1) # goal

	plt.title(fileAggregateID)
	plt.savefig(outputFilename, figsize=(1,1), dpi = 300, bbox_inches='tight')
	plt.close()

	# IDEA: could plot dotted line as representing speed

def findTeamString(currentEvent,TeamAstring,TeamBstring):
	# print(currentEvent['RefTeam'])
	if all(currentEvent['RefTeam'] == TeamAstring):
		ref = TeamAstring + (' (refTeam)')
		oth = TeamBstring + (' (othTeam)')
	elif all(currentEvent['RefTeam'] == TeamBstring):
		ref = TeamBstring + (' (refTeam)')
		oth = TeamAstring + (' (othTeam)')
	else:
		warn('\nFATAL WARNING: Could not identify who was the reference team for this event..\n')
		warn('\nWARNING: refTeam <%s> did not correspond with TeamAstring <%s> or TeamBstring <%s>.\nCould not establish who the refernce team was.\nContinued with <%s> as refTeam.\n' %(refTeam,TeamAstring,TeamBstring,TeamAstring))
		ref = TeamAstring + (' (refTeam)')
		oth = TeamBstring + (' (othTeam)')

	teamStrings = (ref,oth)

	return teamStrings

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

			if 'refTeam' in tmp[itmp]:
				ofRefTeamstring = ' of refTeam'
				if ofRefTeamstring in tmp[itmp]:
					tmp[itmp] = tmp[itmp].replace(ofRefTeamstring,'')
				else:
					tmp[itmp] = tmp[itmp].replace('refTeam','both teams')

			if 'othTeam' in tmp[itmp]:
				ofOthTeamstring = ' of othTeam'
				if ofOthTeamstring in tmp[itmp]:
					tmp[itmp] = tmp[itmp].replace(ofOthTeamstring,'')
				else:
					tmp[itmp] = tmp[itmp].replace('othTeam','both teams')

		else:
			tmp.append('Unknown')
			warn('\nWARNING: y-label not specified.\nPlease provide y-label in <attributeLabel> for <%s>.' %plotThisAttribute[itmp])
	yLabel = tmp[0]

	# try:
	if tmp[0] != tmp[1]:
		# To do: find a way to generalize labels (i.e., take out team bit)
		warn('\nWARNING: y-labels not identical:\n<%s>\nand\n<%s>' %(tmp[0],tmp[1]))
	# except:
	# 	print(tmp)
	return yLabel

def pairwisePerTeam(plotThisAttribute,eventExcerptPanda,rowswithinrangeTeam,teamStrings):

	# if not plotThisAttribute[0] in eventExcerptPanda.keys():

	Y1 = eventExcerptPanda[plotThisAttribute[0]][rowswithinrangeTeam]
	Y2 = eventExcerptPanda[plotThisAttribute[1]][rowswithinrangeTeam]

	X1 = eventExcerptPanda['Ts'][rowswithinrangeTeam]
	X2 = eventExcerptPanda['Ts'][rowswithinrangeTeam]

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
		pltA = plt.plot(X1,Y1,color='red',linestyle='-',label = teamStrings[0]) ##### I should use code below to make it stick to the window
		pltB = plt.plot(X2,Y2,color='blue',linestyle='--',label= teamStrings[1])
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
		plt.legend([pltA[0], pltB[0]], [teamStrings[0],teamStrings[1]])

def plotPerPlayerPerTeam(plotThisAttribute,eventExcerptPanda,rowswithinrangePlayerA,rowswithinrangePlayerB,TeamAstring,TeamBstring,**keyword_param):
	x_attribute = 'Ts'
	if 'x_value' in keyword_param:
		x_attribute = keyword_param['x_value']

	## made a start with plotting dotted line as representing velocity
	# timeWithinRange = ( min(eventExcerptPanda['Ts'][rowswithinrangePlayerA]),max(eventExcerptPanda['Ts'][rowswithinrangePlayerA]) )

	# # select 0.5s periods
	# # take the min value
	# tmin = min(eventExcerptPanda['Ts'][rowswithinrangePlayerA])
	# tmax = max(eventExcerptPanda['Ts'][rowswithinrangePlayerA])
	# tnow = tmin
	# while tmin < tmax:
	# 	tnow = tnow + 0.5 # results in a sampling rate of about 0.5s

	# # find the first value larger than min + 0.5
	# # until you reach the max value
	# print(timeWithinRange)
	# rowswithinrangePlayerA_sparse
	# Y1_sparse

	Y1 = eventExcerptPanda.loc[rowswithinrangePlayerA].pivot(columns='PlayerID',values=plotThisAttribute)
	Y2 = eventExcerptPanda.loc[rowswithinrangePlayerB].pivot(columns='PlayerID',values=plotThisAttribute)
	X1 = eventExcerptPanda.loc[rowswithinrangePlayerA].pivot(columns='PlayerID',values=x_attribute)
	X2 = eventExcerptPanda.loc[rowswithinrangePlayerB].pivot(columns='PlayerID',values=x_attribute)

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
	if Y1.shape[1] < 8:
		rangeMultiplier = 2

	indA = Y1.shape[1]
	startColor = math.floor(refIndA[0] - rangeMultiplier * indA)
	endColor = math.floor(refIndA[0] + rangeMultiplier * indA)
	dC = round((endColor - startColor) / indA)
	colorPickerA = np.arange(startColor,endColor,dC)
	colorPickerA[round(len(colorPickerA)/2)], colorPickerA[-1] = colorPickerA[-1], colorPickerA[round(len(colorPickerA)/2)]

	# Colors B
	refIndB = [i for i,v in enumerate(sorted_names) if v == refColorB]
	if refIndB == []:
		warn('\nWARNING: Specified reference color not found.\nConsider specifying a different color.')

	indB = Y2.shape[1]
	startColor = math.floor(refIndB[0] - rangeMultiplier * indB)
	endColor = math.floor(refIndB[0] + rangeMultiplier * indB)
	dC = round((endColor - startColor) / indB)
	colorPickerB = np.arange(startColor,endColor,dC)
	colorPickerB[round(len(colorPickerB)/2)], colorPickerB[-1] = colorPickerB[-1], colorPickerB[round(len(colorPickerB)/2)]

	for ix,player in enumerate(X1.keys()):
		curColor = sorted_names[colorPickerA[ix]]
		if x_attribute == 'X': # cheeky way to find plotSnapshot
			# Highlight the last point as the current position
			# print(X1[player].notnull()[::-1].idxmax())
			# print(X1[player][X1[player] == max(X1[player])].index)
			plt.plot(X1[player],Y1[player],'s',markersize = 2,color=curColor)

			tmp = X1[player].notnull()[::-1].idxmax()
			pltA = plt.plot(X1[player].loc[tmp],Y1[player].loc[tmp],'s',color=curColor)
		else:
			pltA = plt.plot(X1[player],Y1[player],color=curColor,linestyle='-')


	for ix,player in enumerate(X2.keys()):
		curColor = sorted_names[colorPickerB[ix]]
		if x_attribute == 'X':
			# Highlight the last point as the current position
			# tmp = X2[player].notnull()[::-1].idxmax()
			# tmp =
			plt.plot(X2[player],Y2[player],'o', markersize = 2, color=curColor)

			tmp = X2[player].notnull()[::-1].idxmax()
			pltB = plt.plot(X2[player].loc[tmp],Y2[player].loc[tmp],'o',color=curColor)
		else:
			pltB = plt.plot(X2[player],Y2[player],color=curColor,linestyle='--')

	plt.legend([pltA[0], pltB[0]], [TeamAstring,TeamBstring])
