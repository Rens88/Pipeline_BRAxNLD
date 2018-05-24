# Some problems still with colouring of sets.

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
import scipy as sp
import scipy.stats

imageExtension = ".png"

if __name__ == '__main__':

	# 09-02-2018 Rens Meerhoff
	# The plot new style
	process(plotTheseAttributes,aggregateLevel,eventExcerptPanda,attributeLabel,tmpFigFolder,fname,debuggingMode)

def process(plotTheseAttributes,aggregateLevel,eventExcerptPanda,attributeLabel,tmpFigFolder,fname,debuggingMode,**kwargs):

	tDatasetVisualization = time.time()	# do stuff

	LPvsNP = False
	if 'LPvsNP' in kwargs:
		LPvsNP = kwargs['LPvsNP']

	xLabel = attributeLabel['Ts']

	if not 'temporalAggregate' in eventExcerptPanda.keys():
		# No events detected
		warn('\nWARNING: No temporalAggregate detected. \nCouldnt plot any data.\nUse <Random> to create random events, or <full> to plot the whole file.\n')
		if debuggingMode:
			elapsed = str(round(time.time() - tDatasetVisualization, 2))
			print('***** Time elapsed during datasetVisualization: %ss' %elapsed)
		return

	tmp = eventExcerptPanda.loc[eventExcerptPanda['PlayerID'] == 'groupRow']
	rowswithinrangeTeamA_groupRows = tmp.index
	rowswithinrangeTeamB_groupRows = tmp.index

	tmp = eventExcerptPanda.loc[eventExcerptPanda['TeamID'] == eventExcerptPanda['RefTeam']]#.index
	rowswithinrangePlayer_ref = tmp.index
	refTeamString = 'refTeam' #eventExcerptPanda['RefTeam'][0]

	tmp = eventExcerptPanda.loc[eventExcerptPanda['TeamID'] == eventExcerptPanda['OthTeam']]#.index
	rowswithinrangePlayer_oth = tmp.index
	othTeamString = 'othTeam'#eventExcerptPanda['OthTeam'][0]

	fileAggregateID = 'window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')'


	if LPvsNP:
		## An attempt for NP
		tmp = eventExcerptPanda.loc[eventExcerptPanda['Exp'] == 'NP']# and eventExcerptPanda['PlayerID'] == 'groupRow']
		tmp = tmp.loc[tmp['PlayerID'] == 'groupRow']

		rowswithinrangeTeamA_groupRows = tmp.index
		refTeamString = 'NP'

		tmp = eventExcerptPanda.loc[eventExcerptPanda['Exp'] == 'LP']# and eventExcerptPanda['PlayerID'] == 'groupRow']
		tmp = tmp.loc[tmp['PlayerID'] == 'groupRow']
		rowswithinrangeTeamB_groupRows = tmp.index
		othTeamString = 'LP'

		fileAggregateID = 'window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')_LPvsNP'


	for plotThisAttribute in plotTheseAttributes:

		plt.figure(num=None, figsize=(3.8*5,3*5), dpi=300, facecolor='w', edgecolor='k')

		if type(plotThisAttribute) == tuple: # pairwise comparison of team

			##################
			if LPvsNP:
				plotThisAttribute = (plotThisAttribute[0],plotThisAttribute[0])
			############

			# Pairwise per team
			yLabel = findYlabel(plotThisAttribute,attributeLabel,refTeamString,othTeamString)

			# Plot it
			plotPerTeamPerEvent(plotThisAttribute,eventExcerptPanda,rowswithinrangeTeamA_groupRows,rowswithinrangeTeamB_groupRows,refTeamString,othTeamString)

			varName = plotThisAttribute[0]
			if plotThisAttribute[0][-1] == 'A' or plotThisAttribute[0][-1] == 'B':
				varName = varName[:-1]
			if plotThisAttribute[0][-4:] == '_oth' or plotThisAttribute[0][-4:] == '_ref':
				varName = varName[:-4]
			outputFilename = tmpFigFolder + fname + '_' + varName + '_' + fileAggregateID + imageExtension
		else:
			if LPvsNP:
				continue
			# # Plot it
			plotPerPlayerPerTeam(plotThisAttribute,eventExcerptPanda,rowswithinrangePlayer_ref,rowswithinrangePlayer_oth,refTeamString,othTeamString)
			# plotPerPlayerPerTeam(plotThisAttribute,currentEvent,rowswithinrangePlayer_ref,rowswithinrangePlayer_oth,refTeamString,othTeamString)
			labelProvided = [True for j in attributeLabel.keys() if plotThisAttribute == j]
			if labelProvided:
				yLabel = attributeLabel[plotThisAttribute][0] # because here it's a pandaaaa
			else:
				yLabel = 'Unknown'

			outputFilename = tmpFigFolder + fname + '_' + plotThisAttribute + '_' + fileAggregateID + imageExtension

		plt.title(fileAggregateID)
		plt.xlabel(xLabel)
		plt.ylabel(yLabel)
		plt.savefig(outputFilename, figsize=(1,1), dpi = 300, bbox_inches='tight')
		print('EXPORTED: <%s>' %outputFilename)
		plt.close()

	if debuggingMode:
		elapsed = str(round(time.time() - tDatasetVisualization, 2))
		print('***** Time elapsed during datasetVisualization: %ss' %elapsed)
	return

def findYlabel(plotThisAttribute,attributeLabel,refTeamString,othTeamString):

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
		if refTeamString in i:
			ofrefTeamString = ' of %s' %refTeamString
			if ofrefTeamString in i:
				i = i.replace(ofrefTeamString,'')
			else:
				i = i.replace(refTeamString,'both teams')

		elif othTeamString in i:
			ofothTeamString = ' of %s' %othTeamString
			if ofothTeamString in i:
				i = i.replace(ofothTeamString,'')
			else:
				i = i.replace(othTeamString,'both teams')
		elif 'refTeam' in i:
			ofRefString = ' of refTeam'
			if ofRefString in i:
				i = i.replace(ofRefString,'')
			else:
				i = i.replace(ofRefString,'both teams')
		elif 'othTeam' in i:
			ofOthString = ' of othTeam'
			if ofOthString in i:
				i = i.replace(ofOthString,'')
			else:
				i = i.replace(ofOthString,'both teams')

		tmp.append(i)

	yLabel = tmp[0]

	if tmp[0] != tmp[1]:
		# To do: find a way to generalize labels (i.e., take out team bit)
		warn('\nWARNING: y-labels not identical:\n<%s>\nand\n<%s>' %(tmp[0],tmp[1]))

	return yLabel

def plotPerPlayerPerTeam(plotThisAttribute,eventExcerptPanda,rowswithinrangePlayer_ref,rowswithinrangePlayer_oth,refTeamString,othTeamString):

	Y1 = eventExcerptPanda.loc[rowswithinrangePlayer_ref].pivot(columns='EventUID',values=plotThisAttribute)
	Players_Y1 = eventExcerptPanda.loc[rowswithinrangePlayer_ref].pivot(columns='EventUID',values='PlayerID')
	Y2 = eventExcerptPanda.loc[rowswithinrangePlayer_oth].pivot(columns='EventUID',values=plotThisAttribute)
	Players_Y2 = eventExcerptPanda.loc[rowswithinrangePlayer_oth].pivot(columns='EventUID',values='PlayerID')
	X1 = eventExcerptPanda.loc[rowswithinrangePlayer_ref].pivot(columns='EventUID',values='eventTime')
	X2 = eventExcerptPanda.loc[rowswithinrangePlayer_oth].pivot(columns='EventUID',values='eventTime')

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

	for ix,event in enumerate(Y1.keys()):
		curColorA = sorted_names[colorPickerA[ix]]
		curColorB = sorted_names[colorPickerB[ix]]

		for player in pd.unique(Players_Y1[event]):
			PlotThisX = X1.loc[Players_Y1[event] == player,event]
			PlotThisY = Y1.loc[Players_Y1[event] == player,event]
			pltA = plt.plot(PlotThisX,PlotThisY,color=curColorA,linestyle='-')

		for player in pd.unique(Players_Y2[event]):
			PlotThisX = X2.loc[Players_Y2[event] == player,event]
			PlotThisY = Y2.loc[Players_Y2[event] == player,event]

			pltB = plt.plot(PlotThisX,PlotThisY,color=curColorB,linestyle='--')

	plt.legend([pltA[0], pltB[0]], [refTeamString,othTeamString])



def plotPerTeamPerEvent(plotThisAttribute,eventExcerptPanda,rowswithinrangeTeamA,rowswithinrangeTeamB,refTeamString,othTeamString):

	# Ideally, I'd use the actual 'eventTime', but because export to csv always creates floating precision, this doesn't work.
	# eventTimeIndex is reliable, but only AFTER interpoloation (using dataset level frame rate)
	# Y1 = eventExcerptPanda.loc[rowswithinrangeTeam].pivot(columns='EventUID',values=plotThisAttribute[0],index = 'eventTime')

	Y1 = eventExcerptPanda.loc[rowswithinrangeTeamA].pivot(columns='EventUID',values=plotThisAttribute[0],index = 'eventTimeIndex')
	Y2 = eventExcerptPanda.loc[rowswithinrangeTeamB].pivot(columns='EventUID',values=plotThisAttribute[1],index = 'eventTimeIndex')
	X1 = eventExcerptPanda.loc[rowswithinrangeTeamA].pivot(columns='EventUID',values='eventTime',index = 'eventTimeIndex')
	X2 = eventExcerptPanda.loc[rowswithinrangeTeamB].pivot(columns='EventUID',values='eventTime',index = 'eventTimeIndex')

	# X_int = X1

	# avgY1 = np.nanmean(Y1, axis = 1)
	# avgY2 = np.nanmean(Y2, axis = 1)

	avgY1,lo95Y1,hi95Y1 = mean_confidence_interval(Y1)
	avgY2,lo95Y2,hi95Y2 = mean_confidence_interval(Y2)

	stdY1 = np.nanstd(Y1, axis = 1)
	stdY2 = np.nanstd(Y2, axis = 1)

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

	# #########################
	# for ix,event in enumerate(Y1.keys()):

	# 	curColorA = sorted_names[colorPickerA[ix]]
	# 	curColorB = sorted_names[colorPickerB[ix]]

	# 	pltA = plt.plot(X_int[event],Y1[event],color=curColorA,linestyle='-')
	# 	pltB = plt.plot(X_int[event],Y2[event],color=curColorB,linestyle='--')

	# #########################

	#########################
	ax = plt.gca()
	ax.fill_between(X1[Y1.keys()[0]], lo95Y1, hi95Y1, where=hi95Y1 >= lo95Y1, facecolor = refColorA, interpolate=True,alpha=0.4)
	ax.fill_between(X2[Y2.keys()[0]], lo95Y2, hi95Y2, where=hi95Y2 >= lo95Y2, facecolor = refColorB, interpolate=True,alpha=0.4)

	for ix,event1 in enumerate(Y1.keys()):
		curColorA = sorted_names[colorPickerA[ix]]
		pltA = plt.plot(X1[event1],Y1[event1],color=curColorA,linestyle='-')

	for ix,event2 in enumerate(Y2.keys()):
		curColorB = sorted_names[colorPickerB[ix]]
		pltB = plt.plot(X2[event2],Y2[event2],color=curColorB,linestyle='--')

	#########################
	pltAvgA = plt.plot(X1[event1],avgY1, color = refColorA, linestyle = '-', linewidth = 4)
	pltAvgB = plt.plot(X2[event2],avgY2, color = refColorB, linestyle = '--', linewidth = 4)
	# plt.legend([pltA[0], pltB[0]], [refTeamString,othTeamString])

	# print(Y1.shape[1])
	lgString1 = refTeamString + ' (n = %s)'  %Y1.shape[1]
	lgString2 = othTeamString + ' (n = %s)'  %Y2.shape[1]

	plt.legend([pltAvgA[0], pltAvgB[0]], [lgString1,lgString2])

def mean_confidence_interval(data, confidence=0.95):
	a = 1.0*np.array(data)
	n = len(a)
	m = np.nanmean(a, axis = 1)

	se = []
	for i in np.arange(0,a.shape[0]):

		a_not_nan = ~np.isnan(a[i,:])
		if not all(a_not_nan):
			warn('\nWARNING: Not every event had data at every timepoint.\nI now adjusted the confidence interval to only be based on the data points available for that timepoint.\nI may want to exclude listwise, in which case I can only compute a confidence interval when all events have that datapoint.\nAt the very least, I should consider including a threshold.\n')
		se.append(scipy.stats.sem(a[i,a_not_nan]))

	se = 1.0*np.array(se)

	# m, se = np.nanmean(a, axis = 1), scipy.stats.sem(a, axis = 1)
	h = se * sp.stats.t._ppf((1+confidence)/2., n-1)
	return m, m-h, m+h
