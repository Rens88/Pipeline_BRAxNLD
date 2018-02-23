# 09-02-2018 Rens Meerhoff
# In process. Continue with PairwisePerTeam2() 
# --> which will be edited to automatically identify whether a variable is a team or individual variable.
#
# 30-11-2017 Rens Meerhoff
# Function to plot timeseries variables.
#
# vNorm => normalized velocity

import pdb; #pdb.set_trace()
from warnings import warn
import numpy as np
from os.path import isfile, join, isdir, exists
from os import listdir, makedirs
import CSVexcerpt
import matplotlib.pyplot as plt
from matplotlib import colors as mcolors


if __name__ == '__main__':		
	# tmin = first time occurring (for title and filename only)
	# tmax = last time occurring (for title and filename only)
	# inds* = indices per player corresponding to desired timeframe (from individualAttributes.PlayerInds)
	# X* = typically time
	# Y* = dependent variable
	# * ==> input that needs to be given separately for each set that is compared
	# xLabel = string for plotting x-label
	# yLabel = string for plotting y-label
	# tmpFigFolder = folder where figure should be saved
	PairwisePerPlayer(tmin,tmax,inds1,X1,Y1,inds2,X2,Y2,xLabel,yLabel,tmpFigFolder) # --> edit for two different datasets
	PerPlayer(tmin,tmax,inds1,X1,Y1,xLabel,yLabel,tmpFigFolder,stringOut) # --> edit for two different datasets
	PairwisePerTeam(tmin,tmax,X,Y1,Y2,xLabel,yLabel,tmpFigFolder,stringOut,rawDict,attributeDict) # along the same dimension

	# 09-02-2018 Rens Meerhoff
	# The plot new style
	PairwisePerTeam2(printTheseAttributes,aggregateLevel,targetEvents,rawDict,attributeDict,attributeLabel,tmpFigFolder,fname)

def PerPlayer(tmin,tmax,inds1,X1,Y1,xLabel,yLabel,tmpFigFolder,stringOut):

	plt.figure(num=None, figsize=(3.8,3), dpi=300, facecolor='w', edgecolor='k')
	titleString = tmin + ' to ' + tmax + 'ms'
	plt.title(titleString)

	plt.xlabel(xLabel)
	plt.ylabel(yLabel)

	# Sort colors by hue, saturation, value and name.
	colors = dict(mcolors.BASE_COLORS, **mcolors.CSS4_COLORS)
	by_hsv = sorted((tuple(mcolors.rgb_to_hsv(mcolors.to_rgba(color)[:3])), name)
	                for name, color in colors.items())
	sorted_names = [name for hsv, name in by_hsv]
	colorPicker = np.arange(0,len(sorted_names),round(len(sorted_names)/len(inds1)))

	for idx in range(len(inds1)): # Do this for every player
		# Repeat this for any variable you want to plot
		XtoPlot = X1[list(range(inds1[idx][1],inds1[idx][2],1))] # TsS --> Time in seconds # Could be outside for loop
		YtoPlot = Y1[list(range(inds1[idx][1],inds1[idx][2],1))] # TsS --> Time in seconds # Could be outside for loop
		plt.plot(XtoPlot,YtoPlot,color=sorted_names[colorPicker[idx]],linestyle='-')
		# plt.plot(XtoPlot,YtoPlot,color=sorted_names[colorPicker[idx]],linestyle=':')

	outputFilename = tmpFigFolder + 'Timeseries' + stringOut + '_' + titleString + '.jpg'

	plt.savefig(outputFilename, figsize=(1,1), dpi = 300, bbox_inches='tight')
	plt.close()
	# print('\n---\nFigure saved as:\n%s\n---\n' %outputFilename)

def PairwisePerPlayer(tmin,tmax,inds1,X1,Y1,inds2,X2,Y2,xLabel,yLabel,tmpFigFolder):
	plt.figure(num=None, figsize=(3.8,3), dpi=300, facecolor='w', edgecolor='k')
	titleString = tmin + ' to ' + tmax + 'ms'
	plt.title(titleString)

	plt.xlabel(xLabel)
	plt.ylabel(yLabel)

	# Sort colors by hue, saturation, value and name.
	colors = dict(mcolors.BASE_COLORS, **mcolors.CSS4_COLORS)
	by_hsv = sorted((tuple(mcolors.rgb_to_hsv(mcolors.to_rgba(color)[:3])), name)
	                for name, color in colors.items())
	sorted_names = [name for hsv, name in by_hsv]
	colorPicker = np.arange(0,len(sorted_names),round(len(sorted_names)/len(inds1)))

	for idx in range(len(inds1)): # Do this for every player
		# Repeat this for any variable you want to plot
		XtoPlot = X1[list(range(inds1[idx][1],inds1[idx][2],1))] # TsS --> Time in seconds # Could be outside for loop
		YtoPlot = Y1[list(range(inds1[idx][1],inds1[idx][2],1))] # TsS --> Time in seconds # Could be outside for loop
		plt.plot(XtoPlot,YtoPlot,color=sorted_names[colorPicker[idx]],linestyle='-')
	
	for idx in range(len(inds2)): # Do this for every player		
		XtoPlot = X2[list(range(inds2[idx][1],inds2[idx][2],1))] # TsS --> Time in seconds # Could be outside for loop
		YtoPlot = Y2[list(range(inds2[idx][1],inds2[idx][2],1))] # TsS --> Time in seconds # Could be outside for loop
		plt.plot(XtoPlot,YtoPlot,color=sorted_names[colorPicker[idx]],linestyle=':')

	outputFilename = tmpFigFolder + 'Timeseries_vNormXSpeed_' + titleString + '.jpg'
	plt.savefig(outputFilename, figsize=(1,1), dpi = 300, bbox_inches='tight')
	plt.close()
	# print('\n---\nFigure saved as:\n%s\n---\n' %outputFilename)

# def PairwisePerTeam(tmin,tmax,inds1,X1,Y1,inds2,X2,Y2,xLabel,yLabel,tmpFigFolder):
def PairwisePerTeam2(printTheseAttributes,aggregateLevel,targetEvents,rawDict,attributeDict,attributeLabel,tmpFigFolder,fname):
	# Idea: rawDict only used for TsS. Perhaps it's more efficient to add it to attributeDict as well?
	# Idea: include a further specification of selecting which variables and which events to plot

	xLabel = 'Time (s)'
	########################################	
	# Copied from temporalAggregation.py (start)
	########################################	
	for idx,currentEvent in enumerate(targetEvents[aggregateLevel[0]]):
		# Create the output string that identifies the current event
		aggregateString = '%03d_%s' %(idx,aggregateLevel[0])	
		
		if type(targetEvents[aggregateLevel[0]]) != list:			
			currentEvent = targetEvents[aggregateLevel[0]]
			fileAggregateID = aggregateString + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')'
		if aggregateLevel[0] == 'Possession' or aggregateLevel[0] == 'Full':
			tStart = currentEvent[0]
			tEnd = currentEvent[1]
			fileAggregateID = aggregateString + '_' 'window(all)_lag(none)'
		else:
			tEnd = currentEvent[0] - aggregateLevel[2]
			tStart = tEnd - aggregateLevel[1]
			if aggregateLevel[0] != 'Possession' or aggregateLevel[0] != 'Full':
				warn('\nCode not yet adjusted to have an unspecified period for any other event than <Possession> and <Full>.\nMay lead to an error.\n')
			fileAggregateID = aggregateString + '_window(' + str(aggregateLevel[1]) + ')_lag(' + str(aggregateLevel[2]) + ')'

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
		########################################	
		# Copied from temporalAggregation.py (end)
		########################################	
		
		for i in printTheseAttributes:
			# NB: Will now give an error when only asking for one plot. 
			# TO DO: allow for single variable plot

		
			if type(i) == tuple: # pairwise comparison of team
				for itmp in [0,1]:
					labelProvided = [True for j in attributeLabel.keys() if i[itmp] == j]
					if labelProvided:
						tmp[itmp] = attributeLabel[i[itmp]] # take the label as provided
					else:
						tmp[itmp] = 'Unknown'
						warn('\nWARNING: y-label not specified.\nPlease provide y-label in <attributeLabel> for <%s>.' %i[itmp])
				yLabel = tmp[0]
				if tmp[0] != tmp[1]:
					warn('\nWARNING: y-labels not identical:\n<%s>\nand\n<%s>' %(tmp[0],tmp[1]))

				Y1 = [attributeDict[i[0]][ind] for ind in rowswithinrangeTeam]
				Y2 = [attributeDict[i[1]][ind] for ind in rowswithinrangeTeam]
			else:
				do_this = []
				# - plot single value (could be one team, could be both teams as one, one player etc.)
				# - plot individual values (e.g., vNorm) --> plot all individuals with different colour

			X = TsS[rowswithinrangeTeam]
									
			titleString = fileAggregateID
			outputFilename = tmpFigFolder + fname + '_' + i[0] + '_' + fileAggregateID + '.jpg'

			plt.figure(num=None, figsize=(3.8,3), dpi=300, facecolor='w', edgecolor='k')
			plt.title(titleString)

			plt.xlabel(xLabel)
			plt.ylabel(yLabel)

			plt.plot(X,Y1,color='red',linestyle='-') ##### I should use code below to make it stick to the window
			plt.plot(X,Y2,color='blue',linestyle='--')
			# To do:
			# - add a legend including TeamAstring etc.

			plt.savefig(outputFilename, figsize=(1,1), dpi = 300, bbox_inches='tight')
			plt.close()

def PairwisePerTeam(tmin,tmax,X,Y1,Y2,xLabel,yLabel,tmpFigFolder,stringOut,rawDict,attributeDict): # along the same dimension
	
	# Quick fix, only works for NP-data. 
	# Actual solution lies in looking at changes in dt under the same player.
	runs = np.array([i for i,val in enumerate(attributeDict['Run']) if val  != '' ])
	runTimes = rawDict['Time']['TsS'][runs]
	for val in runTimes: # for every run time, make vNorm 0
		for i,val2 in enumerate(rawDict['Time']['TsS']):
			if val2 == val:
				Y1[i] = 0 # artificially set to zero because time is not continuous in NP data
				Y2[i] = 0				


	plt.figure(num=None, figsize=(3.8,3), dpi=300, facecolor='w', edgecolor='k')
	titleString = tmin + ' to ' + tmax + 'ms'
	plt.title(titleString)

	plt.xlabel(xLabel)
	plt.ylabel(yLabel)

	plt.plot(X,Y1,color='red',linestyle='-') ##### I should use code below to make it stick to the window
	plt.plot(X,Y2,color='blue',linestyle='--')	


	# for idx in range(len(inds1)): # Do this for every player
	# 	# Repeat this for any variable you want to plot
	# 	XtoPlot = X1[list(range(inds1[idx][1],inds1[idx][2],1))] # TsS --> Time in seconds # Could be outside for loop
	# 	YtoPlot = Y1[list(range(inds1[idx][1],inds1[idx][2],1))] # TsS --> Time in seconds # Could be outside for loop
	# 	plt.plot(XtoPlot,YtoPlot,color=sorted_names[colorPicker[idx]],linestyle='-')
	
	# for idx in range(len(inds2)): # Do this for every player		
	# 	XtoPlot = X2[list(range(inds2[idx][1],inds2[idx][2],1))] # TsS --> Time in seconds # Could be outside for loop
	# 	YtoPlot = Y2[list(range(inds2[idx][1],inds2[idx][2],1))] # TsS --> Time in seconds # Could be outside for loop
	# 	plt.plot(XtoPlot,YtoPlot,color=sorted_names[colorPicker[idx]],linestyle=':')

	outputFilename = tmpFigFolder + 'Timeseries' + stringOut + '_' + titleString + '.jpg'
	plt.savefig(outputFilename, figsize=(1,1), dpi = 300, bbox_inches='tight')
	plt.close()
	# print('\n---\nFigure saved as:\n%s\n---\n' %outputFilename)