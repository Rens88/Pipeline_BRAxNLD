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
	if not exists(tmpFigFolder):
		makedirs(tmpFigFolder)

	plt.savefig(outputFilename, figsize=(1,1), dpi = 300, bbox_inches='tight')
	plt.close()
	print('\n---\nFigure saved as:\n%s\n---\n' %outputFilename)

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
	print('\n---\nFigure saved as:\n%s\n---\n' %outputFilename)

# def PairwisePerTeam(tmin,tmax,inds1,X1,Y1,inds2,X2,Y2,xLabel,yLabel,tmpFigFolder):
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
	print('\n---\nFigure saved as:\n%s\n---\n' %outputFilename)