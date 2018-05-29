# 13-11-2017, Rens Meerhoff
# This script can be used to plot a snapshot of a football match based on LPM data
import csv
import numpy as np
from numpy import *
import scipy.linalg
from scipy.interpolate import InterpolatedUnivariateSpline
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.lines as mlines
import pdb; #pdb.set_trace()
from os import listdir, makedirs
from os.path import isfile, join, isdir 
from warnings import warn

# import CSVimportAsColumns
# import identifyDuplHeader

if __name__ == '__main__':
	# ############
	# # USER INPUT
	# ############
	# filename specifies the dataset (with our without .csv), always call with filename before excerpt (this function will look for the best excerpt)
	# curFrame specifies which Frame (in ms) is plotted
	execFunction(folder,filename,curFrame)

def execFunction(folder,filename,curFrame):
	# optional user input
	timeUnit = 1000 # unit of time as given in dataset in Hertz (i.e., 1000 = milliseconds)
	frameRate = 10 # in Hertz (depends on data input)
	traceWindow = 1 # in seconds
	
	traceWindowFrames = traceWindow * frameRate
	dtCorrection = timeUnit / frameRate
	filenameWholeDataset = filename

	# omit '.csv' if necessary
	if filename[-4:] == '.csv':
	    # print('WARNING: The extension CSV was automatically added to the original filename')
	    filename = filename[:-4]	    

	# Field parameters
	x0 = 0
	x1 = -50 
	x2 = 50
	y0 = 0.1
	y1 = -32.5
	y2 = 32.7
	# NB: Parameters need to be corrected to meters

	# Prepare some variables 
	tmin = []
	firstFrame = []
	excerpt = 'False'
	curFrame = str(curFrame)

	# Before loading full dataset, check if there is an excerpt that includes curFrame (saves time)
	mypath = 'C:\\Users\\rensm\\Documents\\PostdocLeiden\\wetransfer-fc4772\\CSVexcerpts\\'
	if isdir(mypath):
		onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
		for idx, val in enumerate(onlyfiles):
			idme = [] # identity of string with value 'm' which is the end of the window
			idms = [] # start of the window based on identify of string 'm'
			idms.append(len(filename)+1)
			if filename == val[0:len(filename)]: # if filenames are the same
				# look for the letter 'm'
				for i, f in enumerate(val[len(filename)+1:]):
					if f == 'm':
						idme.append(len(filename) + 1 + i )
				idms.append(idme[0] + 3)
				tmin = val[idms[0]:idme[0]]
				tmax = val[idms[1]:idme[1]]
				if int(curFrame) - traceWindowFrames >= int(tmin) and int(curFrame) + traceWindowFrames <= int(tmax):
					# Ideally, there's a file that includes the trace as well (the 'perfect' excerpt)
					newFilename = mypath + val
					firstFrame = tmin
					lastFrame = tmax
					excerpt = 'True'
					break
				elif int(curFrame) >= int(tmin) and int(curFrame) <= int(tmax):
					# Otherwise, it's good enough to go with a file that just includes the curFrame
					newFilename = mypath + val
					firstFrame = tmin
					lastFrame = tmax
					excerpt = 'True'
					# Just in case there is a 'perfect' excerpt, don't automatically break here					
				# IDEA: Could include a break function if you want to limit going through files
				
	else:
		warn('\nWARNING: Couldnt find excerpts; create excerpts to improve speed\n')
	if excerpt == 'False':
		warn('\nWARNING: Consider creating excerpt to improve speed\n')
		filename = folder + filename
	else:
		# Overwrite filename here.
		filename = newFilename

	# Add csv to filename
	if filename[-4:] != '.csv':
	    # print('WARNING: The extension CSV was automatically added to the original filename')
	    filename = filename + '.csv'
	# Import data
	cols2read = ['Timestamp','Timestamp','X','Y','Name','Name']
	data = CSVimportAsColumns.readPosData(filename,cols2read)
	dataCols = identifyDuplHeader.idTS(data,cols2read,filename)
	dataCols = identifyDuplHeader.idName(data,dataCols)
	for idx,val in enumerate(dataCols):
		if val == 'TsMS':
			TsMS = data[idx]
		elif val == 'X':
			X = np.empty([len(data[idx]),1],dtype=np.float64)
			for ind,i in enumerate(data[idx]):
				# print(i)
				if  not i == '':
					X[ind] = np.float64(i)
				else: # missing data (empty cell to be precise)					
					X[ind] = np.nan

			# X = np.array([])
			# for i in data[idx]: # COSTLY
			# 	try:
			# 		X = np.append(X,float(i))
			# 	except ValueError:
			# 		# Missing value
			# 		X = np.append(X,np.nan)
		elif val == 'Y':
			Y = np.empty([len(data[idx]),1],dtype=np.float64)
			for ind,i in enumerate(data[idx]):
				if  not i == '':
					Y[ind] = np.float64(i)
				else: # missing data					
					Y[ind] = np.nan
		elif val == 'TeamID': # This should coincidentally work because we're after the seconde 'name'
			Team = data[idx]

	if excerpt == 'False':
		firstFrame = int(curFrame) - traceWindow * timeUnit	
		lastFrame = int(curFrame) + traceWindow * timeUnit	
		if firstFrame < float(TsMS[0]):
			firstFrame = TsMS[0]
		if lastFrame > float(TsMS[-1]):
			lastFrame = TsMS[-1]
		firstFrame = str(firstFrame)
		lastFrame = str(lastFrame)
	if tmin == []:
		tmin = TsMS[0]
	if firstFrame == []:		
		firstFrame	= tmin

	##################################################
	# 001 Indexing a list and access existing variables
	##################################################

	ind = []
	XindA = []
	YindA = []
	XindB = []
	YindB = []


	for i in range(len(TsMS)):  	# For every ith time stamp in milliseconds
		if int(TsMS[i]) == int(curFrame):		# Look for all TsMS equal to curFrame (i.e., every instance of curFrame)
			ind.append(i)			# Put this ith index in ind
			if Team[i] == 'Team1': # player Team A
				XindA.append(float(X[i]))
				YindA.append(float(Y[i]))
			elif Team[i] == 'Team2': # player Team B
				XindB.append(float(X[i]))
				YindB.append(float(Y[i]))
			elif Team[i] == 'Team A': # Team A
				doNothing = []
			elif Team[i] == 'Team B': # Team B
				doNothing = []
			else:
				warn('\nWARNING: could not identify Team\n')

	###########################################################################
	# Accessing data at this index to:
	# 002 Plot existing variables
	###########################################################################
	# Prepare plot
	plt.figure(num=None, figsize=(3.8,3), dpi=300, facecolor='w', edgecolor='k')
	fig, ax = plt.subplots() # note we must use plt.subplots, not plt.subplot

	# Plot the field
	plt.plot([x1, x2, x2, x1, x1], [y1, y1, y2, y2, y1], color='k', linestyle='-', linewidth=1) # Field outline
	plt.plot([x0, x0],[y1, y2], color='k', linestyle='-', linewidth=1) # middle line
	MidCircle = plt.Circle((x0,y0), 9.15, color='k', fill=False, linewidth=1) # middle circle
	plt.plot([x1,x1+16.5,x1+16.5,x1], [y0-20,y0-20,y0+20,y0+20], color='k', linestyle='-', linewidth=1) # penalty box
	plt.plot([x2,x2-16.5,x2-16.5,x2], [y0-20,y0-20,y0+20,y0+20], color='k', linestyle='-', linewidth=1) # penalty box
	plt.plot([x1+11,x2-11],[y0,y0],".", color = 'k', markersize=2) # penalty mark
	plt.plot([x1,x1+5.5,x1+5.5,x1], [y0-9.12,y0-9.12,y0+9.12,y0+9.12], color='k', linestyle='-', linewidth=1) # keeper box
	plt.plot([x2,x2-5.5,x2-5.5,x2], [y0-9.12,y0-9.12,y0+9.12,y0+9.12], color='k', linestyle='-', linewidth=1) # keeper box
	ax.add_patch(patches.Arc((x1+11,y0), 9.15*2,9.15*2, 0,127+180,233+180)) # stupid semi-circle
	ax.add_patch(patches.Arc((x2-11,y0), 9.15*2,9.15*2, 0,127,233)) # stupid semi-circle
	plt.plot([x1,x1-.5,x1-.5,x1], [y0-3.62,y0-3.62,y0+3.62,y0+3.62], color='k', linestyle='-', linewidth=1) # goal
	plt.plot([x2,x2+.5,x2+.5,x2], [y0-3.62,y0-3.62,y0+3.62,y0+3.62], color='k', linestyle='-', linewidth=1) # goal
	ax.add_artist(MidCircle)
	plt.plot(x0,y0,".", color = 'k', markersize=3) # centre spot

	plt.xlabel('X-position (m)')
	plt.ylabel('Y-position (m)')
	plt.title(str(TsMS[ind[0]]) + ' ms')

	# Plot positie data
	PlotA = plt.plot(XindA,YindA,".", color = 'r')
	PlotB = plt.plot(XindB,YindB,".", color = 'b')

	# Trace up until curFrame (both teams black)
	dtCorrection = timeUnit / frameRate
	dtPast = int((int(curFrame) - int(firstFrame)) / dtCorrection) # 100 to convert ms into frames: (1000ms / frameRate) where frameRate = 10Hz
	if dtPast < traceWindowFrames:
		# Only plot trace from firstframe onward
		for i in ind:
			## Working solution, but should not be necessary now that missing data are replaced with nan
			# if X[i-dtPast] == '':
			# 	warn('\nPlotting sequence terminated early because of empty cell\n')
			# 	break
			plt.plot(X[i-dtPast:i],Y[i-dtPast:i],color = 'k',linewidth=.5)
	else:
		# Plot trace from 10 frames before firstFrame
		for i in ind:
			## Working solution, but should not be necessary now that missing data are replaced with nan
			# if X[i-traceWindowFrames] == '':
			# 	warn('\nPlotting sequence terminated early because of empty cell\n')
			# 	break
			plt.plot(X[i-traceWindowFrames:i],Y[i-traceWindowFrames:i],color = 'k',linewidth=.5)

	# Trace future (both teams black)	
	dtFut = int((int(lastFrame) - int(curFrame)) / dtCorrection) # 100 to convert ms into frames: (1000ms / frameRate) where frameRate = 10Hz
	if dtFut < traceWindowFrames:
		# Only plot trace until last frame
		for i in ind:
			## Working solution, but should not be necessary now that missing data are replaced with nan
			# if X[i+dtFut] == '':
			# 	warn('\nPlotting sequence terminated early because of empty cell\n')
			# 	break
			plt.plot(X[i:i+dtFut],Y[i:i+dtFut],color = 'k',linewidth=.5,linestyle = ':')
	else:
		# Plot trace until 10 frames after curFrame
		for i in ind:
			## Working solution, but should not be necessary now that missing data are replaced with nan
			# if X[i+traceWindowFrames] == '':
			# 	warn('\nPlotting sequence terminated early because of empty cell\n')
			# 	break
			plt.plot(X[i:i+traceWindowFrames],Y[i:i+traceWindowFrames],color = 'k',linewidth=.5,linestyle = ':')

	# Create legend
	
	red_line = mlines.Line2D([], [], color='r', marker='.',markersize=15, label='Team1')
	blue_line = mlines.Line2D([], [], color='b', marker='.',markersize=15, label='Team2')
	plt.legend(handles=[red_line,blue_line])



	# Save figure
	# Check if folder exists (otherwise make it)
	
	if not folder[-10:]=='Figs\\Temp\\':
		figTmpFolder = folder + 'Figs\\Temp\\' + 'Snapshots_'+ filenameWholeDataset + '\\'
	else:
		figTmpFolder = folder + 'Snapshots_'+ filenameWholeDataset + '\\'
	if not isdir(figTmpFolder):
	    makedirs(figTmpFolder)

	plt.savefig(figTmpFolder + 'SnapshotPositions_' + TsMS[ind[1]] + 'ms' + '.jpg', figsize=(1,1), dpi = 300, bbox_inches='tight')
	plt.close()

	# To avoid warnings, you could repeat
	# plt.close()
	# However, that's a bit slower. 
	# I guess that I still don't really understand what I'm closing...
