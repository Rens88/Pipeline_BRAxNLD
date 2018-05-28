# 07-03-2018
# After cleanupData.py, the rawData should have the labels:
#  'Ts' --> Timestamp
#  'X' --> X-position
#  'Y' --> Y-position
#  'PlayerID' --> Player identification. NB: Ball-rows should be 'ball' and Match-rows should be 'groupRow' (to indicate CentroidTeamA)
#  'TeamID' --> Team idenfitification
#
# 06-03-2018 Rens Meerhoff
# Made the function more generic.
# Should still include funcationality that was previously embedded in:
# - importTimeseriesData.py
# - dataToDict2.py
#
# --> which includes making all rawdata headers have a fixed unit of measurement.

# 29-01-2017 Rens Meerhoff
# This function pre-processes the data by allocating a systematic filename, discarding empty rows 
# and columns (at the end of a file only), and it does a quick scan of the consistency of string inputs.

import pdb; #pdb.set_trace()
import csv
from warnings import warn
import numpy as np
from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs
import re
import pandas as pd
import student_LT_cleanUp
import time
import FillGaps_and_Filter
import math
import matplotlib.pyplot as plt

if __name__ == '__main__':

	process(dirtyFname,cleanFname,dataType,dataFolder,cleanedFolder,TeamAstring,TeamBstring,headers,readAttributeCols)	
	# Clean up based on to be expected characteristics of the dataset.
	# Generic:
	omitRowsWithout_XandY(df,x,y)
	omitRowsWithExtreme_XandY(df,x,y,expectedVals)	
	verifyTimestampConsistency()

	# Database specific:
	# Football Data Project --> .csv from inmotio "SpecialExport.csv"
	FDP(DirtyDataFiles,cleanFname,dataFolder,cleanedFolder,headers,readAttributeCols)
	splitName_and_Team(df,headers,ID,newPlayerIDstring,newTeamIDstring)
	omitXandY_equals0(df,x,y,ID)

	# Nonlinear Pedagogy
	NP(dataFiles,cleanFname,folder,cleanedFolder,TeamAstring,TeamBstring)

#########################################################################
def process(dirtyFname,cleanFname,dataType,dataFolder,cleanedFolder,spatAggFname,spatAggFolder,eventAggFolder,eventAggFname,TeamAstring,TeamBstring,headers,readAttributeCols,timestampString,readEventColumns,conversionToMeter,skipCleanup,skipSpatAgg,skipEventAgg,exportData, exportDataString,includeCleanupInterpolation,datasetFramerate,debuggingMode):
	tCleanup = time.time()	# do stuff

	headers = headers.copy()
	
	debugOmittedRows = False # Optional export of data that was omitted in the cleaning process
	fatalTimeStampIssue = False
	fatalTeamIDissue = False
	fatalIssue = False
	fatalGroupRowIssue = False

	# Clean up data, if necessary
	cleanFnames = [f for f in listdir(cleanedFolder) if isfile(join(cleanedFolder, f)) if '.csv' in f]
	spatAggFnames = [f for f in listdir(spatAggFolder) if isfile(join(spatAggFolder, f)) if '.csv' in f]
	eventAggFnames = [f for f in listdir(eventAggFolder) if isfile(join(eventAggFolder, f)) if '.csv' in f]

	if spatAggFname in spatAggFnames and skipSpatAgg == True:
		warn('\nContinued with previously cleaned and spatially aggregated data.\nIf you want to add new spatial aggregates, change <skipSpatAgg> into <False>.\n')
		# Spat agg files don't exist if there was a fatal error, so:
		fatalIssue = False
		loadFolder = spatAggFolder
		loadFname = spatAggFname

		if eventAggFname in eventAggFnames and skipEventAgg == True:
		# If there is a spat agg file, AND if skipEventAgg == True,
		# then, it needs to be verified whether there is a row with an event aggregate for the current file.
			df = pd.read_csv(eventAggFolder+eventAggFname,usecols=(exportDataString),low_memory=True) # NB: low_memory MUST be True, otherwise it results in problems later on.
			# testCount = 0
			for i in np.arange(len(df.keys())):
				# testCount = testCount + 1

				try:
					if not any(df[exportDataString[i]] == exportData[i]):
						skipEventAgg = False
						break
				except: # this error occurred a couple of times. Not sure why... something with an invalid comparison, so presumably one of the inputs wasnt a string??
					print('DataFrame type:')
					print(type(df[exportDataString[i]]))
					print('\nDataFrame contents:')
					print(df[exportDataString[i]])
					print('\n----------\n')
					print('File identifiers type:')
					print(type(exportData[i]))
					print('\nFile identifiers contents:')
					print(exportData[i])
					print('\nNB: In the past, this problem was related to the string input resembling a float input.\nFor example, <1E3>, which (with low_memory = True) is read as a float (1,000).\n')
					if not exportData[i] in exportDataString[i]:
						skipEventAgg = False
						break
					else:
						raise Exception ('exit')
		else:
			skipEventAgg = False
		if TeamAstring == None:
			dfTeamString = pd.read_csv(cleanedFolder+cleanFname,usecols= ['TeamID'] ,low_memory=False) # NB: low_memory MUST be True, otherwise it results in problems later on.
			TeamAstring,TeamBstring = dataDrivenTeamstring(dfTeamString['TeamID'])
		if debuggingMode:
			elapsed = str(round(time.time() - tCleanup, 2))
			print('***** Time elapsed during cleanupData: %ss' %elapsed)
		return loadFolder,loadFname,fatalIssue,skipSpatAgg,skipEventAgg,TeamAstring,TeamBstring

	skipEventAgg = False
	skipSpatAgg = False # over-rule skipSpatAgg as the corresponding spatAgg output file could not be found

	if isfile(cleanedFolder + cleanFname) and skipCleanup:
	# if cleanFname in cleanFnames and skipCleanup:
		with open(cleanedFolder+cleanFname, 'r') as f:
			reader = csv.reader(f)
			fileHeaders = list(next(reader))

		if 'fatalIssue' in fileHeaders:
			fatalIssue = True
			warn('\nFATAL WARNING: In a previous clean-up, there was a problem with timestamp:\nSome timestamps occurred more often than there were unique <PlayerID>s')
		else:
			warn('\nContinued with previously cleaned data.\nIf problems exist with data consistency, consider writing a function in cleanupData.py.\n')
		loadFolder = cleanedFolder
		loadFname = cleanFname
		
		if TeamAstring == None and not fatalIssue:
			df = pd.read_csv(cleanedFolder+cleanFname,usecols= ['TeamID'] ,low_memory=False) # NB: low_memory MUST be True, otherwise it results in problems later on.

			TeamAstring,TeamBstring = dataDrivenTeamstring(df['TeamID'])
		if debuggingMode:
			elapsed = str(round(time.time() - tCleanup, 2))
			print('***** Time elapsed during cleanupData: %ss' %elapsed)
		return loadFolder,loadFname,fatalIssue,skipSpatAgg,skipEventAgg,TeamAstring,TeamBstring#, readAttributeCols#, attrLabel
	else: # create a new clean Fname
		print('\nCleaning up file...')

		if dataType == "NP":
			# NB: cleanupData currently dataset specific (NP or FDP). Fixes are quite specific and may not easily transfer to different datasets.
			# df_cleaned,df_omitted,headers,readAttributeCols,readEventColumns = \
			df_cleaned,df_omitted = \
			NP(dirtyFname,cleanFname,dataFolder,cleanedFolder,headers,readAttributeCols,debugOmittedRows,readEventColumns,TeamAstring,TeamBstring)
		elif dataType == "FDP":
			df_cleaned,df_omitted,fatalTeamIDissue,TeamAstring,TeamBstring = FDP(dirtyFname,cleanFname,dataFolder,cleanedFolder,headers,readAttributeCols,debugOmittedRows,readEventColumns,TeamAstring,TeamBstring)
			# return 1,2,3,4,5
		elif dataType == "KNVB":#LT: added!
			df_cleaned,df_omitted,fatalTeamIDissue = \
			student_LT_cleanUp.process(dirtyFname,cleanFname,dataFolder,cleanedFolder,headers,readAttributeCols,debugOmittedRows,readEventColumns,TeamAstring,TeamBstring)
		else:
			# overwrite cleanedFolder and add a warning that no cleanup had taken place
			loadFolder = dataFolder
			loadFname = dirtyFname
			warn('\nWARNING: No clean-up function available for file <%s>.\nContinued without cleaning the data.' %dirtyFname)

			if debuggingMode:
				elapsed = str(round(time.time() - tCleanup, 2))
				print('***** Time elapsed during cleanupData: %ss' %elapsed)
			return loadFolder,loadFname,fatalIssue,skipSpatAgg,skipEventAgg

		## Genereic clean up function (for all datasets)
		# First: Rename columns to be standardized.
		# From now onward, standardized columnNames can be used.
		df_cleaned = standardizeColumnHeaders(df_cleaned,headers)

		# Convert to meters
		df_cleaned = convertToMeters(df_cleaned,conversionToMeter)

		# Check if there is already a set of rows for team values (i.e., rows without playerID that are not 'ball')
		# df_cleaned.to_csv('C:\\Users\\rensm\\Documents\\PostdocLeiden\\NP repository\\Output\\test.csv')
		df_cleaned,fatalGroupRowIssue = verifyGroupRows(df_cleaned)

		## OLD BUT USEFUL
		# Confirm whether Every timestamp occurs equally often, to enable indexing based on timestamp
		# tsConsistent = verifyTimestampConsistency(df_cleaned,TeamAstring,TeamBstring)
			## you could omit interpolation to save time. But 
			# if not tsConsistent:
				# --> interpolation highly recommended
		## \OLD BUT USEFUL

		# The first fatal error. Skip file and continue.
		fatalTimeStampIssue = checkForFatalTimestampIssue(df_cleaned)
		
		# df_cleaned,df_omitted = \
		# student_LT_cleanUp.process(dirtyFname,cleanFname,dataFolder,cleanedFolder,headers,readAttributeCols,debugOmittedRows,readEventColumns,TeamAstring,TeamBstring)

		if exists(cleanedFolder + cleanFname):
			warn('\nOverwriting file <%s> \nin cleanedFolder <%s>.\n' %(cleanFname,cleanedFolder))

		# Export cleaned data to CSV
		if fatalTeamIDissue:
			df_Fatal = pd.DataFrame(['fatalTeamIDissue'],columns=['fatalIssue'])
			df_Fatal.to_csv(cleanedFolder + cleanFname)
			fatalIssue = True
			warn('\nFATAL CLEANUP ISSUE: An issue with creating the new TeamID by splitting up player name.\nFatal cleanup issue result in the current file being skipped in its entirety\n')
		elif fatalTimeStampIssue:
			df_Fatal = pd.DataFrame(['fatalTimeStampIssue'],columns=['fatalIssue'])
			df_Fatal.to_csv(cleanedFolder + cleanFname)
			fatalIssue = True
			warn('\nFATAL CLEANUP ISSUE: Something with the same timestamp occurring multiple times for the same playerID\nFatal cleanup issue result in the current file being skipped in its entirety\n')
		elif fatalGroupRowIssue:
			df_Fatal = pd.DataFrame(['fatalGroupRowIssue'],columns=['fatalIssue'])
			df_Fatal.to_csv(cleanedFolder + cleanFname)
			fatalIssue = True
			warn('\nFATAL CLEANUP ISSUE: problems with groupRows. Perhaps one of the players did not have a team assigend to it.\nFatal cleanup issue result in the current file being skipped in its entirety\n')

		else:
			# print(len(df_cleaned.loc[df_cleaned['PlayerID'] == 'groupRow','Ts'].unique()))
			if includeCleanupInterpolation:
				# tmp = df_cleaned.loc[df_cleaned['TeamID'] == TeamAstring]#.index
				# rowswithinrangePlayerA = tmp.index
				# X1 = df_cleaned.loc[rowswithinrangePlayerA].pivot(columns='PlayerID',values='Ts')
				# Y1 = df_cleaned.loc[rowswithinrangePlayerA].pivot(columns='PlayerID',values='Speed')
				# for ix,player in enumerate(X1.keys()):
				# 	tmp = X1[player].notnull()[::-1].idxmax()
				# 	pltA = plt.plot(X1[player].loc[tmp],Y1[player].loc[tmp],'s',color='blue')
				# 	plt.show()


				# # plt.show()
				# pdb.set_trace()
				#LT: deleted, because of negative Speed
				# df_cleaned,fatalGroupRowIssueAfterFiltering = FillGaps_and_Filter.process(df_cleaned,datasetFramerate = datasetFramerate)
				if 'fatalIssue' in df_cleaned:
					df_cleaned.to_csv(cleanedFolder + cleanFname)
					fatalIssue = True

			# print(len(df_cleaned.loc[df_cleaned['PlayerID'] == 'groupRow','Ts'].unique()))
			# pdb.set_trace()
			#LT: deleted, because of negative Speed --> nog fillgaps and filter
			# if fatalGroupRowIssueAfterFiltering:
			# 	df_Fatal = pd.DataFrame(['fatalGroupRowIssueAfterFiltering'],columns=['fatalIssue'])
			# 	df_Fatal.to_csv(cleanedFolder + cleanFname)
			# 	fatalIssue = True
			# 	warn('\nFATAL CLEANUP ISSUE: This one is doubly weird, as there was no issue with the grouprows before FillGaps_and_Filter.py..\nThis really should not be possible.\nIf it happens, check out the bit in FillGaps_and_Filter where it calls upon verifyGroupRows again.\nFatal cleanup issue result in the current file being skipped in its entirety\n')
			if not fatalIssue:
				df_cleaned.to_csv(cleanedFolder + cleanFname)
	
		# Optional: Export data that has been omitted, in case you suspect that relevent rows were omitted.
		if debugOmittedRows:
			omittedFolder = dataFolder + 'Omitted\\'
			omittedFname = fname[:-4] + '_omitted.csv'
			if not exists(omittedFolder):
				makedirs(omittedFolder)
			else:
				if exists(omittedFolder + omittedFname):
					warn('\nOverwriting file <%s> \nin omittedFolder <%s>.\n' %(omittedFname,omittedFolder))
			df_omitted.to_csv(omittedFolder + omittedFname)

		# # Now that Time was loaded, change the key to its generic value
		# # Add time to the attribute columns (easy for indexing)
		# readAttributeCols = [timestampString] + readAttributeCols # This makes sure that timeStamp is also imported in attribute cols, necessary for pivoting etc.
		# attrLabel.update({'Ts': 'Time (s)'})
		# readAttributeCols[0] = 'Ts'
		loadFolder = cleanedFolder
		loadFname = cleanFname

	if debuggingMode:
		elapsed = str(round(time.time() - tCleanup, 2))
		print('***** Time elapsed during cleanupData: %ss' %elapsed)

	return loadFolder,loadFname,fatalIssue,skipSpatAgg,skipEventAgg,TeamAstring,TeamBstring #, readAttributeCols#, attrLabel

def FDP(fname,cleanFname,dataFolder,cleanedFolder,headers,readAttributeCols,debugOmittedRows,readEventColumns,TeamAstring,TeamBstring):
	expectedVals = (-60,60,-40,40) # This should probably be dataset specific.
	conversion_to_S = .001
	# FDP specific function where the column 'Naam' is seperated into its PlayerID and TeamID
	ts = headers['Ts']
	x,y = headers['Location']
	ID = headers['PlayerID']

	colHeaders = [ts,x,y,ID] + readAttributeCols + readEventColumns
	if headers['TeamID'] != None:
	 # If there is a header for 'TeamID', then include it as the colHeaders that will be read from the CSV file.
	 # Mede mogelijk gemaakt door: Lars
	 Tid = headers['TeamID']
	 colHeaders = [ts,x,y,ID,Tid] + readAttributeCols + readEventColumns
	 
	newPlayerIDstring = 'Player'
	newTeamIDstring = 'Team'

	# Only read the headers as a check-up:
	with open(dataFolder+fname, 'r') as f:
	 reader = csv.reader(f)
	 fileHeaders = list(next(reader))

	for i in colHeaders:
	 if not i in fileHeaders:
	  warn('\nEXIT: Column header <%s> not in column headers of the file:\n%s\n\nSOLUTION: Change the user input in \'process\' \n' %(i,fileHeaders))
	  exit()

	df = pd.read_csv(dataFolder+fname,usecols=(colHeaders),low_memory=False)
	df[ts] = df[ts]*conversion_to_S # Convert from ms to s.

	## When no teamstring has been assigned yet, extract it from the data. Will only work if there are two unique string inputs in the row teamID
	if TeamAstring == None:
		TeamAstring, TeamBstring = dataDrivenTeamstring(df[Tid])
		
	## Cleanup for BRAxNLD
	fatalTeamIDissue = False
	if headers['TeamID'] == None:
		df,headers,fatalTeamIDissue = splitName_and_Team(df,headers,ID,newPlayerIDstring,newTeamIDstring,TeamAstring,TeamBstring)
		# Delete the original ID, but only if the string is not the same as the new Dict.Keys
		if ID != newPlayerIDstring and ID != newTeamIDstring:
			del df[ID]
			# del df_omitted[ID]		
		ID = headers['PlayerID']

	# if referee rows exist, drop them
	refRows = (df[ headers['TeamID']].isnull()) & (df[x].notnull()) & (df[ID] != 'ball')
	if any(refRows):
		df.drop(df[refRows].index,inplace = True)
		warn('\nWARNING: Some rows were identified as referee rows. These were dropped.\n')

	df_cropped01,df_omitted01 = omitXandY_equals0(df,x,y,ID)
	df_cropped02,df_omitted02 = omitRowsWithout_XandY(df_cropped01,x,y)	
	df_cropped03,df_omitted03 =	omitRowsWithExtreme_XandY(df_cropped02,x,y,expectedVals)	

	# Idea: by incorporting df_omittedNN in a list, you could make the script work with variable lenghts of omitted dfs.
	df_omitted = pd.concat([df_omitted01, df_omitted02, df_omitted03])
	## End cleanup for BRAxNLD

	df_cleaned = df_cropped03

	return df_cleaned, df_omitted,fatalTeamIDissue,TeamAstring,TeamBstring

def NP(fname,newfname,folder,cleanedFolder,headers,readAttributeCols,debugOmittedRows,readEventColumns,TeamAstring,TeamBstring):

	# Only read the headers as a check-up:
	with open(folder+fname, 'r') as f:
		reader = csv.reader(f)
		fileHeaders = list(next(reader))

	# Create a copy of the headers that may be manipulated if the file is an exception.
	# By creating a copy, you avoid any carry-over effects to the next file.
	colHeaders,headersCopy,readEventColumnsCopy,readAttributeColsCopy = \
	checkKnownExceptions_colHeaders(fileHeaders,headers,readAttributeCols,readEventColumns)

	for i in colHeaders:
		# if not [True for j in fileHeaders if re.search(i,j)]:
		if not [True for j in fileHeaders if i == j]:			
			print('WARNING (potentially fatal): Column header \n<%s>' %i)
			warn('\nnot in column headers of the file:\n%s\n\n' %fileHeaders)
	df = pd.read_csv(folder+fname,usecols=(colHeaders),low_memory=False,skip_blank_lines  = True,skipinitialspace  = True)

	# Strip the column headers from spaces at the start or end of the string.
	df.rename(columns=lambda x: x.strip(),inplace = True)

	# Rename columns to originals (to avoid issues with the copies of the headers)
	df = renameHeaders(df,headers,headersCopy,readAttributeCols,readAttributeColsCopy,readEventColumns,readEventColumnsCopy)

	# Omit rows without value
	df.dropna(axis=0, how='all',inplace = True)

	# Convert timestamp to seconds
	df = convertHHMMSS_to_s(df,headers['Ts'])

	# teamstring consistency (in team ID)
	df = checkTeamString_withoutSpaces_ignoringCase(df,headers['TeamID'],TeamAstring,TeamBstring)
	
	# Check the group rows that contain information.
	df = checkGroupRows_withInformation(df,headers,readEventColumns,TeamAstring,TeamBstring)

	df_cleaned = df
	df_omitted = pd.DataFrame([],columns = [df.keys()])
	# Idea: by incorporting df_omittedNN in a list, you could make the script work with variable lenghts of omitted dfs.
	# df_omitted = pd.concat([df_omitted01, df_omitted02, df_omitted03])

	return df_cleaned,df_omitted#,headers,readAttributeCols,readEventColumns

def splitName_and_Team(df,headers,ID,newPlayerIDstring,newTeamIDstring,TeamAstring,TeamBstring):

	# Force all values to be a string
	df[ID] = df[ID].apply(str)
	# Split 'Naam' into PlayerID and TeamID
	df_temp = pd.DataFrame(df[ID].str.split('_',1).tolist(),columns = [newPlayerIDstring,newTeamIDstring])
	# Necessary addition, as some players of different teams had the same 'PlayerID'	
	TeamA_rows = df_temp[newTeamIDstring] == TeamAstring
	df_temp[newPlayerIDstring][TeamA_rows] = df_temp[newPlayerIDstring][TeamA_rows] + "A"
	TeamB_rows = df_temp[newTeamIDstring] == TeamBstring
	df_temp[newPlayerIDstring][TeamB_rows] = df_temp[newPlayerIDstring][TeamB_rows] + "B"
	
	fatalTeamIDissue = False
	if sum(TeamA_rows) == 0:
		warn('\nFATAL WARNING: Could not find any <%s> values in the data.\nCheck whether the identification of the team''s ID string worked correctly.\n' %TeamAstring)
		fatalTeamIDissue = True
	if sum(TeamB_rows) == 0:
		warn('\nFATAL WARNING: Could not find any <%s> values in the data.\nCheck whether the identification of the team''s ID string worked correctly.\n' %TeamBstring)		
		fatalTeamIDissue = True
	df = pd.concat([df, df_temp], axis=1, join='inner')
	
	headers['PlayerID'] = newPlayerIDstring
	headers['TeamID'] = newTeamIDstring

	return df,headers,fatalTeamIDissue

def omitXandY_equals0(df,x,y,ID):
	# Omit rows where both x and y = 0 and where there is no team value
	XandY_equals0 = ( ((df[x] == 0) & (df[y] == 0) & (df[ID] == 'nan')) ) 
	df[XandY_equals0 == True]
	df_cleaned 	= df[XandY_equals0 == False]
	df_omitted 	= df[XandY_equals0 == True]

	return df_cleaned,df_omitted

def omitRowsWithout_XandY(df,x,y):
	# Omit rows that have no x and y value.
	rowsWith_XandY = (df[x].notnull()) & (df[y].notnull())

	df_cleaned = df[rowsWith_XandY == True ]
	df_omitted = df[rowsWith_XandY == False ]

	warn('\nWARNING: Omitted rows without X and Y. These could have been goupRows.\nConsider using <skip_blank_lines  = True> in pd.read_csv.' )

	return df_cleaned, df_omitted

def omitRowsWithExtreme_XandY(df,x,y,expectedVals):
	# Omit rows that have X and Y values that lie outside of the expected range.
	# Where df is the dataframe, x and y indicate the dictionary key that represent
	# the x and y positions. ExpectedVals is a tuple that contains (xmin,xmax,ymin,ymax).
	
	xmin = expectedVals[0]
	xmax = expectedVals[1]	
	ymin = expectedVals[2]	
	ymax = expectedVals[3]	

	rowsWithoutExtreme_XandY = (df[x] > xmin) & (df[x] < xmax) & (df[y] > ymin) & (df[y] < ymax)
	df_cleaned = df[rowsWithoutExtreme_XandY == True]
	df_omitted = df[rowsWithoutExtreme_XandY == False]	

	return df_cleaned,df_omitted

def verifyTimestampConsistency(df,TeamAstring,TeamBstring):

	## Work in progress. Here you can do some sort of interpolation	if the timestamps don't match-up
	# uniqueTs = pd.unique(df['Ts'])
	
	# dfA = df[df['TeamID'] == TeamAstring]
	# dfB = df[df['TeamID'] == TeamBstring]
	# #pivot X and Y dataframes for Team A
	# Team_A_Ts = dfA.pivot(columns='PlayerID', values='Ts')
	# Team_B_Ts = dfB.pivot(columns='PlayerID', values='Ts')

	# perPlayer_Ts = df.pivot(columns='PlayerID',values='Ts')
	
	# perPlayer_Ts.to_csv('C:\\Users\\rensm\\Documents\\PostdocLeiden\\BRAxNLD repository\\Data\\tmp\\test.csv')

	# # df = df.reindex(pd.unique(ts))
	# # dfnew = df.set_index('Ts', drop=False, append=False, inplace=False, verify_integrity=True)
	# # dfnew = df.pivot('Ts','PlayerID','X')
	# # tmp = df.pivot(index='PlayerID', columns='Ts', values='X')
	# # newDf.join(tmp).drop('Ts', axis=1)
	# df.pivot(index='Ts', columns)
	# newDf.to_csv('C:\\Users\\rensm\\Documents\\PostdocLeiden\\BRAxNLD repository\\Data\\tmp\\test.csv')

	# pdb.set_trace()

	uniquePlayerID = pd.unique(df['PlayerID'])
	uniqueTs = pd.unique(df['Ts'])

	if len(uniqueTs)*len(uniquePlayerID) == len(df['Ts']): # not very accurate, but should be good enough
		tsConsistent = True
	else:
		tsConsistent = False
		warn('WARNING: Timestamp not consistent for every player.\nConsider adding a function in cleanupData.py to add empty rows for missing data.\nBe careful indexing based on timestamp.\nConsider interpolation to make sure timestamps match up.\n')
		for i in uniquePlayerID:
			# Take the unique timestamps per player
			uTsPl = pd.unique(df['Ts'][df['PlayerID'] == i])
			print('Player <%s>: Number of observations %d (%d-%ds).' %(i,len(uTsPl),min(uTsPl),max(uTsPl)))
			# print('\nConsider interpolation to make sure timestamps match up.\n')
	
	return tsConsistent
	
def checkForFatalTimestampIssue(rawDict):
	# A security measure to pick up any inconsistencies in timestamp
	# Could potentially be expanded with some automatic corrections
	fatalTimeStampIssue = False
	PlayerID = rawDict['PlayerID']
	TsS = rawDict['Ts']

	uniqueTsS,freqUniqueTs = np.unique(TsS,return_counts=True)
	uniquePlayers = pd.unique(PlayerID)

	# if any(tmp != np.median(tmp)) or len(uniqueTsS) != len(PlayerID) / len(uniquePlayers):
	# 	# Problem with timestamp. Not every timestamp occurs equally often and/or there isn't the expected number of unique timestamps
	# 	warn('\nFATAL WARNING: Problem with timestamp. Not every timestamp occurs equally often.')
	# 	indices = np.where(tmp != np.median(tmp))
	# 	for i in np.nditer(indices):
	# 		i2 = np.where(uniqueTsS[i]==TsS)
	# 		# print('i2 = %s' %np.nditer(i2)[0])
	# 		print('Timestamp <%s> occurred <%s> times.' %(uniqueTsS[i],tmp[i]))
	# 	fatalTimeStampIssue = True
	
	if any(freqUniqueTs != np.median(freqUniqueTs)) or len(uniqueTsS) != len(PlayerID) / len(uniquePlayers):
		warn('\nWARNING: Potential problem with timestamp. Not every timestamp occurred equally often.')

	if max(freqUniqueTs) > len(uniquePlayers):
		# Problem with timestamp. Not every timestamp occurs equally often and/or there isn't the expected number of unique timestamps
		warn('\nFATAL WARNING: Some timestamps occurred more often than unique players exist:')
		print('Duplicate timestamps?')

		indices = np.where(freqUniqueTs != np.median(freqUniqueTs))
		for idx,i in enumerate(np.nditer(indices)):
			i2 = np.where(uniqueTsS[i]==TsS)
			# print('i2 = %s' %np.nditer(i2)[0])
			print('Timestamp <%s> occurred <%s> times.' %(uniqueTsS[i],freqUniqueTs[i]))
			if idx == 10:
				print('(...)\nFirst %s timestamps printed only.\n' %idx)
				break
		fatalTimeStampIssue = True

	return fatalTimeStampIssue

def verifyGroupRows(df_cleaned):

	fatalGroupRowIssue = False
	# Group Rows are the rows where any feature can be stored that captures multiple players (i.e., team or attackers/midfielders/defenders)
	# groupRows should have the 'PlayerID' value 'groupRow'
	
	# Grouprows - at this stage - are characterized by:
	# 1) a 'TeamID' that isnull()
	groupRows = (df_cleaned['TeamID'].isnull()) & (df_cleaned['Ts'].notnull()) 

	# 2) a 'PlayerID' that is not a 'ball'	
	if df_cleaned['PlayerID'].dtype != float:
		groupRows = (groupRows) & (df_cleaned['PlayerID'] != 'ball')

	
	uniqueTs = pd.unique(df_cleaned['Ts'])
	uniqueTs = np.sort(uniqueTs)	
	
	# if not any(groupRows):
	# 	# groupRows don't exist. Let's see if there are any rows without PlayerID
	# 	everyPlayerIDs = df_cleaned['PlayerID'].unique()
	# 	everyPlayerIDs = pd.DataFrame(everyPlayerIDs,columns = ['everyPlayerIDs'])
	# 	if any(everyPlayerIDs[everyPlayerIDs['everyPlayerIDs'].isnull()]):
	# 		warn('\nWARNING: Found rows without a PlayerID.\nThe pipeline assumes these rows are groupRows.\n')
	# 		groupRows = df_cleaned['PlayerID'].isnull().index
	# 		df_cleaned.loc[groupRows,'PlayerID'] = 'groupRow'


	# When there are no group rows, they need to be created for every unique timestamp.
	if df_cleaned['Ts'][(groupRows)].empty:# and not any(df_cleaned['PlayerID'] == 'groupRow'):
		# If groupRows don't exist, then create them
		# For every existing timestamp
		# Create a string value
		groupPlayerID = ['groupRow' for i in uniqueTs]
		# Create groupIndex by adding to highest existing index
		firstGroupIndex = df_cleaned.index[-1] + 1
		groupIndex = firstGroupIndex + np.arange(len(groupPlayerID))

		# Put these in a DataFrame with the same column headers
		df_group = pd.DataFrame({'Ts':uniqueTs,'PlayerID':groupPlayerID},index = groupIndex)# possibly add the index ? index = []
		
		# Append them to the existing dataframe
		df_cleaned = df_cleaned.append(df_group)

	else: # If group rows do exist,
		
		# verify that 'PlayerID' = 'groupRow'
		if df_cleaned['PlayerID'].dtype == float or any(df_cleaned['PlayerID'][groupRows] != 'groupRow'): # Input was not a string, so no groupRows indicated.
		# 	df_cleaned.loc[groupRows,('PlayerID')] = 'groupRow'

		# elif any(df_cleaned['PlayerID'][groupRows] != 'groupRow'):		
			# df_cleaned['PlayerID'][groupRows] = 'groupRow'
			timeString = time.strftime("%Hh%Mm_%d_%B_%Y")
			df_cleaned.loc[groupRows].to_csv('debug_overwritten_as_grouprows_' + timeString + '.csv')

			warn('\nWARNING: Contents of PlayerID overwritten for group rows.\nBe sure that group rows were identified correctly.\n')
			df_cleaned.loc[groupRows,('PlayerID')] = 'groupRow'

		# Verify that x, y, and TeamID are empty
		if not all(df_cleaned['X'][(groupRows)].isnull()):			
			warn('\nFATAL WARNING: X values of groupRows are not empty.\Consider cleaning. ')
			fatalGroupRowIssue = True

		if not all(df_cleaned['Y'][(groupRows)].isnull()):
			warn('\nFATAL WARNING: Y values of groupRows are not empty.\Consider cleaning. ')
			fatalGroupRowIssue = True

		if not all(df_cleaned['TeamID'][(groupRows)].isnull()):
			warn('\nWARNING: TeamID values of groupRows are not empty.\Consider cleaning. ')			

		# and finally, verify that there is a group row for every timestamp.
		if len(df_cleaned['PlayerID'][groupRows]) != len(uniqueTs):
			warn('\nWARNING: Not as many groupRows as unique timestamps.\nSolved it by appending the missing timestamps as groupRows to the end of the file.\nThis may result in a non-ordered dataset!! (if groupRows were not originally at the end of the file)\nCould consider re-ordering dataFrame after inserting these missing timestamps.\n')

			# Definitely not the fastest way. But it works.
			# Check which Ts values are missing for the groupRows
			missingGroupTs = []
			for i in uniqueTs:
				ismissingGroupTs = True
				if i == '':
					ismissingGroupTs = False
					continue
				for j in df_cleaned['Ts'][groupRows]:
					if i == j:
						ismissingGroupTs = False
				if ismissingGroupTs:
					missingGroupTs.append(i)

			# Add missing group rows as empty rows
			# Create a string value
			groupPlayerID = ['groupRow' for i in missingGroupTs]

			
			## Adjustment to order the newly created groupRows by Ts
			# Create a dataframe of old and new gropurows
			missingGroupRows = pd.DataFrame({'Ts':missingGroupTs,'PlayerID':groupPlayerID})# possibly add the index ? index = []
			existingGroupRows = df_cleaned.loc[groupRows]
			old_and_new_grouprows = pd.concat([missingGroupRows,existingGroupRows],axis = 0)
			old_and_new_grouprows = old_and_new_grouprows.sort_values(['Ts'])
			old_and_new_grouprows.reset_index(drop=True,inplace = True)
			# Drop the old ones from the original df
			df_cleaned.drop(df_cleaned[groupRows].index,inplace = True)
			# Add the new ones
			df_cleaned = df_cleaned.append(old_and_new_grouprows)
			# df_cleaned.to_csv('C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\NP repository\\newgrouprows.csv')

			## The old method ##
			# # Create groupIndex by adding to highest existing index
			# firstGroupIndex = df_cleaned.index[-1] + 1
			# groupIndex = firstGroupIndex + range(len(groupPlayerID))

			# # Put these in a DataFrame with the same column headers
			# df_group = pd.DataFrame({'Ts':missingGroupTs,'PlayerID':groupPlayerID},index = [groupIndex])# possibly add the index ? index = []
			
			# # Append them to the existing dataframe
			# df_cleaned = df_cleaned.append(df_group)
			## / The old method ##

			df_cleaned.reset_index(drop=True,inplace = True)

			newGroupRows = df_cleaned['PlayerID'] == 'groupRow'
			groupRows = newGroupRows

			# # print(len(missingGroupTs))
			# print(len(np.unique(df_cleaned['Ts'][groupRows])))

			# pdb.set_trace()
		# ##########################
		# #### Work in progress ####
		# ##########################
		# ### I haven't finished this last clean up procedure. 
		# ### The foundations are there (including an elaborate way of locating the problematic cells).

		# # And finally finally, check that every groupRow timestamp occurs in a Player Row.
		# uniqueTs_GroupRows = df_cleaned['Ts'][df_cleaned['PlayerID'] == 'groupRow'].unique()
		# uniqueTs_nonGroupRows = df_cleaned['Ts'][df_cleaned['PlayerID'] != 'groupRow'].unique()
		# Ts_to_be_removed = [i for i in uniqueTs_GroupRows if not np.isin(i,uniqueTs_nonGroupRows)]

		# # An elaborate way to pinpoint the problem: (alternatively, could just check if Ts_to_be_removed is empty..)
		# orderedPerPlayer_perTs = df_cleaned.groupby('PlayerID')['Ts']
		# orderedPerPlayer_perTs_nunique = orderedPerPlayer_perTs.nunique()
		# uniquePlayers = pd.unique(df_cleaned['PlayerID'])
		# verification = np.array([])
		# for i in uniquePlayers:
		# 	if i == 'groupRow':
		# 		# Skip the number of unique timestamps in the groupRow
		# 		continue

		# 	idx = np.where(i == orderedPerPlayer_perTs_nunique.keys())
		# 	# Could consider adding excluding 'ball' here as well..
		# 	if orderedPerPlayer_perTs_nunique[idx[0][0]] > orderedPerPlayer_perTs_nunique['groupRow']:
		# 		# Not good.. the number of unique timeframes can't be more than the number of groupRows.
		# 		# In fact, this should have been corrected above: "and finally, verify that there is a group row for every timestamp."
		# 		verification = np.append(verification,1)
		# 	elif orderedPerPlayer_perTs_nunique[idx[0][0]] < orderedPerPlayer_perTs_nunique['groupRow']:
		# 		# This can happen. It's checked under checkForFatalTimestampIssue()
		# 		# BUT: If ALL players have less timestamps than the groupRow, then it is problematic.
		# 		verification = np.append(verification,2)

		# 	elif orderedPerPlayer_perTs_nunique[idx[0][0]] == orderedPerPlayer_perTs_nunique['groupRow']:
		# 		# As long as at least one player has this, then there is no problem.
		# 		verification = np.append(verification,3)
		# 	else:
		# 		# Should not be possible
		# 		warn('\nWARNING: This should not be possible. The number of unique timestamps for groupRows and non-groupRows was incomparable.\nThis may have consequences for cleaning up the data to have the right number of groupRows.\n')
		
		# if not any(verification == 3):
		# 	# Indeed, some of the groupRow timestamps did not occur in the non-groupRows (including 'ball')
		# 	# I can think of two ways to solve this:
			
		# 	# 1) Delete the groupRows that have a timestamp that does not occur for any player.
		# 	# + = easy. - = it may delete groupRows with information.

		# 	# 2) Add the missing timestamp to the non-groupRows.
		# 	# + = don't risk deleting groupRows with information. - = not sure what the impact is on analyses using these groupRows

		# 	# I'll first work out solution 1 (with an if statement to check whether removed groupRow is empty)
		# 	# If problems persist, solution 2 can be worked out as well.

		# 	# Omit groupRows that have a timestamp that does not occur for any non-groupRow:
		# 	# Already did this above:
		# 	## uniqueTs_GroupRows = df_cleaned['Ts'][df_cleaned['PlayerID'] == 'groupRow'].unique()
		# 	## uniqueTs_nonGroupRows = df_cleaned['Ts'][df_cleaned['PlayerID'] != 'groupRow'].unique()
		# 	## Ts_to_be_removed = [i for i in uniqueTs_GroupRows if not np.isin(i,uniqueTs_nonGroupRows)]
		# 	doSomething = [] # option 1 or 2 as described above.

		# ##########################
		# #### \Work in progress ###
		# ##########################	

		# print(df_cleaned.keys())
		# pdb.set_trace()
	return df_cleaned,fatalGroupRowIssue

def convertHHMMSS_to_s(df,ts):
	# I used a regular expression to convert a timestamp to numbers.
	# Timestamp can either be h+:mm:ss.d+ or m+:ss.d+
	# First assume there are 2 ':'
	newdf = pd.DataFrame(data = [],index = df.index, columns = [ts],dtype = 'int32')
	regex = r'(\d+):(\d{2}):(\d{2})\.(\d+)'
	regex_alternative = r'(\d+):(\d{2})\.(\d+)'
	for idx,tmpString in enumerate(df[ts]):

		match = re.search(regex,tmpString)
		if match:
			grp = match.groups()
			DecimalCorrection = 10 ** len(grp[2])

			timestamp = int(grp[0])*3600*DecimalCorrection +\
						int(grp[1])*60*DecimalCorrection +\
						int(grp[2])*1*DecimalCorrection +\
						int(grp[3])
			
		else:
			# Apparently, sometimes it's MM:SS instead of HH:MM:SS
			match = re.search(regex_alternative,tmpString)
			if match:
				grp = match.groups()
				DecimalCorrection = 10 ** len(grp[2])

				timestamp = int(grp[0])*60*DecimalCorrection +\
							int(grp[1])*1*DecimalCorrection +\
							int(grp[2])
			else:
				warn('\nCould not decipher timestamp.')

		newdf[ts][idx] = timestamp / DecimalCorrection

	df[ts] = newdf[ts]

	return df

def checkTeamString_withoutSpaces_ignoringCase(df,Team,TeamAstring,TeamBstring):

	df.loc[df[Team].isnull(),(Team)] = np.nan
	teama01 = (df[Team].str.lower() == 'team a')
	teama02 = (df[Team].str.lower() == 'teama')

	df.loc[teama01,(Team)] = TeamAstring
	df.loc[teama02,(Team)] = TeamAstring

	teama01 = (df[Team].str.lower() == 'team b')
	teama02 = (df[Team].str.lower() == 'teamb')

	df.loc[teama01,(Team)] = TeamBstring
	df.loc[teama02,(Team)] = TeamBstring

	# # This works as well, but the code above is 50% faster. I'm just keeping this in case the code above fails.
	# newdf = pd.DataFrame(data = [],index = df.index, columns = [Team],dtype = 'str')
	# for idx,tmpString in enumerate(df[Team]):
		
	# 	if pd.isnull(tmpString):
	# 		newdf[Team][idx] = np.nan

	# 	elif re.search('team a',tmpString, re.IGNORECASE) or re.search('teama',tmpString, re.IGNORECASE):
	# 		newdf[Team][idx] = str(TeamAstring)

	# 	elif re.search('team b',tmpString, re.IGNORECASE) or re.search('teamb',tmpString, re.IGNORECASE):
	# 		newdf[Team][idx] = str(TeamBstring)

	# 	else:
	# 		print(tmpString)
	# 		pdb.set_trace()

	# df[Team] = newdf[Team]

	return df

## Ancient function. Use instead: "df.dropna(axis=0, how='all',inplace = True)"
# def omitRowsWithoutValues(df):
# 	# Omit rows that have no x and y value.
# 	# test = [df[i].notnull() for i in colHeaders]
# 	emptyRows = pd.DataFrame(data = [],index = df.index, columns = ['emptyRows'],dtype = 'bool')
# 	t = time.time()	# do stuff
#
# 	for idx in df.index:
# 		test = [pd.notnull(df[i][idx]) for i in df.keys()]
# 		if any(test):
# 			# Row not empty
# 			emptyRows['emptyRows'] = False
# 		else:
# 			# Empty row
# 			emptyRows['emptyRows'] = True
# 			print(emptyRows)
# 			pdb.set_trace()
# 		if 	time.time() - t > 5:
# 		# it's been longer than 5s. Let's apply a stricter rule
# 			doSomething = []
# 			print('I did something')
# 			pdb.set_trace()
#
#
#	df_cleaned = df[emptyRows['emptyRows'] == False ]
#	df_omitted = df[emptyRows['emptyRows'] == True  ]
#
#	return df_cleaned, df_omitted

def standardizeColumnHeaders(df_cleaned,headers):
	
	ts = headers['Ts']
	x,y = headers['Location']
	PlID = headers['PlayerID']
	T_ID = headers['TeamID']
	df_cleaned.rename(columns={ts: "Ts", x: "X", y: "Y", PlID: "PlayerID", T_ID: "TeamID"}, inplace=True)

	return df_cleaned

def convertToMeters(df_cleaned,conversionToMeter):
	
	df_cleaned['X'] = df_cleaned['X'] * conversionToMeter
	df_cleaned['Y'] = df_cleaned['Y'] * conversionToMeter

	return df_cleaned

def checkKnownExceptions_colHeaders(fileHeaders,headers,readAttributeCols,readEventColumns):
	
	ts = headers['Ts']
	x,y = headers['Location']
	ID = headers['PlayerID']
	Team = headers['TeamID']

	readEventColumnsCopy = readEventColumns.copy()
	readAttributeColsCopy = readAttributeCols.copy()
	headersCopy = headers.copy()
	# Known exceptions:
	timeException = [True for j in fileHeaders if re.search('Video timing',j,re.IGNORECASE)]
	if timeException:
		headersCopy['Ts'] = 'Video timing'
		ts = headersCopy['Ts']

	posIdx = [idx for idx,i in enumerate(readEventColumns) if 'Possession' in i]

	possessionException01 = [True for j in fileHeaders if re.search('Possession / Turnover',j,re.IGNORECASE)]
	if possessionException01 and not posIdx == []:
		readEventColumnsCopy[posIdx[0]] = 'Possession / Turnover'

	possessionException02 = [True for j in fileHeaders if re.search('Possession/ Turnover',j,re.IGNORECASE)]
	if possessionException02 and not posIdx == []:
		readEventColumnsCopy[posIdx[0]] = 'Possession/ Turnover'

	# This copy is only for the colHeaders
	readAttributeColsCopy_tmp = readAttributeColsCopy.copy()
	readEventColumnsCopy_tmp = readEventColumnsCopy.copy()	

	for j in fileHeaders:
		if j == '': # small correction for empty column headers
			continue
		if j[-1] == ' ':
			if j[-2] == ' ':
				replaceSpace = '  '
			else:
				replaceSPace = ' '
			if ts in j:
				ts = j#headers['Ts'] + replaceSPace
			elif x in j:
				x = j#headers['Location'][0] + replaceSPace
			elif y in j:
				y = j#headers['Location'][1] + replaceSPace
			elif ID in j:
				ID = j#headers['PlayerID'] + replaceSPace
			elif Team in j:
				Team = j#headers['TeamID'] + replaceSPace
			else:
				idxAttr = [idx for idx,i in enumerate(readAttributeColsCopy_tmp) if i in j]
				idxEvent = [idx for idx,i in enumerate(readEventColumnsCopy_tmp) if i in j]
				if len(idxAttr) != 0:
					readAttributeColsCopy_tmp[idxAttr[0]] = j#readAttributeCols[idxAttr[0]] + replaceSPace
				elif len(idxEvent) != 0:
					readEventColumnsCopy_tmp[idxEvent[0]] = j#readEventColumnsCopy[idxEvent[0]] + replaceSPace

	# The colheaders need to be exported seperately for the error in the spaces. 
	# But afterwards, these are stripped, so no need to include this in the copies.
	colHeaders = [ts,x,y,ID,Team] + readAttributeColsCopy_tmp + readEventColumnsCopy_tmp

	return colHeaders,headersCopy,readEventColumnsCopy,readAttributeColsCopy

def	renameHeaders(df,headers,headersCopy,readAttributeCols,readAttributeColsCopy,readEventColumns,readEventColumnsCopy):

	for key in headers:
		if key != 'Location':
			df.rename(columns={headersCopy[key]: headers[key]}, inplace=True)
		else:
			df.rename(columns={headersCopy[key][0]: headers[key][0]}, inplace=True)
			df.rename(columns={headersCopy[key][1]: headers[key][1]}, inplace=True)

	for idx,val in enumerate(readAttributeCols):
		df.rename(columns={readAttributeColsCopy[idx]: val}, inplace=True)

	for idx,val in enumerate(readEventColumns):
		df.rename(columns={readEventColumnsCopy[idx]: val}, inplace=True)

	return df

def removeSpacesEventColheaders(df,readEventColumns,readEventColumnsCopy,posIdx):

	if posIdx != []:
		# Could do the same for any other column headers that are inconsistent
		df.rename(columns={readEventColumnsCopy[posIdx[0]]: readEventColumns[posIdx[0]]}, inplace=True)

	runIdx = [idx for idx,i in enumerate(readEventColumns) if 'Run' in i]
	if runIdx != []:
		# Could do the same for any other column headers that are inconsistent
		df.rename(columns={readEventColumnsCopy[runIdx[0]]: readEventColumns[runIdx[0]]}, inplace=True)
		# df.rename(columns={readEventColumns[runIdx[0]]: "Run"}, inplace=True)
		# readEventColumns[runIdx[0]] = 'Run'

	goalIdx = [idx for idx,i in enumerate(readEventColumns) if 'Goal' in i]
	if goalIdx != []:
		# Could do the same for any other column headers that are inconsistent
		df.rename(columns={readEventColumnsCopy[goalIdx[0]]: readEventColumns[goalIdx[0]]}, inplace=True)
		# df.rename(columns={readEventColumns[goalIdx[0]]: "Goal"}, inplace=True)
		# readEventColumns[goalIdx[0]] = 'Goal'

	passIdx = [idx for idx,i in enumerate(readEventColumns) if 'Pass' in i]
	if passIdx != []:
		# Could do the same for any other column headers that are inconsistent
		df.rename(columns={readEventColumnsCopy[passIdx[0]]: readEventColumns[passIdx[0]]}, inplace=True)
		# df.rename(columns={readEventColumns[passIdx[0]]: "Pass"}, inplace=True)
		# readEventColumns[passIdx[0]] = 'Pass'

	return df#,readEventColumns

def checkGroupRows_withInformation(df,headers,readEventColumns,TeamAstring,TeamBstring):

	# identify groupRows
	groupRows = (df[headers['TeamID']].isnull())
	if df[headers['PlayerID']].dtype != float:
		# groupRows = (df_cleaned['TeamID'].isnull()) & (str(df_cleaned['PlayerID']) != 'ball') 		
		groupRows = (groupRows) & (df['PlayerID'] != 'ball')

	# Identify Run and Goal (swap wrong headers)

	# Identify teamstring in possession

	# When the group rows are emtpy, there's nothing to check
	if df[headers['Ts']][(groupRows)].empty:
		return df

	idx_groupRows = [idx for idx,i in enumerate(groupRows) if i == True]

	for i in readEventColumns:
		
		# I know this can be written better. But it works. so....
		eventWithInfo = df[i].notnull()
		idx_eventWithInfo = [idx for idx,i in enumerate(eventWithInfo) if i == True]
		tmp = [i for i in idx_eventWithInfo if not i in idx_groupRows ]

		if tmp != []: # just a safety measure
			warn('\nWARNING: a none groupRow contains <%s> information:\n<%s>'%(i,tmp))

		if 'Run' == i: # specific for columns that are Runs
			# df[i][groupRows].to_csv('C:\\Users\\rensm\\Documents\\PostdocLeiden\\NP repository\\Output\\test.csv')			runWithInfo = df[i].notnull()
			# I know this can be written better. But it works. so....
			for j in idx_eventWithInfo:
				curCell = df[i][j]	
				if not ('nd' in curCell or 'un' in curCell):
					warn('\nWARNING: Didnt recognize run: <%s>' %curCell)

		elif 'Goal' == i:
			for j in idx_eventWithInfo:
				curCell = df[i][j]	
				if not ('oal' in curCell):
					warn('\nWARNING: Didnt recognize goal: <%s>' %curCell)

				if not (TeamAstring[1:] in curCell or TeamBstring[1:] in curCell):
					warn('\nWARNING: could not identify team: <%s>' %curCell)

		elif 'Possession' in i:
			for j in idx_eventWithInfo:
				curCell = df[i][j]	

				if not (TeamAstring[1:] in curCell or TeamBstring[1:] in curCell):
					if 'Start A possession' in curCell:
						# A known issue. Replace the content.
						df[i][j] = 'Start %s possession' %TeamAstring

					else:
						warn('\nWARNING: could not identify team: <%s>' %curCell)

		elif 'Pass' == i:
			for j in idx_eventWithInfo:
				curCell = df[i][j]	
				if not (TeamAstring[1:] in curCell or TeamBstring[1:] in curCell):
					warn('\nWARNING: could not identify team: <%s>' %curCell)
		else:
			warn('\nWARNING: Did not recognize event column <%s>.\nConsider writing clean-up protocol in cleanupData.py.' %i)

	return df
	## Other clean-up ideas for NP data
	## Replace nearly empty cells
	# if val.isspace():
	# 	row[idx] = ''
	## Strip rows from useless spaces
	# row = [s.strip() for s in row]

def dataDrivenTeamstring(TeamID_Data):

	# Data driven team strings
	uniqueTeams = TeamID_Data.unique()
	uniqueTeams = [st for st in uniqueTeams if type(st) == str]
	if len(uniqueTeams) == 2:
		TeamAstring = uniqueTeams[0] 
		TeamBstring = uniqueTeams[1] 
		warn('\nWARNING: Teamstring had to be determined based on the data.\nTeamAstring = <%s>\nTeamBstring = <%s>' %(TeamAstring,TeamBstring))
	else:
		warn('\nFATAL WARNING: Teamstrings are non-existent and there were not precisely 2 unique team strings in the data.\nIt was thus impossible to determine the Team strings based on the data.\nTeamstrings in data:\n%s' %uniqueTeams)

	return TeamAstring, TeamBstring