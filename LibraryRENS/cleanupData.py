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

import VPcleanup
import LTcleanup


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
def process(dirtyFname,cleanFname,dataType,dataFolder,cleanedFolder,TeamAstring,TeamBstring,headers,readAttributeCols):
	debugOmittedRows = False # Optional export of data that was omitted in the cleaning process
	# Clean up data, if necessary
	cleanFnames = [f for f in listdir(cleanedFolder) if isfile(join(cleanedFolder, f)) if '.csv' in f]

	if cleanFname in cleanFnames:
		warn('\nContinued with previously cleaned data.\nIf problems exist with data consistency, consider writing a function in cleanupData.py.\n')

	else: # create a new clean Fname
		if dataType == "NP":
			# NB: cleanupData currently dataset specific (NP or FDP). Fixes are quite specific and may not easily transfer to different datasets.
			df_cleaned = NP(dirtyFname,cleanFname,dataFolder,cleanedFolder,TeamAstring,TeamBstring)
		elif dataType == "FDP":
			print('\nCleaning up file...')
			df_cleaned = FDP(dirtyFname,cleanFname,dataFolder,cleanedFolder,headers,readAttributeCols,debugOmittedRows)

		####################################################################################
		elif dataType == "VP_LT":
			# Here you could add more clean up functions.
			PrintThisToo = VPcleanup.this_is_a_function('\nThis text will be printed.\n')
			print(PrintThisToo)
			PrintThisToo = LTcleanup.this_is_a_function('\nThis text will be printed.\n')
			print(PrintThisToo)
		####################################################################################

		else:
			# overwrite cleanedFolder and add a warning that no cleanup had taken place
			cleanedFolder = dataFolder
			exit('\exit: No clean-up function available for file <%s>.\nContinued without cleaning the data.' %dirtyFname)

		# Relevant for all timeseries analysis:

		# Rename columns to be standardized.
		ts = headers['Ts']
		x,y = headers['Location']
		PlID = headers['PlayerID']
		T_ID = headers['TeamID']
		df_cleaned.rename(columns={ts: "Ts", x: "X", y: "Y", PlID: "PlayerID", T_ID: "TeamID"}, inplace=True)
		# Confirm whether Every timestamp occurs equally often, to enable indexing based on timestamp
		tsConsistent = verifyTimestampConsistency(df_cleaned,TeamAstring,TeamBstring)
		if not tsConsistent:
			warn('\nTO DO: Timestamp is not consistent: \nWrite the code to smooth out timestamp.')

		# Check if there is already a set of rows for team values (i.e., rows without playerID that are not 'ball')
		df_cleaned = verifyGroupRows(df_cleaned)

		# Export cleaned data to CSV
		df_cleaned.to_csv(cleanedFolder + cleanFname)		

	return cleanedFolder

def FDP(fname,cleanFname,dataFolder,cleanedFolder,headers,readAttributeCols,debugOmittedRows):

	expectedVals = (-60,60,-40,40) # This should probably be dataset specific.
	conversion_to_S = .001
	# FDP specific function where the column 'Naam' is seperated into its PlayerID and TeamID
	ts = headers['Ts']
	x,y = headers['Location']
	ID = headers['PlayerID']
	colHeaders = [ts,x,y,ID] + readAttributeCols
	newPlayerIDstring = 'Player'
	newTeamIDstring = 'Team'

	df = pd.read_csv(dataFolder+fname,usecols=(colHeaders),low_memory=False)
	df[ts] = df[ts]*conversion_to_S # Convert from ms to s.

	## Cleanup for BRAxNLD
	if headers['TeamID'] == None:
		df_WithName_and_Team,headers = splitName_and_Team(df,headers,ID,newPlayerIDstring,newTeamIDstring)
		
		df_cropped01,df_omitted01 = omitXandY_equals0(df_WithName_and_Team,x,y,ID)
		df_cropped02,df_omitted02 = omitRowsWithout_XandY(df_cropped01,x,y)	
		df_cropped03,df_omitted03 =	omitRowsWithExtreme_XandY(df_cropped02,x,y,expectedVals)	

		# Idea: by incorporting df_omittedNN in a list, you could make the script work with variable lenghts of omitted dfs.
		df_omitted = pd.concat([df_omitted01, df_omitted02,df_omitted03])

		# Delete the original ID, but only if the string is not the same as the new Dict.Keys
		if ID != newPlayerIDstring and ID != newTeamIDstring:
			del df_cropped03[ID]
			del df_omitted[ID]
	## End clenaup for BRAxNLD

	df_cleaned = df_cropped03.copy()

	if exists(cleanedFolder + cleanFname):
		warn('\nOverwriting file <%s> \nin cleanedFolder <%s>.\n' %(cleanFname,cleanedFolder))
	
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

	return df_cleaned


def splitName_and_Team(df,headers,ID,newPlayerIDstring,newTeamIDstring):

	# Force all values to be a string
	df[ID] = df[ID].apply(str)
	# Split 'Naam' into PlayerID and TeamID
	df_temp = pd.DataFrame(df[ID].str.split('_',1).tolist(),columns = [newPlayerIDstring,newTeamIDstring])
	# Merge new columns with existing dataframe
	df_WithName_and_Team = pd.concat([df, df_temp], axis=1, join='inner')
	
	headers['PlayerID'] = newPlayerIDstring
	headers['TeamID'] = newTeamIDstring

	return df_WithName_and_Team,headers

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




def NP(fname,newfname,folder,cleanedFolder,TeamAstring,TeamBstring):

	with open(folder+fname, 'r') as infile:
		reader = csv.reader(infile)
		headers = list(next(reader))
		# Systematic check to see if the columns appear to be correct (insensitive for capitalization-, and spacing - inconsistencies)
		substringCertain = ['GPS','ideo','eam','ersey','x','y','un','oal','urnover','ass']
		for i in range(10):
			if not substringCertain[i] in headers[i]:
				print(fname)
				warn('\nDid not recognize %s\n' %headers[i])
		substringConsistent = ['GPS timing', 'Video time (s)', 'Team', 'jersey n.', 'x', 'y', 'Run', 'Goal', 'Possession/Turnover', 'Pass']

		if exists(cleanedFolder + newfname):
			warn('Overwriting file <%s>\nwith <%s>' %(newfname,fname))
			
		with open(cleanedFolder + newfname,'w',newline='') as outfile:
			wr = csv.writer(outfile)
			wr.writerow(substringConsistent)
			for row in reader:
				# Strip rows from useless spaces
				row = [s.strip() for s in row]							

				# stop adding rows if row is completely empty
				if not any('' != s for s in row): # Idea: could improve this with isspace()
					break
				# check whether teamstrings are consistent
				# Use the same team string
				if re.search('team a',row[2], re.IGNORECASE) or re.search('teama',row[2], re.IGNORECASE):
					row[2] = TeamAstring
				elif re.search('team b',row[2], re.IGNORECASE) or re.search('teamb',row[2], re.IGNORECASE):
					row[2] = TeamBstring
				elif row[2] != '':
					warn('\ncould not identify team: <%s>' %row[2])
					print(fname)
					print(row)
					pdb.set_trace()

				# omit team label if team row
				###### if row[2] != '' and all('' == s or ' ' == s for s in row[3:6]): # if it's a team row.					
				if row[2] != '' and all(s == '' for s in row[3:6]): # if it's a team row.										
					row[2] = '' 

				# check if given attributes are organized similarly
				###### if any('' != s and ' ' != s for s in row[6:10]): # if it's a team row WITH information.
				if any(s != '' for s in row[6:10]): # if it's a team row WITH information.
					###### if '' != row[6] and ' ' != row[6]: # Run
					if row[6] != '': # Run						
						if not ('nd' in row[6] or 'un' in row[6]):
							warn('\nDidnt recognize run: <%s>' %row[6])
					###### if '' != row[7] and ' ' != row[7] and '  ' != row[7]: # Goal
					if row[7] != '': # Goal
						if not 'oal' in row[7]:
							warn('\ncould not recognize goal: <%s>' %row[7])
							print(fname)
							pdb.set_trace()
						
					for i in [7, 8, 9]: # Goal, Possession / Turnover, Pass
						###### if row[i]'' != row[i] and ' ' != row[i] and '  ' != row[i]: # Goal
						if row[i] != '': # Goal
							if not (TeamAstring[1:] in row[i] or TeamBstring[1:] in row[i]):
								if 'Start A possession' in row[i]:
									row[i] = 'Start %s possession' %TeamAstring
								else:
									warn('\ncould not identify team: <%s>' %row[i])
									print(fname)
									print(row)

									pdb.set_trace()
				# To replace any nearly empty cells
				for idx,val in enumerate(row):
					if val.isspace():
						row[idx] = ''
				wr.writerow(row[0:10]) # 0:9 is to omit any empty/useless headers

def verifyTimestampConsistency(df,TeamAstring,TeamBstring):

	
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

	if len(uniqueTs)*len(uniquePlayerID) == len(df['Ts']):
		tsConsistent = True
	else:
		tsConsistent = False
		warn('WARNING: Timestamp not consistent for every player.\nConsider adding a function in cleanupData.py to add empty rows for missing data.\nBe careful indexing based on timestamp.')
		for i in uniquePlayerID:
			# Take the unique timestamps per player
			uTsPl = pd.unique(df['Ts'][df['PlayerID'] == i])
			print('Player <%s>: Number of observations %ds, min %d, max %ds.' %(i,len(uTsPl),min(uTsPl),max(uTsPl)))

	return tsConsistent

def verifyGroupRows(df_cleaned):

	# Group Rows are the rows where any feature can be stored that captures multiple players (i.e., team or attackers/midfielders/defenders)
	# groupRows should have the 'PlayerID' value 'group'
	
	# First, verify whether groupRows exist by checking if there are rows with empty TeamIDs that are not ball.
	groupRows = (df_cleaned['TeamID'].isnull()) & (df_cleaned['PlayerID'] != 'ball') 
	if df_cleaned['Ts'][(groupRows)].empty:
		groupRows = (df_cleaned['PlayerID'] == 'groupRow')

	if df_cleaned['Ts'][(groupRows)].empty:# and not any(df_cleaned['PlayerID'] == 'groupRow'):
		# If groupRows don't exist, then create them
		# For every existing timestamp
		uniqueTs = pd.unique(df_cleaned['Ts'])
		uniqueTs = np.sort(uniqueTs)
		# Create a string value
		groupPlayerID = ['groupRow' for i in uniqueTs]
		# Create groupIndex by adding to highest existing index
		firstGroupIndex = df_cleaned.index[-1] + 1
		groupIndex = firstGroupIndex + range(len(groupPlayerID))

		# Put these in a DataFrame with the same column headers
		df_group = pd.DataFrame({'Ts':uniqueTs,'PlayerID':groupPlayerID},index = [groupIndex])# possibly add the index ? index = []
		
		# Append them to the existing dataframe
		df_cleaned = df_cleaned.append(df_group)

	else:

		# If they do exist,
		# verify that 'PlayerID' = 'groupRow'
		if any(df_cleaned['PlayerID'][groupRows] != 'groupRow'):		
			# df_cleaned['PlayerID'][groupRows] = 'groupRow'
			df_cleaned.loc[groupRows,('PlayerID')] = 'groupRow'
			warn('\nWARNING: Contents of PlayerID overwritten for group rows.\nBe sure that group rows were identified correctly.\n')
			df_cleaned.to_csv('C:\\Users\\rensm\\Documents\\PostdocLeiden\\BRAxNLD repository\\Data\\tmp\\testOverwrittenGrouprows.csv')

		# and verify that there is a group row for every timestamp.
		if len(df_cleaned['PlayerID'][groupRows]) != len(pd.unique(df_cleaned['Ts'])):
			warn('\nWARNING: Not as many groupRows as unique timestamps.\nConsider including code to fill up missing timestamps in groupRows.\n')

		# And finally, verify whether x, y, and TeamID are empty
		if not all(df_cleaned['X'][(groupRows)].isnull()):
			warn('\nWARNING: X values of groupRows are not empty.\nConisder cleaning. ')
		if not all(df_cleaned['Y'][(groupRows)].isnull()):
			warn('\nWARNING: Y values of groupRows are not empty.\nConisder cleaning. ')
		if not all(df_cleaned['TeamID'][(groupRows)].isnull()):
			warn('\nWARNING: TeamID values of groupRows are not empty.\nConisder cleaning. ')			

	return df_cleaned