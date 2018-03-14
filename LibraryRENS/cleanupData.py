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
import student_XX_cleanUp

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
def process(dirtyFname,cleanFname,dataType,dataFolder,cleanedFolder,TeamAstring,TeamBstring,headers,readAttributeCols,timestampString,readEventColumns,conversionToMeter):

	debugOmittedRows = False # Optional export of data that was omitted in the cleaning process
	# Clean up data, if necessary
	cleanFnames = [f for f in listdir(cleanedFolder) if isfile(join(cleanedFolder, f)) if '.csv' in f]

	if cleanFname in cleanFnames:
		warn('\nContinued with previously cleaned data.\nIf problems exist with data consistency, consider writing a function in cleanupData.py.\n')
		return cleanedFolder, readAttributeCols#, attrLabel
	else: # create a new clean Fname
		print('\nCleaning up file...')
		if dataType == "NP":
			# NB: cleanupData currently dataset specific (NP or FDP). Fixes are quite specific and may not easily transfer to different datasets.
			df_cleaned,df_omitted,headers,readAttributeCols,readEventColumns = \
			NP(dirtyFname,cleanFname,dataFolder,cleanedFolder,headers,readAttributeCols,debugOmittedRows,readEventColumns,TeamAstring,TeamBstring)
		elif dataType == "FDP":
			df_cleaned,df_omitted = FDP(dirtyFname,cleanFname,dataFolder,cleanedFolder,headers,readAttributeCols,debugOmittedRows,readEventColumns)
		else:
			# overwrite cleanedFolder and add a warning that no cleanup had taken place
			cleanedFolder = dataFolder
			exit('\exit: No clean-up function available for file <%s>.\nContinued without cleaning the data.' %dirtyFname)

		## Genereic clean up function (for all datasets)
		# First: Rename columns to be standardized.
		# From now onward, standardized columnNames can be used.
		df_cleaned = standardizeColumnHeaders(df_cleaned,headers)

		# Convert to meters
		df_cleaned = convertToMeters(df_cleaned,conversionToMeter)

		# Confirm whether Every timestamp occurs equally often, to enable indexing based on timestamp
		tsConsistent = verifyTimestampConsistency(df_cleaned,TeamAstring,TeamBstring)
		if not tsConsistent:
			warn('\nTO DO: Timestamp is not consistent: \nWrite the code to smooth out timestamp.')

		# Check if there is already a set of rows for team values (i.e., rows without playerID that are not 'ball')
		# df_cleaned.to_csv('C:\\Users\\rensm\\Documents\\PostdocLeiden\\NP repository\\Output\\test.csv')
		df_cleaned = verifyGroupRows(df_cleaned)
		
		df_cleaned,df_omitted = \
		student_XX_cleanUp.process(df_cleaned,df_omitted,TeamAstring,TeamBstring,headers,readAttributeCols,readEventColumns)

		if exists(cleanedFolder + cleanFname):
			warn('\nOverwriting file <%s> \nin cleanedFolder <%s>.\n' %(cleanFname,cleanedFolder))

		# Export cleaned data to CSV
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

	return cleanedFolder, readAttributeCols#, attrLabel

def FDP(fname,cleanFname,dataFolder,cleanedFolder,headers,readAttributeCols,debugOmittedRows,readEventColumns):

	expectedVals = (-60,60,-40,40) # This should probably be dataset specific.
	conversion_to_S = .001
	# FDP specific function where the column 'Naam' is seperated into its PlayerID and TeamID
	ts = headers['Ts']
	x,y = headers['Location']
	ID = headers['PlayerID']
	colHeaders = [ts,x,y,ID] + readAttributeCols + readEventColumns
	newPlayerIDstring = 'Player'
	newTeamIDstring = 'Team'

	# Only read the headers as a check-up:
	with open(dataFolder+fname, 'r') as f:
		reader = csv.reader(f)
		tmpHeaders = list(next(reader))

	for i in colHeaders:
		if not i in tmpHeaders:
			exit('EXIT: Column header <%s> not in column headers of the file:\n%s\n\nSOLUTION: Change the user input in \'process\' \n' %(i,tmpHeaders))
  
	df = pd.read_csv(dataFolder+fname,usecols=(colHeaders),low_memory=False)
	df[ts] = df[ts]*conversion_to_S # Convert from ms to s.
	
	## Cleanup for BRAxNLD
	if headers['TeamID'] == None:
		df_WithName_and_Team,headers = splitName_and_Team(df,headers,ID,newPlayerIDstring,newTeamIDstring)
		# Delete the original ID, but only if the string is not the same as the new Dict.Keys
		if ID != newPlayerIDstring and ID != newTeamIDstring:
			del df_WithName_and_Team[ID]
			# del df_omitted[ID]		
		ID = headers['PlayerID']
		df = df_WithName_and_Team

	df_cropped01,df_omitted01 = omitXandY_equals0(df,x,y,ID)
	df_cropped02,df_omitted02 = omitRowsWithout_XandY(df_cropped01,x,y)	
	df_cropped03,df_omitted03 =	omitRowsWithExtreme_XandY(df_cropped02,x,y,expectedVals)	

	# Idea: by incorporting df_omittedNN in a list, you could make the script work with variable lenghts of omitted dfs.
	df_omitted = pd.concat([df_omitted01, df_omitted02, df_omitted03])
	## End cleanup for BRAxNLD

	df_cleaned = df_cropped03

	return df_cleaned, df_omitted

def NP(fname,newfname,folder,cleanedFolder,headers,readAttributeCols,debugOmittedRows,readEventColumns,TeamAstring,TeamBstring):

	# Only read the headers as a check-up:
	with open(folder+fname, 'r') as f:
		reader = csv.reader(f)
		tmpHeaders = list(next(reader))

	colHeaders,posIdx = \
	checkKnownExceptions_colHeaders(tmpHeaders,headers,readAttributeCols,readEventColumns)

	for i in colHeaders:
		if not [True for j in tmpHeaders if re.search(i,j)]:
			exit('EXIT: Column header <%s> not in column headers of the file:\n%s\n\nSOLUTION: Change the user input in \'process\' \n' %(i,tmpHeaders))

	df = pd.read_csv(folder+fname,usecols=(colHeaders),low_memory=False,skip_blank_lines  = True,skipinitialspace  = True)

	df,readEventColumns = removeSpacesColheaders(df,readEventColumns,posIdx)	

	# Convert timestamp to seconds
	df = convertHHMMSS_to_s(df,headers['Ts'])

	# teamstring consistency (in team ID)
	df = checkTeamString_withoutSpaces_ignoringCase(df,headers['TeamID'],TeamAstring,TeamBstring)
	
	# Check the group rows that contain information.
	df = checkGroupRows_withInformation(df,headers,readEventColumns,TeamAstring,TeamBstring)

	# Omit rows without value
	df_cleaned,df_omitted = omitRowsWithoutValues(df)

	# Idea: by incorporting df_omittedNN in a list, you could make the script work with variable lenghts of omitted dfs.
	# df_omitted = pd.concat([df_omitted01, df_omitted02, df_omitted03])

	return df_cleaned,df_omitted,headers,readAttributeCols,readEventColumns

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

	if len(uniqueTs)*len(uniquePlayerID) == len(df['Ts']):
		tsConsistent = True
	else:
		tsConsistent = False
		warn('WARNING: Timestamp not consistent for every player.\nConsider adding a function in cleanupData.py to add empty rows for missing data.\nBe careful indexing based on timestamp.\nConsider interpolation to make sure timestamps match up.\n')
		for i in uniquePlayerID:
			# Take the unique timestamps per player
			uTsPl = pd.unique(df['Ts'][df['PlayerID'] == i])
			print('Player <%s>: Number of observations %ds, min %d, max %ds.' %(i,len(uTsPl),min(uTsPl),max(uTsPl)))
			# print('\nConsider interpolation to make sure timestamps match up.\n')
	
	return tsConsistent

def verifyGroupRows(df_cleaned):

	# Group Rows are the rows where any feature can be stored that captures multiple players (i.e., team or attackers/midfielders/defenders)
	# groupRows should have the 'PlayerID' value 'groupRow'
	
	# First, verify whether groupRows exist by checking if there are rows with empty TeamIDs that are not ball.
	# df_cleaned['PlayerID'] = str(df_cleaned['PlayerID'])
		
	# df_cleaned.to_csv('C:\\Users\\rensm\\Documents\\PostdocLeiden\\NP repository\\Output\\test.csv')

	# Grouprows - at this stage - are characterized by:
	# 1) a 'TeamID' that isnull()
	groupRows = (df_cleaned['TeamID'].isnull())# & (str(df_cleaned['PlayerID']) != 'ball') 

	# 2) a 'PlayerID' that is not a 'ball'	
	if df_cleaned['PlayerID'].dtype != float:
		# groupRows = (df_cleaned['TeamID'].isnull()) & (str(df_cleaned['PlayerID']) != 'ball') 		
		groupRows = (groupRows) & (df_cleaned['PlayerID'] != 'ball')
	# print(df_cleaned['Ts'][(groupRows)].empty)
	# pdb.set_trace()
	# if not df_cleaned['Ts'][(groupRows)].empty:
	# 	groupRows = (df_cleaned['PlayerID'] == 'groupRow')
	# else:
	# 	groupRows = (df_cleaned['TeamID'].isnull()) & (df_cleaned['PlayerID'] != 'ball')
	# pdb.set_trace()

	# When there are no group rows, they need to be created for every unique timestamp.
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

	else: # If group rows do exist,
		
		# verify that 'PlayerID' = 'groupRow'
		if df_cleaned['PlayerID'].dtype == float: # Input was not a string, so no groupRows indicated.
			df_cleaned.loc[groupRows,('PlayerID')] = 'groupRow'

		elif any(df_cleaned['PlayerID'][groupRows] != 'groupRow'):		
			# df_cleaned['PlayerID'][groupRows] = 'groupRow'
			warn('\nWARNING: Contents of PlayerID overwritten for group rows.\nBe sure that group rows were identified correctly.\n')
			df_cleaned.loc[groupRows,('PlayerID')] = 'groupRow'

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

def omitRowsWithoutValues(df):
	# Omit rows that have no x and y value.
	# test = [df[i].notnull() for i in colHeaders]
	emptyRows = pd.DataFrame(data = [],index = df.index, columns = ['emptyRows'],dtype = 'bool')
 
	for idx in df.index:
		test = [pd.notnull(df[i][idx]) for i in df.keys()]
		if any(test):
			# Row not empty
			emptyRows['emptyRows'] = False
		else:
			# Empty row
			emptyRows['emptyRows'] = True

	df_cleaned = df[emptyRows['emptyRows'] == False ]
	df_omitted = df[emptyRows['emptyRows'] == True  ]

	return df_cleaned, df_omitted

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

def checkKnownExceptions_colHeaders(tmpHeaders,headers,readAttributeCols,readEventColumns):
	# Known exceptions:
	timeException = [True for j in tmpHeaders if re.search('Video timing',j,re.IGNORECASE)]
	if timeException:
		headers['Ts'] = 'Video timing'

	posIdx = [idx for idx,i in enumerate(readEventColumns) if 'Possession' in i]

	possessionException01 = [True for j in tmpHeaders if re.search('Possession / Turnover',j,re.IGNORECASE)]
	if possessionException01 and not posIdx == []:
		readEventColumns[posIdx[0]] = 'Possession / Turnover'

	possessionException02 = [True for j in tmpHeaders if re.search('Possession/ Turnover',j,re.IGNORECASE)]
	if possessionException02 and not posIdx == []:
		readEventColumns[posIdx[0]] = 'Possession/ Turnover'
	
	for j in tmpHeaders:
		if j[-1] == ' ':
			if headers['Ts'] in j:
				headers['Ts'] = headers['Ts'] + ' '
			elif headers['Location'][0] in j:
				headers['Location'][0] = headers['Location'][0] + ' '
			elif headers['Location'][1] in j:
				headers['Location'][1] = headers['Location'][1] + ' '
			elif headers['PlayerID'] in j:
				headers['PlayerID'] = headers['PlayerID'] + ' '
			elif headers['TeamID'] in j:
				headers['TeamID'] = headers['TeamID'] + ' '
			else:
				idxAttr = [idx for idx,i in enumerate(readAttributeCols) if i in j]
				idxEvent = [idx for idx,i in enumerate(readEventColumns) if i in j]
				if len(idxAttr) != 0:
					readAttributeCols[idxAttr[0]] = readAttributeCols[idxAttr[0]] + ' '
				elif len(idxEvent) != 0:
					readEventColumns[idxEvent[0]] = readEventColumns[idxEvent[0]] + ' '

	ts = headers['Ts']
	x,y = headers['Location']
	ID = headers['PlayerID']
	Team = headers['TeamID']
	colHeaders = [ts,x,y,ID,Team] + readAttributeCols + readEventColumns

	return colHeaders,posIdx

def removeSpacesColheaders(df,readEventColumns,posIdx):

	if posIdx != []:
		# Could do the same for any other column headers that are inconsistent
		df.rename(columns={readEventColumns[posIdx[0]]: "Possession"}, inplace=True)
		readEventColumns[posIdx[0]] = 'Possession'

	runIdx = [idx for idx,i in enumerate(readEventColumns) if 'Run' in i]
	if runIdx != []:
		# Could do the same for any other column headers that are inconsistent
		df.rename(columns={readEventColumns[runIdx[0]]: "Run"}, inplace=True)
		readEventColumns[runIdx[0]] = 'Run'

	goalIdx = [idx for idx,i in enumerate(readEventColumns) if 'Goal' in i]
	if goalIdx != []:
		# Could do the same for any other column headers that are inconsistent
		df.rename(columns={readEventColumns[goalIdx[0]]: "Goal"}, inplace=True)
		readEventColumns[goalIdx[0]] = 'Goal'

	passIdx = [idx for idx,i in enumerate(readEventColumns) if 'Pass' in i]
	if passIdx != []:
		# Could do the same for any other column headers that are inconsistent
		df.rename(columns={readEventColumns[passIdx[0]]: "Pass"}, inplace=True)
		readEventColumns[passIdx[0]] = 'Pass'

	return df,readEventColumns

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

		elif 'Possession' == i:
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