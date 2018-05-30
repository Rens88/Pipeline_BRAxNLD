# A student's template.
# To make your module function with the pipeline:
# 0) copy the whole example folder, and replace XX with your initials
# 1) edit the name of the student's function in cleanupData.py (around line 137)
# 2) edit the student function that's imported at the top of cleanupData.py
# 	(where it now says "import student_XX_cleanUp")
# 3) change the string content of <studentFolder> in the 'process_Template' (NB: you can make a copy of process_Template and call it process_StudentXX)

import numpy as np
import math
import csv
import pandas as pd
from warnings import warn
import pdb; #pdb.set_trace()

## Here, you can clarify which functions exist in this module.
if __name__ == '__main__': 
	
	# -- df --
	# Contains the dataframe of the imported data from the 'dirty' file.
	# Each relevant key is stored in headers.

	# -- headers --
	# Contains the keys of the relevant attributes:
	# headers['Ts'] contains the string for time
	# headers['Location'] contains the tuple with the strings for x and y
	# headers['TeamID'] contains the string for the team's identity
	# headers['PlayerID'] contains the string for time the player's identity
	
	process(df,df_omitted,headers)
	omitXandY_equals0(df,x,y,ID)

## Here, you specifiy what each function does
def process(dirtyFname,cleanFname,dataFolder,cleanedFolder,headers,readAttributeCols,debugOmittedRows,readEventColumns,TeamAstring,TeamBstring):
	# NB: Keys are standardized
	# - Timestamp (in seconds) = 'Ts'
	# - Locations (in meters) are 'X' and 'Y'
	# - Player identity is 'PlayerID'
	# - Team identity is 'TeamID'

	# This is an example of the type of thing you may want to clean.
	# It omits any row that has no X or Y value.
	# NB: If you exported team attributes through inmotio, the X and Y values might be empty.
	# In that case, you don't want to run them.
	# df_cropped01,df_omitted01 = \
	# omitXandY_equals0(df)

	df_cleaned,df_omitted,fatalTeamIDissue = KNVB(dirtyFname,cleanFname,dataFolder,cleanedFolder,headers,readAttributeCols,debugOmittedRows,readEventColumns,TeamAstring,TeamBstring)
		

	# df_omitted = pd.concat([df_omitted, df_omitted01]) # Only relevant when cleaning up in multiple steps.
	# Use the cropped df (df_cropped01) to feed into the next function.

	# df_croppedLAST = df_cropped01.copy()

	return df_cleaned,df_omitted,fatalTeamIDissue#,halfTime,secondHalfTime

#LT: TODO: sort values by Ts
def KNVB(fname,cleanFname,dataFolder,cleanedFolder,headers,readAttributeCols,debugOmittedRows,readEventColumns,TeamAstring,TeamBstring):
	expectedVals = (-60,60,-40,40) # This should probably be dataset specific.
	conversion_to_S = .001
	# FDP specific function where the column 'Naam' is seperated into its PlayerID and TeamID
	ts = headers['Ts']
	x,y = headers['Location']
	ID = headers['PlayerID']

	colHeaders = [ts,x,y,ID] + readAttributeCols + readEventColumns
	if headers['TeamID'] != None:
		# If there is a header for 'TeamID', then include it as the colHeaders that will be read from the CSV file.
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
			exit('EXIT: Column header <%s> not in column headers of the file:\n%s\n\nSOLUTION: Change the user input in \'process\' \n' %(i,fileHeaders))
  
	#LT: TODO: sort values by Ts
	df = pd.read_csv(dataFolder+fname,usecols=(colHeaders),low_memory=False)
	df[ts] = df[ts]*conversion_to_S # Convert from ms to s.

	
	## Cleanup for BRAxNLD
	if headers['TeamID'] == None:
		df,headers,fatalTeamIDissue = splitName_and_Team(df,headers,ID,newPlayerIDstring,newTeamIDstring,TeamAstring,TeamBstring)
		# Delete the original ID, but only if the string is not the same as the new Dict.Keys
		if ID != newPlayerIDstring and ID != newTeamIDstring:
			del df[ID]
			# del df_omitted[ID]		
		ID = headers['PlayerID']
	#LT: added!
	else:
		fatalTeamIDissue = False

	#Copy shirtnumbers of opponnents to PlayerID's and multiply with -1
	df = setPlayerID(df,ID,Tid,TeamBstring)

	#delete referees
	refereeIdx = (df[Tid].isnull()) & (df[ID] != 'ball')
	df = df[refereeIdx == False]

	#no negative speed
	speedZero = df['Speed'] < 0.0
	df.loc[speedZero,'Speed'] = 0.0

	df_cropped01,df_omitted01 = omitXandY_equals0(df,x,y,ID)
	df_cropped02,df_omitted02 = omitRowsWithout_XandY(df_cropped01,x,y)	
	df_cropped03,df_omitted03 =	omitRowsWithExtreme_XandY(df_cropped02,x,y,expectedVals)	

	# Idea: by incorporting df_omittedNN in a list, you could make the script work with variable lenghts of omitted dfs.
	df_omitted = pd.concat([df_omitted01, df_omitted02, df_omitted03])
	## End cleanup for BRAxNLD

	#LT: controleren of dit er in moet!
	df_cleaned = df_cropped03
	
	return df_cleaned, df_omitted,fatalTeamIDissue

def setPlayerID(df,ID,Tid,TeamBstring):
	#Copy shirtnumbers of opponnents to PlayerID's and multiply with -1 if they don't have a PlayerID
	teamOppIdx = (df[Tid] == TeamBstring) & (df[ID] == 0)
	df.loc[teamOppIdx,ID] = df.loc[teamOppIdx,'Shirt'] * -1

	#Set ball
	# Ball: Team = NaN, PlrID = 0, Shirt = 1, inBallPoss always 0
	ballIdx = (df[Tid].isnull()) & (df[ID] == 0) & (df['Shirt'] == 1)
	df.loc[ballIdx,ID] = 'ball'

	return df

def omitXandY_equals0(df,x,y,ID):
	# Omit rows where both x and y = 0 and where there is no team value
	# XandY_equals0 = ( ((df[x] == 0) & (df[y] == 0) & (df[ID] == 'nan')) ) 
	# print(df)
	# XandY_equals0 = ( (df[ID] == 'nan') )
	XandY_equals0 = ( ((df[x] == 0) & (df[y] == 0) & (df[ID].isnull())) )
	# pdb.set_trace()
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

# def determineHalfTime(headers,df):
# 	noPlayers = 0
# 	seconds = 60 # 1 minute

# 	halfTime = -1

# 	for idx,i in enumerate(pd.unique(df[headers['Ts']])):
# 		curTime = df.iloc[idx][headers['Ts']]
# 		curX = df['X'][df[headers['Ts']] == i]
# 		if all(curX.isnull()):
# 			noPlayers = noPlayers + 1
# 		else:
# 			noPlayers = 0

# 		if (noPlayers == seconds):
# 			halfTime = i - (seconds/10) #10 Hz

# 		if halfTime > 0 and all(curX.notnull()):#LT: maybe also for 60 seconds?
# 			secondHalfTime = i #- (seconds/10)

# 	return halfTime, secondHalfTime

# def omitXandY_equals0(df):
# 	# Omit rows where both x and y = 0 and where there is no team value
# 	XandY_equals0 = ( ((df['X'] == 0) & (df['Y'] == 0) & (df['PlayerID'] == 'nan')) ) 
# 	df[XandY_equals0 == True]
# 	df_cleaned 	= df[XandY_equals0 == False]
# 	df_omitted 	= df[XandY_equals0 == True]

# 	return df_cleaned,df_omitted