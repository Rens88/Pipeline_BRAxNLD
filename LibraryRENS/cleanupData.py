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
	
	# Clean up based on to be expected characteristics of the dataset.
	# Organized per database:

	# Football Data Project --> .csv from inmotio "SpecialExport.csv"
	FDP(DirtyDataFiles,dataFolder,cleanedFolder,headers,readAttributeCols)

	splitName_and_Team(df,headers,ID)
	omitXandY_equals0(df,x,y,ID)
	omitRowsWithout_XandY(df,x,y)	

	# Nonlinear Pedagogy
	NP(dataFiles,folder,cleanedFolder,TeamAstring,TeamBstring)

#########################################################################
def FDP(DirtyDataFiles,dataFolder,cleanedFolder,headers,readAttributeCols,debugOmittedRows):

	# FDP specific function where the column 'Naam' is seperated into its PlayerID and TeamID
	ts = headers['Ts']
	x,y = headers['Location']
	ID = headers['PlayerID']
	colHeaders = [ts,x,y,ID] + readAttributeCols

	for fname in DirtyDataFiles:
		df = pd.read_csv(dataFolder+fname,usecols=(colHeaders),low_memory=False)

		## Cleanup for BRAxNLD
		if headers['TeamID'] == None:
			df_WithName_and_Team,headersUpdated = splitName_and_Team(df,headers,ID )

			df_cleaned01,df_omitted01 = omitXandY_equals0(df_WithName_and_Team,x,y,ID)
			df_cleaned02,df_omitted02 = omitRowsWithout_XandY(df_cleaned01,x,y)	

			# Idea: by incorporting df_omittedNN in a list, you could make the script work with variable lenghts of omitted dfs.
			df_omitted = pd.concat([df_omitted01, df_omitted02])

			# Delete the original ID, but only if the string is not the same as the new Dict.Keys
			if ID != 'PlayerID' and ID != 'TeamID':
				del df_cleaned02[ID]
				del df_omitted[ID]
		## End clenaup for BRAxNLD

		PrintThisToo = VPcleanup.this_is_a_function('\nThis text will be printed.\n')
		print(PrintThisToo)
		PrintThisToo = LTcleanup.this_is_a_function('\nThis text will be printed.\n')
		print(PrintThisToo)

		df_cleaned = df_cleaned02

		newFname = fname[:-4] + '_cleaned.csv'
		if exists(cleanedFolder + newFname):
			warn('Overwriting file <%s> in cleanedFolder <%s>.' %(newFname,cleanedFolder))
		
		# Export cleaned data to CSV
		df_cleaned.to_csv(cleanedFolder + newFname)

		# Optional: Export data that has been omitted, in case you suspect that relevent rows were omitted.
		if debugOmittedRows:
			if not exists(dataFolder + 'Omitted\\'):
				makedirs(dataFolder + 'Omitted\\')
			df_omitted.to_csv(dataFolder + 'Omitted\\' + fname[:-4] + '_omitted.csv')

	return headers

def NP(dataFiles,folder,cleanedFolder,TeamAstring,TeamBstring):

	for fname in dataFiles:
		with open(folder+fname, 'r') as infile:
			reader = csv.reader(infile)
			headers = list(next(reader))
			# NB: Column headers can be visually checked by uncommenting:
			# print(fname)
			# print(headers)
			# Systematic check to see if the columns appear to be correct (insensitive for capitalization-, and spacing - inconsistencies)
			substringCertain = ['GPS','ideo','eam','ersey','x','y','un','oal','urnover','ass']
			for i in range(10):
				if not substringCertain[i] in headers[i]:
					print(fname)
					warn('\nDid not recognize %s\n' %headers[i])
			substringConsistent = ['GPS timing', 'Video time (s)', 'Team', 'jersey n.', 'x', 'y', 'Run', 'Goal', 'Possession/Turnover', 'Pass']

			# Trial parameters:
			# School & Class
			if 'JYSS' in fname:
				School = 'JYSS'
				if '1E' in fname:
					Class = fname[fname.find('1E'):fname.find('1E')+3]
				else:
					warn('\nCould not identify class: <%s>' %fname)
			elif 'St Pat' in fname:
				School = 'StPt'
				
				classStrings = ['1A','1E','12','13']
				Class = ['X' + i for i in classStrings if i in fname]
				if Class == [] or len(Class) > 1:
					warn('\nCould not identify class: <%s>' %fname)				
				Class = Class[0]
			else:
				warn('\nCould not identify School: <%s>' %fname)

			
			# Test
			if re.search('ret',fname, re.IGNORECASE):
				Test = 'RET'
			elif re.search('pre',fname, re.IGNORECASE):
				Test = 'PRE'
			elif re.search('tra',fname, re.IGNORECASE) or re.search('T1',fname):
				Test = 'TRA'
			elif re.search('pos',fname, re.IGNORECASE):
				Test = 'POS'
			else:
				warn('\nCould not identify Test: <%s>' %fname)


			# if 'Retention' in fname:
			# 	Test = 'RET'
			# elif 'Pre test' in fname:
			# 	Test = 'PRE'
			# elif 'Post test' in fname:
			# 	Test = 'POS'
			# elif 'T1' in fname or 'Transfer' in fname:
			# 	Test = 'TRA'
			# else:
			# 	warn('\nCould not identify Test: <%s>' %fname)
			# Group
			if ' v ' in fname:
				grInd = fname.find(' v ')
				Group = (fname[grInd-1] +  'v' + fname[grInd + 3])
			else:
				warn('\nCould not identify group: <%s>' %fname)

			newfname = School + '_' + Class + '_' + Group + '_' + Test + '.csv'
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

					# if row[2] == 'TeamA' or row[2] == 'teamA' or row[2] == 'teamA 'or row[2] == 'team A ' or row[2] == 'Team A ':
					# 	row[2] = TeamAstring
					# elif row[2] == 'TeamB' or row[2] == 'teamB' or row[2] == 'teamB ' or row[2] == 'team B ' or row[2] == 'Team B ':
					# 	row[2] = TeamBstring
					
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




def splitName_and_Team(df,headers,ID):
	newPlayerIDstring = ['PlayerID']
	newTeamIDstring = ['TeamID']

	# Force all values to be a string
	df[ID] = df[ID].apply(str)
	# Split 'Naam' into PlayerID and TeamID
	df_temp = pd.DataFrame(df[ID].str.split('_',1).tolist(),columns = [newPlayerIDstring,newTeamIDstring])
	# Merge new columns with existing dataframe
	df_WithName_and_Team = pd.concat([df, df_temp], axis=1, join='inner')
	
	# df_omitted = pd.concat([df_halfOmitted, df_otherHalfOmitted])

	# # Delete the original ID, but only if the string is not the same as the new Dict.Keys
	# if ID != 'PlayerID' and ID != 'TeamID':
	# 	del df_cleaned[ID]
	# 	del df_omitted[ID]

	headersOut = headers.copy()
	headersOut['PlayerID'] = newPlayerIDstring
	headersOut['TeamID'] = newTeamIDstring

	return df_WithName_and_Team,headers

def omitXandY_equals0(df,x,y,ID):
	# Omit rows where both x and y = 0 and where there is no team value
	df_cleaned 	= df[( ((df[x] == 0) & (df[y] == 0) & (df[ID] == 'nan')) ) == False]
	df_omitted 	= df[( ((df[x] == 0) & (df[y] == 0) & (df[ID] == 'nan')) ) == True]

	return df_cleaned,df_omitted

def omitRowsWithout_XandY(df,x,y):
	# Omit rows that have no x and y value.
	df_cleaned = df[( (df[x].notnull()) & (df[y].notnull()) ) == True ]
	df_omitted = df[( (df[x].notnull()) & (df[y].notnull()) ) == False ]

	return df_cleaned,df_omitted


