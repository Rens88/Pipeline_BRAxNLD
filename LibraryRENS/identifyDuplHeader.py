# 21-11-2017 Rens Meerhoff
# Identify duplicate header
# 
# idTS:
# Function to identify which timestamp is in milliseconds and which in 
# LPMtime, as both get the header 'Timestamp' in the CSV file exported by 
# the inMotio software.
#
# idName:
# Distinguish between PlayerID en TeamID.

import pdb; #pdb.set_trace()
from warnings import warn
import numpy as np

if __name__ == '__main__':
	
	idTS(data,tsInds,filename) # tsInds contains the originally allocated column and the duplicate column. The function puts the duplicate in the original column if it was identified as the real deal.
	idName(data,tsInds)

def idName(data,tsInds): # FDP specific
	# if not type(cols) == 'NoneType':
	# tsInds = []
	# for idx,val in enumerate(cols):
	# 	if val == 'Name':
	# 		tsInds.append(idx)
	# if tsInds == []:
	# 	warn('\nCouldnt find duplicate <<Name>>\nEither it is not necessary to look for duplicates,\nor, something is wrong.\n')
	# 	return cols
	# else:
	print('Duplicate <<Name>> allocated')
	
	# print(rawData[3][0:5])
	TS0 = data[tsInds[0]]
	TS1 = data[tsInds[1]]
	if str.isdigit(TS0[0]) and str.isdigit(TS1[0]):
		warn('\n\nDigit in both teamID and playerID.\nCannot be certain about allocation of columns.\nFind a different way to discriminate.')

	if str.isdigit(TS0[0]): # based on assumption that there is a digit in player ID and not in Team ID
		# TS0 = PlayerID
		indPlayerID = tsInds[0]
		indTeamID = tsInds[1]
	elif str.isdigit(TS1[0]):
		# TS1 = PlayerID
		indPlayerID = tsInds[1]
		indTeamID = tsInds[0]
	else:
		warn('\nNo clarity on which is the team. \nConsider expanding conditional statement.\n\
			Script works because the second Name was randomly exported as TeamID\n')
		indPlayerID = tsInds[1]
		indTeamID = tsInds[0]

	data[tsInds[1]] = data[indPlayerID] # Player ID
	data[tsInds[0]] = data[indTeamID] # Player ID

	return data

############################################################################
############################################################################	

def idTS(data,tsInds,filename):

	print('Duplicate <<Timestamp>> allocated')
	TS0 = data[tsInds[0]]
	TS1 = data[tsInds[1]]
	
	# Usually, time in milliseconds is smaller than inmotio time, 
	# because time in milliseconds starts at 0 (in the full file)
	if int(TS0[0]) < int(TS1[0]):
		# TS0 is probably milliseconds
		suspectedMS = tsInds[0]
	else:
	 	# TS0 is probably inmotio time, so TS1 is suspected to be in milliseconds
	 	suspectedMS = tsInds[1] 	

	# For all excerpts, the filename has the first value of time in milliseconds
	if not filename[-6:] == 'ms.csv':
		# If the imported file is NOT an excerpt.
		# NB: in process_timeseriesAttribute.py this is always the case.
		tmin = 0
	else:
		# search for the second 'ms' combination from the end and back
		# DEBUGGING INFO: Note that code to find times in filename are slightly different here compared to LoadOrCreateCSVexcerpt.py (this is better)
		tmp = 1
		for i in range(len(filename)-1):
			if filename[-i-2] + filename[-i-1] == 'ms':
				if tmp == 2:			
					idme = len(filename)-i-3		
				tmp = tmp + 1
			if tmp > 2:
				if filename[-i-3] == '_':
					idms = len(filename)-i-2
					break
		tmin = int(filename[idms:idme+1])
	if int(TS0[0]) == tmin:
		# TS0 certainly in milliseconds
		indTimeMS = tsInds[0]
		indTimeIM = tsInds[1]		
	elif int(TS1[0]) == tmin:
		# TS1 certainly in millisconds
		indTimeMS = tsInds[1]
		indTimeIM = tsInds[0]
	else:
		# Error, no milliseconds identified
		# Default option is to continue with the second timestamp being in milliseconds (as is the case with the current files)
		indTimeMS = tsInds[1]
		indTimeIM = tsInds[0]
		warn('\nTimstamp (ms) could not be found with certainty.\
		\nCheck manually. \
		\nCurrently, the second occurrence of Timestamp is determined to be in millisconds.\n')
	
	# And then one check-up
	if not suspectedMS == indTimeMS:
		warn('\nUncertain whether indTimeMS is indeed time in millisconds.\
			\n InMotion time was somehow smaller than time in millisconds.\
			\n Check with LMPtime in data.\n')

	data[tsInds[0]] = data[indTimeMS] # Timestamp in milliseconds

	return data