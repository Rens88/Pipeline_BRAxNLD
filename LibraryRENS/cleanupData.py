# 29-01-2017 Rens Meerhoff
# This function pre-processes the data by allocating a systematic filename, discarding empty rows 
# and columns (at the end of a file only), and it does a quick scan of the consistency of string inputs.

import pdb; #pdb.set_trace()
import csv
from warnings import warn
import numpy as np
from os.path import isfile, join, isdir
from os import listdir, startfile

if __name__ == '__main__':
	
	# Clean up based on to be expected characteristics of the dataset.
	# Organized per database:

	FDP(dataFiles,folder,cleanedFolder,TeamAstring,TeamBstring)
	# Nonlinear Pedagogy
	NP(dataFiles,folder,cleanedFolder,TeamAstring,TeamBstring)

#########################################################################

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
			elif 'St Pats' in fname:
				School = 'StPt'
				if '13' in fname:
					Class = 'X13'
				else:
					warn('\nCould not identify class: <%s>' %fname)				
			else:
				warn('\nCould not identify School: <%s>' %fname)

			# Test
			if 'Retention' in fname:
				Test = 'RET'
			elif 'Pre test' in fname:
				Test = 'PRE'
			elif 'Post test' in fname:
				Test = 'POS'
			elif 'T1' in fname or 'Transfer' in fname:
				Test = 'TRA'
			else:
				warn('\nCould not identify Test: <%s>' %fname)
			# Group
			if ' v ' in fname:
				grInd = fname.find(' v ')
				Group = (fname[grInd-1] +  'v' + fname[grInd + 3])
			else:
				warn('\nCould not identify group: <%s>' %fname)

			newfname = School + '_' + Class + '_' + Group + '_' + Test + '.csv'
			with open(cleanedFolder + newfname,'w',newline='') as outfile:
				wr = csv.writer(outfile)
				wr.writerow(substringConsistent)
				for row in reader:									
					# stop adding rows if row is completely empty
					if not any('' != s for s in row):
						break
					# check whether teamstrings are consistent
					# Use the same team string
					if row[2] == 'TeamA' or row[2] == 'teamA' or row[2] == 'teamA ':
						row[2] = TeamAstring
					elif row[2] == 'TeamB' or row[2] == 'teamB' or row[2] == 'teamB ':
						row[2] = TeamBstring
					
					# omit team label if team row
					if row[2] != '' and all('' == s or ' ' == s for s in row[3:6]): # if it's a team row.					
						# print(row)
						row[2] = '' 
						# print(row)
					# check if given attributes are organized similarly
					if any('' != s and ' ' != s for s in row[6:10]): # if it's a team row WITH information.
						if '' != row[6] and ' ' != row[6]: # Run
							if not ('nd' in row[6] or 'un' in row[6]):
								warn('\nDidnt recognize run: <%s>' %row[6])
						if '' != row[7] and ' ' != row[7]: # Goal
							if not 'oal' in row[7]:
								warn('\ncould not recognize goal: <%s>' %row[7])
								print(fname)
							
						for i in [7, 8, 9]: # Goal, Possession / Turnover, Pass
							if '' != row[i] and ' ' != row[i]: # Goal
								if not (TeamAstring[1:] in row[i] or TeamBstring[1:] in row[i]):
									if 'Start A possession' in row[i]:
										row[i] = 'Start %s possession' %TeamAstring
									else:
										warn('\ncould not identify team: <%s>' %row[i])
										print(fname)
					# To replace any nearly empty cells
					for idx,val in enumerate(row):
						if val == ' ':
							row[idx] = ''
					wr.writerow(row[0:10]) # 0:9 is to omit any empty/useless headers