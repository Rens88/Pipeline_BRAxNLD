# 06-03-2018 Rens Meerhoff
# A script to derive information from the filename, if available.

# Called from:
# process_Template.py

import pdb; #pdb.set_trace()
import csv
from warnings import warn
import numpy as np
from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs
import re
import pandas as pd
import student_XX_dissectFilename

if __name__ == '__main__':
	
	process(fname,dataType,TeamAstring,TeamBstring)
	FDP(fname)
	NP(fname)
	default(fname)


#########################################################################
def process(fname,dataType,TeamAstring,TeamBstring):
	if dataType == "NP":
		exportData, exportDataString, exportDataFullExplanation,cleanFname = NP(fname)
	elif dataType == "FDP":
		exportData, exportDataString, exportDataFullExplanation,cleanFname,TeamAstring,TeamBstring = FDP(fname)
	else:
		exportData, exportDataString, exportDataFullExplanation,cleanFname = \
		student_XX_dissectFilename.process(fname,dataType,TeamAstring,TeamBstring)
		# exportData, exportDataString, exportDataFullExplanation,cleanFname = default(fname)

	return exportData, exportDataString, exportDataFullExplanation,cleanFname,TeamAstring,TeamBstring

def FDP(fname):	
	# Using regular expression to extract info from filename		
	regex = r'([a-zA-Z]{1})([a-zA-Z]{1})(\d+)_([a-zA-Z]{1})([a-zA-Z]{1})(\d{1})(\d{3})_v_([a-zA-Z]{1})([a-zA-Z]{1})(\d{1})(\d{3})'
	match = re.search(regex,fname)
	if match:
		grp = match.groups()
		MatchContinent = grp[0]
		MatchCountry = grp[1]
		MatchID = grp[2]
		HomeTeamContinent = grp[3]
		HomeTeamCountry = grp[4]
		HomeTeamAgeGroup = grp[5]
		HomeTeamID = grp[6]
		AwayTeamContinent = grp[7]
		AwayTeamCountry = grp[8]
		AwayTeamAgeGroup = grp[9]
		AwayTeamID = grp[10]

		TeamAstring = HomeTeamContinent + HomeTeamCountry + HomeTeamAgeGroup + HomeTeamID
		TeamBstring = AwayTeamContinent + AwayTeamCountry + AwayTeamAgeGroup + AwayTeamID

		# Prepare the tabular export
		exportData = [MatchContinent,MatchCountry,MatchID,HomeTeamContinent,HomeTeamCountry, \
		HomeTeamAgeGroup,HomeTeamID,AwayTeamContinent,AwayTeamCountry,AwayTeamAgeGroup,AwayTeamID]
		exportDataString = ['MatchContinent','MatchCountry','MatchID','HomeTeamContinent','HomeTeamCountry', \
		'HomeTeamAgeGroup','HomeTeamID','AwayTeamContinent','AwayTeamCountry','AwayTeamAgeGroup','AwayTeamID']
		exportDataFullExplanation = ['The continent where the match was played.','The country where the match was played.','The unique identifier of the match.','The continent of the home team.','The country of the home team.', \
		'The age group of the home team.','The unique identifier of the home team.','The continent of the away team.','The country of the away team.', \
		'The age group of the home away.','The unique identifier of the away team.']
	else:
		exportData, exportDataString, exportDataFullExplanation = default(fname)

	cleanFname = fname[:-4] + '_cleaned.csv'
	return exportData, exportDataString, exportDataFullExplanation,cleanFname,TeamAstring,TeamBstring

def default(fname):
	exportData = [fname]
	exportDataString = 'filename'
	exportDataFullExplanation = ['This is simply the complete filename.']
	warn('\nWARNING: Could not identify match characteristics based on filename <%s>.\nInstead, filename itself was exported as match characteristic.' %fname)

	return exportData, exportDataString, exportDataFullExplanation,fname

def NP(fname):
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

	if ' v ' in fname:
		grInd = fname.find(' v ')
		Group = (fname[grInd-1] +  'v' + fname[grInd + 3])
	else:
		warn('\nCould not identify group: <%s>' %fname)

	cleanFname = School + '_' + Class + '_' + Group + '_' + Test + '.csv'

	exportData = [School, Class, Group, Test]
	exportDataString = ['School', 'Class', 'Group', 'Test']
	exportDataFullExplanation = ['School experiment was held at','Class the participants were from','Identifier groups that played each other','Name of the type of trial (PRE = pre-test, POS = post-test, TRA = transfer test, RET = retention test)']

	return exportData, exportDataString, exportDataFullExplanation,cleanFname