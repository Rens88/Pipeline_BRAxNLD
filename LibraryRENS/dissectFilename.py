# 06-03-2018 Rens Meerhoff
# A script to derive information from the filename, if available.

# Called from:
# process_Template.py

import pdb; #pdb.set_trace()
import csv
from warnings import warn
import numpy as np
from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs, sep
import re
import pandas as pd
import student_XX_dissectFilename
import time


if __name__ == '__main__':
	
	process(fname,dataType,TeamAstring,TeamBstring)
	FDP(fname)
	NP(fname)
	default(fname)


#########################################################################
def process(fname,dataType,TeamAstring,TeamBstring,debuggingMode):

	tDissectFilename = time.time()	# do stuff

	if dataType == "NP":
		exportData, exportDataString, exportDataFullExplanation,cleanFname = NP(fname)

	elif dataType == "FDP":
		exportData, exportDataString, exportDataFullExplanation,cleanFname,TeamAstring,TeamBstring = FDP(fname)

	else:
		exportData, exportDataString, exportDataFullExplanation,cleanFname = \
		student_XX_dissectFilename.process(fname,dataType,TeamAstring,TeamBstring)
		# exportData, exportDataString, exportDataFullExplanation,cleanFname = default(fname)
	
	# if the raw data was organized in subfolders, this is the way to omit the last subfolders
	cleanFname = cleanFname.split(sep)[-1]

	spatAggFname = 'TimeseriesAttributes_' + cleanFname

	if debuggingMode:
		elapsed = str(round(time.time() - tDissectFilename, 2))
		print('***** Time elapsed during dissectFilename: %ss' %elapsed)
	return exportData, exportDataString, exportDataFullExplanation,cleanFname,spatAggFname,TeamAstring,TeamBstring

def FDP(fname):	

	cleanFname = fname[:-4] + '_cleaned.csv'

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
		coding_newStyle = fname.split('_')
		if coding_newStyle[0] == 'CROPPED':
			coding_newStyle = coding_newStyle[1:]

		if len(coding_newStyle) == 3:
			MatchID = coding_newStyle[0]
			Competition = coding_newStyle[1]
			Season = coding_newStyle[2][:-4]

			exportData = [MatchID, Competition, Season]
			exportDataString = ['MatchID','Competition','Season']
			exportDataFullExplanation = ['Unique identifier of the match.','Type of competition (ERE / EL / NLCUP)','Season (roman numerals).']

		else:
			# If none of these match, then just go for the default option where the filename is used as the match identifier.
			exportData, exportDataString, exportDataFullExplanation = default(fname)

		TeamAstring = None
		TeamBstring = None
	return exportData, exportDataString, exportDataFullExplanation,cleanFname,TeamAstring,TeamBstring

def default(fname):
	exportData = [fname]
	exportDataString = ['filename']
	exportDataFullExplanation = ['This is simply the complete filename.']
	warn('\nWARNING: Could not identify match characteristics based on filename <%s>.\nInstead, filename itself was exported as match characteristic.' %fname)

	return exportData, exportDataString, exportDataFullExplanation

def NP(fname):
	# Trial parameters:
	# School & Class
	if 'JYSS' in fname:
		School = 'JYSS'
		if '1E' in fname:
			Class = fname[fname.find('1E'):fname.find('1E')+3]
			if Class in ['1E3', '1E4']:
				Exp = 'NP'
			elif Class in ['1E1', '1E2']:
				Exp = 'LP'
			else:
				warn('\nWARNING: Could not identify experimental gruop: <%s>' %fname)				

		else:
			warn('\nWARNING: Could not identify class: <%s>' %fname)
	elif 'St Pat' in fname:
		School = 'StPt'
		
		classStrings = ['1A','1E','12','13']
		Class = ['X' + i for i in classStrings if i in fname]
		if Class == [] or len(Class) > 1:
			warn('\nCould not identify class: <%s>' %fname)				
		Class = Class[0]
		if Class in ['X1A', 'X13']:
			Exp = 'NP'
		elif Class in ['X1E', 'X12']:
			Exp = 'LP'
		else:
			warn('\nWARNING: Could not identify experimental gruop: <%s>' %fname)

	else:
		warn('\nWARNING: Could not identify School: <%s>' %fname)
	
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
		warn('\nWARNING: Could not identify Test: <%s>' %fname)

	if ' v ' in fname:
		grInd = fname.find(' v ')
		Group = (fname[grInd-1] +  'v' + fname[grInd + 3])
	else:
		warn('\nWARNING: Could not identify group: <%s>' %fname)

	cleanFname = School + '_' + Class + '_' + Group + '_' + Test + '_' + Exp + '.csv'

	exportData = [str(School), str(Class), str(Group), str(Test), str(Exp)]
	exportDataString = ['School', 'Class', 'Group', 'Test', 'Exp']
	exportDataFullExplanation = ['School experiment was held at','Class the participants were from','Identifier groups that played each other','Name of the type of trial (PRE = pre-test, POS = post-test, TRA = transfer test, RET = retention test)', 'Experimental group (LP = Linear Pedagogy, NP = Nonlinear Pedgagogy)']

	return exportData, exportDataString, exportDataFullExplanation,cleanFname