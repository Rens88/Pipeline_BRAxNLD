# A student's template.
# To make your module function with the pipeline:
# 0) copy the whole example folder, and replace XX with your initials
# 1) edit the name of the student's function in discetFilename.py (around line 32)
# 2) edit the student function that's imported at the top of discetFilename.py
# 	(where it now says "import student_XX_dissectFilename")
# 3) change the string content of <studentFolder> in the 'process_Template' (NB: you can make a copy of process_Template and call it process_StudentXX)

import numpy as np
import math
import re
import pandas as pd
from warnings import warn
import pdb; #pdb.set_trace()

## Here, you can clarify which functions exist in this module.
if __name__ == '__main__': 
	
	# -- fname --
	# Contains the filename string
	
	# -- dataType --
	# Contains the code that you can use to make the analysis specific to your dataType

	# -- TeamAstring --
	# Contains the strings of the Team A as they should be in the dataset.

	# -- TeamAstring --
	# Contains the strings of the Team B as they should be in the dataset.	
	process(fname,dataType,TeamAstring,TeamBstring)

	default(fname)

## Here, you specifiy what each function does
def process(fname,dataType,TeamAstring,TeamBstring):
	# - fname contains the filename string
	# - dataType contains the code that you can use to make the analysis specific to your dataType
	# - TeamAstring and TeamBstring contain the strings of the teams as they should be in the dataset.

	# This is the default option of how to dissect the filename.
	exportData, exportDataString, exportDataFullExplanation,cleanFname,TeamAstring,TeamBstring = KNVB(fname)
	# In dissectFilename.py you can find more advanced examples that use regular expressions.

	return exportData, exportDataString, exportDataFullExplanation,cleanFname,TeamAstring,TeamBstring

def KNVB(fname):	
	# Using regular expression to extract info from filename		
	regex = r'(\d{8})_([a-zA-Z\s\d]*)_([a-zA-Z\s\d]*)'
	match = re.search(regex,fname)
	if match:
		grp = match.groups()
		MatchDate = grp[0]
		HomeTeam = grp[1]
		AwayTeam = grp[2]
		TeamAstring = HomeTeam
		TeamBstring = AwayTeam

		# Prepare the tabular export
		exportData = [MatchDate,HomeTeam,AwayTeam]
		exportDataString = ['MatchDate','HomeTeam','AwayTeam']
		exportDataFullExplanation = ['Date of the match.','Home team.','Away Team.']
	else:
		exportData, exportDataString, exportDataFullExplanation, fname = default(fname)

	cleanFname = fname[:-4] + '_cleaned.csv'

	return exportData, exportDataString, exportDataFullExplanation,cleanFname,TeamAstring,TeamBstring

def default(fname):
	exportData = [fname]
	exportDataString = 'filename'
	exportDataFullExplanation = ['This is simply the complete filename.']
	warn('\nWARNING: Could not identify match characteristics based on filename <%s>.\nInstead, filename itself was exported as match characteristic.' %fname)

	return exportData, exportDataString, exportDataFullExplanation, fname
