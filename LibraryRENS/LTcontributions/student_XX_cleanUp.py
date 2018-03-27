# A student's template.
# To make your module function with the pipeline:
# 0) copy the whole example folder, and replace XX with your initials
# 1) edit the name of the student's function in cleanupData.py (around line 137)
# 2) edit the student function that's imported at the top of cleanupData.py
# 	(where it now says "import student_XX_cleanUp")
# 3) change the string content of <studentFolder> in the 'process_Template' (NB: you can make a copy of process_Template and call it process_StudentXX)

import numpy as np
import math
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
def process(df,df_omitted,TeamAstring,TeamBstring,headers,readAttributeCols,readEventColumns):
	# NB: Keys are standardized
	# - Timestamp (in seconds) = 'Ts'
	# - Locations (in meters) are 'X' and 'Y'
	# - Player identity is 'PlayerID'
	# - Team identity is 'TeamID'

	# This is an example of the type of thing you may want to clean.
	# It omits any row that has no X or Y value.
	# NB: If you exported team attributes through inmotio, the X and Y values might be empty.
	# In that case, you don't want to run them.
	df_cropped01,df_omitted01 = \
	omitXandY_equals0(df)

	df_omitted = pd.concat([df_omitted, df_omitted01]) # Only relevant when cleaning up in multiple steps.
	# Use the cropped df (df_cropped01) to feed into the next function.

	df_croppedLAST = df_cropped01.copy()

	return df_croppedLAST,df_omitted

def omitXandY_equals0(df):
	# Omit rows where both x and y = 0 and where there is no team value
	XandY_equals0 = ( ((df['X'] == 0) & (df['Y'] == 0) & (df['PlayerID'] == 'nan')) ) 
	df[XandY_equals0 == True]
	df_cleaned 	= df[XandY_equals0 == False]
	df_omitted 	= df[XandY_equals0 == True]

	return df_cleaned,df_omitted
