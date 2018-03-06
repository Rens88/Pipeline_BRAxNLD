# an example script where you can include specific cleanup procedures

# Called from:
# process_Template.py
# cleanupData.py

import pdb; #pdb.set_trace()
import csv
from warnings import warn
import numpy as np
from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs
import re
import pandas as pd
import cleanupData

if __name__ == '__main__':
	this_is_a_function(PrintThis)

#########################################################################
def this_is_a_function(PrintThis):
	print(PrintThis)

	## You could also use function of the modul cleanupData:
	# splitName_and_Team(df,headers,ID)
	# omitXandY_equals0(df,x,y,ID)
	# omitRowsWithout_XandY(df,x,y)
	return 'VPcleanup was executed'
