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

if __name__ == '__main__':
	this_is_a_function(PrintThis)

#########################################################################
def this_is_a_function(PrintThis):
	print(PrintThis)
	return 'VPcleanup was executed'
