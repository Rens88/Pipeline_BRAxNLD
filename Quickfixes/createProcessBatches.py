# quick work-around to create process files
from os.path import isfile, join, isdir, exists
import numpy as np

#################################
### User input
batchFolder = 'C:\\Users\\rensm\\Documents\\GitHub\\Pipeline_BRAxNLD\\ProcessBRxNL_iterateWindows\\'
nBatches = 15
#################################


theTemplate = batchFolder + 'processBatches_Template.py'

if not isfile(theTemplate):
	print('Could not create the batches. Put a file named <processBatches_Template.py> in batchFolder <%s>.' %batchFolder)
	exit()

for n in np.arange(1,nBatches+1):

	curFile = 'process_BRxNL_iterateWindows_%s-%s.py' %(n,nBatches)
	print('parallelProcess = (%s,%s)' %(n,nBatches))

	# Read in the file
	with open(theTemplate, 'r') as file:
	  filedata = file.read()

	# Replace the target string
	filedata = filedata.replace('parallelProcess = (1,1)', 'parallelProcess = (%s,%s)' %(n,nBatches))

	# Write the file out again
	with open(batchFolder + curFile, 'w') as file:
	  file.write(filedata)