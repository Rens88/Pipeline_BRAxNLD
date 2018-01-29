# 06-12-2017 Rens Meerhoff
# Script to export a CSV. Check whether the file already exists and then adds it.

import pdb; #pdb.set_trace()
import csv
from warnings import warn
import numpy as np
from os.path import isfile, join, isdir
from os import listdir, startfile
import CSVexcerpt


if __name__ == '__main__':
	
	# fname can include the folder as well
	newOrAdd(fname,header,data)	
	varDescription(fname,exportDataString,exportFullExplanation)
	
	# Filename should probably be 'temp.csv'
	# varToPrint is the variable you want to print
	# winopen: if True: open in windows
	debugPrint(filename,varToPrint,winopen)
#########################################################################

def debugPrint(filename,varToPrint,winopen):

	with open(filename,'w',newline='') as myfile:
		wr = csv.writer(myfile)
		for i in varToPrint:
			# if len(i) == 1:
			# 	print('I did this')
			# 	print([i][0:5])
			# 	wr.writerow([i])
			# else:
			# 	print('I did the other this')
			# 	print(len(i))
			# 	print(i[0:5])				
			wr.writerow(i)


	if winopen == True:
		startfile(filename)

def varDescription(fname,exportDataString,exportFullExplanation):
	with open(fname,'w') as myfile:
		for idx,val in enumerate(exportDataString):
			curLine = '%s:\t\t\t %s\n' %(val,exportFullExplanation[idx])
			if len(val) >= 15:
				curLine = '%s:\t\t %s\n' %(val,exportFullExplanation[idx])	
			if len(val) >= 23:
				curLine = '%s:\t %s\n' %(val,exportFullExplanation[idx])				
			myfile.write(curLine)

#########################################################################

def newOrAdd(fname,header,data):
	if not isfile(fname):
		# write new

		with open(fname, 'w',newline='') as myfile:
			wr = csv.writer(myfile)
			wr.writerow(header)
			wr.writerow(data)

	else:
		# append
		with open(fname, 'r') as myfile:
			reader = csv.reader(myfile)
			existingHeaders = list(next(reader))
		if len(existingHeaders) != len(header):
			warn('\nWARNING: original file did not have the same number of columns as the newly exported data.\nConsider creating a new file or at least edit the existingHeaders.')	    

		with open(fname, 'a',newline='') as myfile:
			wr = csv.writer(myfile)
			wr.writerow(data)
