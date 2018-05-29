# 21-11-2017 Rens Meerhoff
# Function to choose between loading an existing CSVexcerpt, or creating a new one.

import pdb; #pdb.set_trace()
from warnings import warn
import numpy as np
from os.path import isfile, join, isdir
from os import listdir
import CSVexcerpt


if __name__ == '__main__':
	# Identify TimeStamp
	idTS(filename,mypath,firstFrameTimeseries,windowTimeseries)

def idTS(filename,folder,firstFrameTimeseries,windowTimeseries):
	# Omit .csv if necessary
	if filename[-4:] == '.csv':
		filename = filename[:-4]	
	
	# By default, make a new excerpt
	createExcerpt = 'True'
	# Unless an existing excerpt is found:
	mypath = folder + 'CSVexcerpts\\' # path where CSVexcerpts are stored

	if isdir(mypath):
		onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
		for idx, val in enumerate(onlyfiles):
			idme = [] # identity of string with value 'm' which is the end of the window
			idms = [] # start of the window based on identify of string 'm'
			idms.append(len(filename)+1)
			if filename == val[0:len(filename)]: # if filenames are the same
				# look for the letter 'm' in the bit after the common filename
				# DEBUGGING INFO: Note that code to find times in filename are slightly different here compared to identifyTimestamp.py (the other is better)
				for i, f in enumerate(val[len(filename)+1:]):
					if f == 'm':
						idme.append(len(filename) + 1 + i )
				idms.append(idme[0] + 3)
				tmin = val[idms[0]:idme[0]]
				tmax = val[idms[1]:idme[1]]	
			if firstFrameTimeseries >= int(tmin) and (firstFrameTimeseries + windowTimeseries) <= int(tmax):
				createExcerpt = 'False'
				filename = val
				break			
	else:
	    print('WARNING: Couldnt find excerpts; create excerpts to improve speed')

	if createExcerpt == 'True':
		print('Creating a new CSV...')
		filename = CSVexcerpt.execFunction(filename,folder,firstFrameTimeseries,windowTimeseries)
		# Determine new tmin and tmax
		tmp_tmin = 1
		tmp_tmax = 2
		maxFound = 'False'
		for i in range(len(filename)-1):
			if maxFound == 'False':
				if filename[-i-2] + filename[-i-1] == 'ms':
					if tmp_tmax == 2:			
						idme_tmax = len(filename)-i-3		
					tmp_tmax = tmp_tmax + 1
				if tmp_tmax > 2:
					if filename[-i-3] == '_':
						idms_tmax = len(filename)-i-2
						maxFound = 'True'

			if filename[-i-2] + filename[-i-1] == 'ms':
				if tmp_tmin == 2:			
					idme_tmin = len(filename)-i-3		
				tmp_tmin = tmp_tmin + 1
			if tmp_tmin > 2:
				if filename[-i-3] == '_':
					idms_tmin = len(filename)-i-2
					break
		tmin = int(filename[idms_tmin:idme_tmin+1])
		tmax = int(filename[idms_tmax:idme_tmax+1])

	return tmin,tmax,filename