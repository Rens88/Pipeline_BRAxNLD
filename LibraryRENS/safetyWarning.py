# 30-11-2017 Rens Meerhoff
# Function of a collection of warnings useful for FDP.
#
# checkWindow => warns when the desired window does not match the window of the data

import pdb; #pdb.set_trace()
from warnings import warn
import numpy as np

if __name__ == '__main__':		
	checkWindow(rawDict,firstFrameTimeseries,windowTimeseries)
	checkLengthExport(expData,expString,expExplanation)

########################################################################	

def checkLengthExport(expData,expString,expExplanation):
	if len(expData) != len(expString):
		warn('\nExported data does not have the same length as the exported header strings:\nCheck whether you didnt forget to add any.\n')
	if len(expData) != len(expExplanation):
		warn('\nExported data does not have the same length as the exported variable explanation:\nCheck whether you didnt forget to add any.\n')
########################################################################	

def checkWindow(rawDict,firstFrameTimeseries,windowTimeseries):
	smallestTime = np.amin(rawDict['Time']['TsMS'])
	biggestTime = np.amax(rawDict['Time']['TsMS'])
	# Check if window (user input) lies within data range
	if firstFrameTimeseries < smallestTime:
		if firstFrameTimeseries + windowTimeseries < smallestTime:
			warn('\nFirst frame AND last frame smaller than smallest occurring frame: \nConsider choosing a different time-window.\n')
		else:
			warn('\nFirst frame smaller than smallest occurring frame: \nFirst frame was replaced by smallest occurring frame\n')
			firstFrameTimeseries = smallestTime
	if firstFrameTimeseries + windowTimeseries > biggestTime:
		if firstFrameTimeseries > biggestTime:
			warn('\nLast frame AND first frame bigger than biggest occurring frame: \nConsider choosing a different time-window.\n')
		else:
			warn('\nLast frame bigger than biggest occurring frame: \nLast frame was replaced by biggest occurring frame.\n')
			windowTimeseries = biggestTime - firstFrameTimeseries