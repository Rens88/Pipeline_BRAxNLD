import pandas as pd
from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs, sep, walk
import numpy as np
import pdb; #pdb.set_trace()


outputFolders = ['C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 05s\\']

# outputFolders = ['C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 05s\\',\
# 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 10s\\',\
# 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 15s\\',\
# 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 25s\\']


logText = []

for outputFolder in outputFolders:
	logText.append('************************')

	parallelOutputFiles = [f for f in listdir(outputFolder) if isfile(join(outputFolder, f)) if '.csv' in f]
	totalMatches = 0
	totalEvents = 0
	totalNlonger = 0
	totalNinside = 0
	totalNinsideLonger = 0
	for f in parallelOutputFiles:
		df = pd.read_csv(outputFolder+f,low_memory=False) # NB: low_memory MUST be True, otherwise it results in problems later on.
		
		substrings = f.split('_')
		for s in substrings:
			if 'of' in s:
				curBatchString = s

		# Count the number of matches per window to keep track of progress
		nMatches = len(df['MatchID'].unique())
		tmp = 'For batch = <%s>:' %s
		print(tmp)
		logText.append(tmp)

		tmp = '<%s> matches recorded' %nMatches
		print(tmp)
		logText.append(tmp)

		tmp = '<%s> events analyzed' %df.shape[0]
		print(tmp)
		logText.append(tmp)

		nInside = sum(df['eventClassification'] == 'inside_16m') #/ df.shape[0] * 100
		tmp = 'of which <%s> were inside 16m' %nInside
		logText.append(tmp)

		##
		threshold = 5		
		
		dfLonger = df[df['dtPrevEvent'] > threshold]
		nLonger = dfLonger.shape[0]
		tmp = '<%s> events analyzed that were longer than <%ss>.' %(nLonger,threshold)
		print(tmp)
		logText.append(tmp)

		nInsideLonger = sum(dfLonger['eventClassification'] == 'inside_16m') # / nLonger * 100
		tmp = 'of which <%s> were inside 16m' %nInsideLonger
		logText.append(tmp)

		lastMatch = df.loc[max(df.index),'MatchID']
		tmp = '<%s> was the last MatchID' %lastMatch
		print(tmp)
		logText.append(tmp)

		allMatches = df['MatchID'].unique()
		tmp = '\nAll MatchIDs:\n%s\n' %allMatches
		logText.append(tmp)
		logText.append('************************')
		
		totalMatches = totalMatches + nMatches
		totalEvents = totalEvents + df.shape[0]
		totalNlonger = totalNlonger + nLonger
		totalNinsideLonger = totalNinsideLonger + nInsideLonger
		totalNinside = totalNinside + nInside

	totalNinside_pct = np.round(totalNinside / totalEvents * 100)
	totalNinsideLonger_pct = np.round(totalNinsideLonger / totalNlonger * 100)
	totalNlonger_pct = np.round(totalNlonger / totalEvents * 100)
	logText.append('\nIn total, <%s> events were recorded (<%s%%> inside 16m), of which:\n<%s> (<%s%%>) were longer than <%ss> (<%s%%> inside 16m).' %(totalEvents, totalNinside_pct,totalNlonger,totalNlonger_pct,threshold,totalNinsideLonger_pct))
	logText.append('In total, <%s> matches were analyzed in <%s> batches.' %(totalMatches,len(parallelOutputFiles)))

	logFname = outputFolder + 'log_combinineParallelOutput.txt'
	text_file = open(logFname, "w")
	for l in logText:
		text_file.write(l + '\n')
	text_file.close()
