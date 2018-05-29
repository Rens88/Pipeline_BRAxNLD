import pandas as pd
from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs, sep, walk
import numpy as np
import pdb; #pdb.set_trace()


outputFolders = ['C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\27-05-2018\\']

# outputFolders = ['C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 05s\\',\
# 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 10s\\',\
# 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 15s\\',\
# 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 25s\\']


logText = []

for outputFolder in outputFolders:
	logText.append('\n\n************************')

	parallelOutputFiles = [f for f in listdir(outputFolder) if isfile(join(outputFolder, f)) if '.csv' in f]

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

		lastMatch = df.loc[max(df.index),'MatchID']
		tmp = '<%s> was the last MatchID' %lastMatch
		print(tmp)
		logText.append(tmp)

		allMatches = df['MatchID'].unique()
		tmp = '\nAll MatchIDs:\n%s\n' %allMatches
		logText.append(tmp)
		logText.append('\n\n************************')
	
	logFname = outputFolder + 'log_combinineParallelOutput.txt'
	text_file = open(logFname, "w")
	for l in logText:
		text_file.write(l + '\n')
	text_file.close()
