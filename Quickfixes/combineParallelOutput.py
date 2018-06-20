import pandas as pd
from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs, sep, walk
import numpy as np
import pdb; #pdb.set_trace()


outputFolders = ['C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 05s\\',\
'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 10s\\',\
'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 15s\\',\
'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 20s\\',\
'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 25s\\',\
'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 30s\\']

# outputFolders = ['C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 05s\\',\
# 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 10s\\',\
# 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 15s\\',\
# 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Turnovers 25s\\']

outputFolder_export = 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Results\\'
logText = []

for outputFolder in outputFolders:
	logText.append('\n\n************************')

	parallelOutputFiles = [f for f in listdir(outputFolder) if isfile(join(outputFolder, f)) if '.csv' in f]

	combined_df = pd.DataFrame([])
	windowString = []
	for f in parallelOutputFiles:
		df = pd.read_csv(outputFolder+f,low_memory=False) # NB: low_memory MUST be True, otherwise it results in problems later on.
		
		substrings = f.split('_')
		for s in substrings:
			if 'window' in s:
				curWindowString = s

		if not windowString == []:
			if not windowString == curWindowString:
				print('WARNING: Windowstrings are not the same for every file that is being combined.')
		windowString = curWindowString

		combined_df = pd.concat([combined_df,df],axis = 0,ignore_index=True)

		# Count the number of matches per window to keep track of progress
		nMatches = len(df['MatchID'].unique())
		tmp = 'For file = <%s>:' %f
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
	
	# first of all, drop any columns that have no data at all
	percentagesMissing = []
	columnThreshold = 10
	# rename index rows:
	combined_df.index.names = ["OverallIndex"]
	combined_df.rename(index=str, columns={"Unnamed: 0": "MatchIndex"},inplace = True)
	for c_orig in combined_df.keys():

		if not c_orig in ['OverallIndex','MatchIndex','EventUID','Competition','MatchID','OthTeam','RefTeam','Season','eventClassification','temporalAggregate']:
			c_new = c_orig + '_' + windowString
			combined_df.rename(index=str, columns={c_orig: c_new},inplace = True)

	combined_df_noMissing = combined_df.copy()
	for c in combined_df_noMissing.keys():

		percMissing = sum(combined_df[c].isnull())/combined_df[c].shape[0]*100
		percentagesMissing.append(percMissing)

		if percMissing > columnThreshold:
			# print('<%s> percent were missing; Therefore, dropped column <%s>.' %(percMissing,c))
			combined_df_noMissing = combined_df_noMissing.drop([c],axis = 1)


	tmp = '\nFeatures omitted due to mostly empty features:\ndf nColumns before: %s' %combined_df.shape[1]
	logText.append(tmp)
	print(tmp)
	tmp = 'df nColumns after: %s\n' %combined_df_noMissing.shape[1]
	logText.append(tmp)
	print(tmp)

	# drop rows listwise
	tmp = '\nEvents omitted (listwise) due to some missing values:\ndf length before: %s' %combined_df.shape[0]
	print(tmp)
	logText.append(tmp)

	combined_df_noMissing.dropna(axis = 0,inplace = True)
	
	tmp = 'df length after: %s' %combined_df_noMissing.shape[0]
	print(tmp)
	logText.append(tmp)

	combined_df.to_csv(outputFolder_export + parallelOutputFiles[0][:-10] + 'combined.csv')
	combined_df_noMissing.to_csv(outputFolder_export + parallelOutputFiles[0][:-10] + 'combinedNOMISSING.csv')

	combined_df_noMissing_sample = combined_df_noMissing.loc[combined_df_noMissing['temporalAggregate'] == 'Turnovers_000']
	combined_df_noMissing_sample.to_csv(outputFolder_export + parallelOutputFiles[0][:-10] + 'combinedNOMISSING_sample.csv')



	# Count the number of matches per window to keep track of progress
	nMatches = len(combined_df['MatchID'].unique())
	tmp = '<%s> matches for windowString = <%s>' %(nMatches,windowString)
	print(tmp)
	logText.append(tmp)

	tmp = 'Finished <%s>' %parallelOutputFiles[0][:-10]
	print(tmp)
	logText.append(tmp)

	if 'Exp' in combined_df.keys():
		# NP only
		NP_df = combined_df[combined_df['Exp'] == 'NP']
		NP_df.to_csv(outputFolder + parallelOutputFiles[0][:-10] + 'combined_NP.csv')

		# LP only
		LP_df = combined_df[combined_df['Exp'] == 'LP']
		LP_df.to_csv(outputFolder + parallelOutputFiles[0][:-10] + 'combined_LP.csv')

		NP_df_pre = NP_df[NP_df['Test'] == 'PRE']
		NP_df_pos = NP_df[NP_df['Test'] == 'POS']
		NP_df_ret = NP_df[NP_df['Test'] == 'RET']
		NP_df_tra = NP_df[NP_df['Test'] == 'TRA']
		NP_df_pre.to_csv(outputFolder + parallelOutputFiles[0][:-10] + 'combined_NP_pre.csv')
		NP_df_pos.to_csv(outputFolder + parallelOutputFiles[0][:-10] + 'combined_NP_pos.csv')
		NP_df_ret.to_csv(outputFolder + parallelOutputFiles[0][:-10] + 'combined_NP_ret.csv')
		NP_df_tra.to_csv(outputFolder + parallelOutputFiles[0][:-10] + 'combined_NP_tra.csv')

		LP_df_pre = LP_df[LP_df['Test'] == 'PRE']
		LP_df_pos = LP_df[LP_df['Test'] == 'POS']
		LP_df_ret = LP_df[LP_df['Test'] == 'RET']
		LP_df_tra = LP_df[LP_df['Test'] == 'TRA']
		LP_df_pre.to_csv(outputFolder + parallelOutputFiles[0][:-10] + 'combined_LP_pre.csv')
		LP_df_pos.to_csv(outputFolder + parallelOutputFiles[0][:-10] + 'combined_LP_pos.csv')
		LP_df_ret.to_csv(outputFolder + parallelOutputFiles[0][:-10] + 'combined_LP_ret.csv')
		LP_df_tra.to_csv(outputFolder + parallelOutputFiles[0][:-10] + 'combined_LP_tra.csv')



logFname = outputFolder_export + 'log_combinineParallelOutput.txt'
text_file = open(logFname, "w")
for l in logText:
	text_file.write(l + '\n')
text_file.close()
