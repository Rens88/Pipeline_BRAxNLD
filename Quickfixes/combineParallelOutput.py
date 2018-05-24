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

outputFolder_export = 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Results\\'

for outputFolder in outputFolders:

	parallelOutputFiles = [f for f in listdir(outputFolder) if isfile(join(outputFolder, f)) if '.csv' in f]


	combined_df = pd.DataFrame([])
	for f in parallelOutputFiles:
		df = pd.read_csv(outputFolder+f,low_memory=False) # NB: low_memory MUST be True, otherwise it results in problems later on.
		
		combined_df = pd.concat([combined_df,df],axis = 0,ignore_index=True)

	# first of all, drop any columns that have no data at all
	percentagesMissing = []
	columnThreshold = 10
	combined_df_noMissing = combined_df.copy()
	for c in combined_df_noMissing.keys():

		percMissing = sum(combined_df[c].isnull())/combined_df[c].shape[0]*100
		percentagesMissing.append(percMissing)

		if percMissing > columnThreshold:
			# print('<%s> percent were missing; Therefore, dropped column <%s>.' %(percMissing,c))
			combined_df_noMissing = combined_df_noMissing.drop([c],axis = 1)

	print('\ndf nColumns before: %s' %combined_df.shape[1])
	print('df nColumns after: %s\n' %combined_df_noMissing.shape[1])

	print('df length before: %s' %combined_df.shape[0])
	combined_df_noMissing.dropna(axis = 0,inplace = True)
	print('df length after: %s' %combined_df_noMissing.shape[0])
	
	
	combined_df.to_csv(outputFolder_export + parallelOutputFiles[0][:-10] + 'combined.csv')
	combined_df_noMissing.to_csv(outputFolder_export + parallelOutputFiles[0][:-10] + 'combinedNOMISSING.csv')

	print('Finished <%s>' %parallelOutputFiles[0][:-10])

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
