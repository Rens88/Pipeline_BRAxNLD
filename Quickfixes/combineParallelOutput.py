import pandas as pd
from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs, sep, walk
import numpy as np

outputFolder = 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\16-05-2018\\'
outputFolder = 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\NP repository\\Output\\17-05-2018\\'

parallelOutputFiles = [f for f in listdir(outputFolder) if isfile(join(outputFolder, f)) if '10.csv' in f]

# Check whether the right ones were selected
print(parallelOutputFiles)
combined_df = pd.DataFrame([])
for f in parallelOutputFiles:
	# print(f)
	df = pd.read_csv(outputFolder+f,low_memory=False) # NB: low_memory MUST be True, otherwise it results in problems later on.
	# print(df.keys())
	# pdb.set_trace()

	combined_df = pd.concat([combined_df,df],axis = 0,ignore_index=True)



# # drop columns below threshold
# columnThreshold = 10 # in percentage
# th = np.round(columnThreshold/100*combined_df.shape[0])
# combined_df.dropna(axis = 1,thresh=th,inplace = True)

# print(combined_df.shape[0])

# # drop rows listwise
# combined_df.dropna(axis = 0,inplace = True)

# print(combined_df.shape[0])


# first of all, drop any columns that have no data at all
percentagesMissing = []
columnThreshold = 10
for c in combined_df.keys():

	percMissing = sum(combined_df[c].isnull())/combined_df[c].shape[0]*100
	percentagesMissing.append(percMissing)

	if percMissing > columnThreshold:
		print('<%s> percent were missing; Therefore, dropped column <%s>.' %(percMissing,c))
		combined_df_noMissing = combined_df.drop([c],axis = 1)

print('df shape before: %s' %combined_df.shape[0])
# for i in combined_df.index:
# 	# omit rows listwise
# 	if not any(combined_df.loc[i].notnull()):
# 		combined_df.drop(c,axis = 0,inplace=True)
combined_df_noMissing.dropna(axis = 0,inplace = True)
print('df shape after: %s' %combined_df.shape[0])
	
combined_df.to_csv(outputFolder + parallelOutputFiles[0][:-10] + 'combined.csv')
combined_df_noMissing.to_csv(outputFolder + parallelOutputFiles[0][:-10] + 'combinedNOMISSING.csv')

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



	# Then, drop any rows with missing data

	# print(combined_df.loc[1226].isnull())

	# for idx in combined_df.index:
	# 	if not any(combined_df.loc[idx].isnull()):
	# 		# print()
	# 		print('hi')

