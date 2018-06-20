import pandas as pd
from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs, sep, walk
import numpy as np
import pdb; #pdb.set_trace()


outputFolder = 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Results\\'

combinedOutputFiles = [f for f in listdir(outputFolder) if isfile(join(outputFolder, f)) if '.csv' in f]

combined_df = pd.DataFrame([])
eventString = []
logText = []
for f in combinedOutputFiles:
	if not 'NOMISSING' in f:
		continue
	if 'combinedNOMISSING_sample' in f:
		continue
	if 'allWindows' in f:
		continue
	substrings = f.split('_')
	curEventString = []
	for s in substrings:
		if 'Turnovers' in s:
			curEventString = s
	if curEventString == []:
		print('Didnt recognize eventString. Add string to code.')
		curEventString = 'Unknown'

	if not eventString == []:
		if not eventString == curEventString:
			print('WARNING: Windowstrings are not the same for every file that is being combined.')
	eventString = curEventString

	df = pd.read_csv(outputFolder+f,low_memory=False) # NB: low_memory MUST be True, otherwise it results in problems later on.
	if len(df['EventUID'].unique()) != df.shape[0]:
		print('WARNING: Not as many eventUIDs as rows... could result in concatenation error')
	
	# df.sort_values('EventUID', axis = 0, inplace = True)
	# df.reset_index(drop=True,inplace = True)
	# df.index.names = ["OverallIndex"]

	# df.reindex(df['EventUID'])
	df.set_index(['EventUID'],inplace = True)

	# print(df['EventUID'])
	
	
	# pdb.set_trace()
	df.index.names = ["EventUID_Index"]


	# check if identifiers are unique
	firstColumns = []
	# secondColumns = []
	for k in df.keys():
		# if k == 'OverallIndex':
		# 	df = df.drop([k],axis = 1)
		# 	firstColumns.append(k)
		# 	continue
		if k in combined_df.keys():
			print('Duplicate (= fileIdentifier) key <%s> was dropped.' %k)
			# # determine longest
			# maxIdx = min([df.shape[0], combined_df.shape[0]]) -1
			# # print(df.loc[0:maxIdx-1,k])
			# # print(combined_df.loc[0:maxIdx-1,k])
			# # pdb.set_trace()
			# if any(df.loc[0:maxIdx,k] != combined_df.loc[0:maxIdx,k]):
			# 	print('Problem.. mis-aligned events?')
			# 	print(df.loc[0])
			# 	print('***')
			# 	print(combined_df.loc[0])
			# 	# pdb.set_trace()


			
			# drop identifiers (but not for the first one)
			df = df.drop([k],axis = 1)
			firstColumns.append(k)
		# else: # assuming that the fileidentifiers are put first
		# 	# secondColumns.append(k)
		# 	break


	combined_df = pd.concat([combined_df,df],axis = 1,join = 'outer')
	# print(combined_df.keys())

	# if combined_df.index.name == 'OverallIndex':
	# 	combined_df = pd.concat([combined_df,df],axis = 0,ignore_index = True)
	# else:
	# 	combined_df = pd.concat([combined_df,df],axis = 0)
	
	# print(df.index.name)
	# print(combined_df.index.name)

cols = list(combined_df.columns.values)
columnOrder = firstColumns
for ck in combined_df.keys():
	if 'eventOverlap' in ck:
		columnOrder.append(ck)

cidx = columnOrder.index("eventClassification")
columnOrder += [columnOrder.pop(cidx)]

for c in cols:
	if not c in firstColumns:
		columnOrder.append(c)
combined_df = combined_df[columnOrder]

# OMIT overlap at window certain window (5s)
# NB: this automatically limits the number of matches to the number available for this window
combined_df_no_Overlap5s = combined_df[combined_df["eventOverlap_window(5)"] == False]
combined_df_no_Overlap30s = combined_df[combined_df["eventOverlap_window(30)"] == False]


# all of them combined
combined_df_sample = combined_df.loc[combined_df['temporalAggregate'] == 'Turnovers_000']

# only matchest that were computed for all windows
combined_df_noMissing = combined_df.dropna(axis = 0)
combined_df_no_Overlap5s_noMissing = combined_df_no_Overlap5s.dropna(axis = 0)
combined_df_no_Overlap30s_noMissing = combined_df_no_Overlap30s.dropna(axis = 0)
combined_df_noMissing_sample =combined_df_noMissing.loc[combined_df_noMissing['temporalAggregate'] == 'Turnovers_000']

tmp = '\nEvents omitted (listwise) due to some missing values:\ndf length before: %s' %combined_df.shape[0]
logText.append(tmp)
print(tmp)

tmp = 'df length after: %s' %combined_df_noMissing.shape[0]
logText.append(tmp)
print(tmp)

tmp = '\nEvents omitted (listwise) due to overlap at 5s (only for %s_allWindowsCombined_NOMISSING_noOverlap5s.csv>):\ndf length before: %s' %(eventString,combined_df.shape[0])
logText.append(tmp)
print(tmp)

tmp = 'df length after: %s' %combined_df_no_Overlap5s.shape[0]
logText.append(tmp)
print(tmp)

tmp = 'Events omitted (listwise) due to some missing values for events with no overlap at 5s:\ndf length before: %s' %combined_df_no_Overlap5s.shape[0]
logText.append(tmp)
print(tmp)

tmp = 'df length after: %s' %combined_df_no_Overlap5s_noMissing.shape[0]
logText.append(tmp)
print(tmp)

tmp = '\nEvents omitted (listwise) due to overlap at 30s (only for %s_allWindowsCombined_NOMISSING_noOverlap30s.csv>):\ndf length before: %s' %(eventString,combined_df.shape[0])
logText.append(tmp)
print(tmp)

tmp = 'df length after: %s' %combined_df_no_Overlap30s.shape[0]
logText.append(tmp)
print(tmp)

tmp = 'Events omitted (listwise) due to some missing values for events with no overlap at 30s:\ndf length before: %s' %combined_df_no_Overlap30s.shape[0]
logText.append(tmp)
print(tmp)

tmp = 'df length after: %s' %combined_df_no_Overlap30s_noMissing.shape[0]
logText.append(tmp)
print(tmp)

# missing matches
missingMatches = [c for c in combined_df['MatchID'].unique() if c not in combined_df_noMissing['MatchID'].unique()]
nTotal = len(combined_df['MatchID'].unique())
nMissing = len(missingMatches)

tmp = '\nTo be precise: <%s> out of <%s> matches were missing.' %(nMissing,nTotal)
logText.append(tmp)
print(tmp)

tmp = 'The missing MatchIDs were:\n%s' %missingMatches
logText.append(tmp)
print(tmp)

# for c in combined_df['MatchID'].unique():
# 	if c not in combined_df_noMissing['MatchID'].unique():




combined_df.to_csv(outputFolder + eventString + '_allWindowsCombined.csv')

combined_df_sample.to_csv(outputFolder + eventString + '_allWindowsCombined_sample.csv')

combined_df_noMissing.to_csv(outputFolder + eventString + '_allWindowsCombined_NOMISSING.csv')

combined_df_noMissing_sample.to_csv(outputFolder + eventString + '_allWindowsCombined_NOMISSING_sample.csv')

# smaple not necessary as 0th event never has overlap
combined_df_no_Overlap5s.to_csv(outputFolder + eventString + '_allWindowsCombined_noOverlap5s.csv')
combined_df_no_Overlap5s_noMissing.to_csv(outputFolder + eventString + '_allWindowsCombined_NOMISSING_noOverlap5s.csv')
combined_df_no_Overlap30s.to_csv(outputFolder + eventString + '_allWindowsCombined_noOverlap30s.csv')
combined_df_no_Overlap30s_noMissing.to_csv(outputFolder + eventString + '_allWindowsCombined_NOMISSING_noOverlap30s.csv')

logFname = outputFolder + 'log_combinineWindowsOutput.txt'
text_file = open(logFname, "w")
for l in logText:
	text_file.write(l + '\n')
text_file.close()