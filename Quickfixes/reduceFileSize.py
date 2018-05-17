# 06-03-2018 Rens Meerhoff
# When working with large files, it can be easy to quickly reduce the filesize.
# NB: Check the lines after 'EDIT THIS'

import pandas as pd

# The folder that contains the file (should end with \\)
## EDIT THIS ##################################################################
# dataFolder = 'C:\\Users\\rensm\\Documents\\PostdocLeiden\\BRAxNLD repository\\Data\\'

originalDataFolder = 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Data\\'
newDataFolder = 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\Sample repository\\Data\\'

originalEventsFolder = 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Data\\existingTargets\\'
newEventsFolder = 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\Sample repository\\Data\\existingTargets\\'

# The filename
## EDIT THIS ##################################################################
fnames = ['1_EL_XIV.csv','2_EL_XIV.csv']
fnames = ['134_NLCUP_XVI.csv']
fnames = ['100_ERE_XV.csv']
for fname in fnames:
	eventsFname = fname[:-4] + '_Event.csv'

	# The strings of the column headers that you want to import.
	## EDIT THIS ##################################################################
	# colHeaders = ['Ts','X','Y','Snelheid','Acceleration','PlayerID','TeamID']

	df = pd.read_csv(originalDataFolder+fname,low_memory=False)
	dfEvent = pd.read_csv(originalEventsFolder+eventsFname,low_memory=False)

	# Make sure this is referencing the key that represents the timestamp. 
	# Make sure the unit of measurement is appropriate.
	## EDIT THIS ##################################################################
	theFirst_n_seconds = 100000 # in ms						

	statement = (df['Timestamp'] < theFirst_n_seconds)# & df['Ts'] < 50)
	statementEvent = (dfEvent['End_Ts (ms)'] < theFirst_n_seconds)

	# The new cropped dataframe
	df_cropped = df[statement]
	dfEvent_cropped = dfEvent[statementEvent]

	# Export the CSV
	df_cropped.to_csv(newDataFolder + 'CROPPED_' + fname)
	dfEvent_cropped.to_csv(newEventsFolder + 'CROPPED_' + eventsFname)