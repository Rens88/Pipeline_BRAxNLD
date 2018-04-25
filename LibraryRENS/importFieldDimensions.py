# 18-04-2018 Rens Meerhoff
# Useable to import specific field dimensions

import pdb; #pdb.set_trace()
from warnings import warn
import numpy as np
import pandas as pd
from os.path import isfile, join, isdir, exists
from os import sep

import pandas as pd

# from os.path import isfile, join, isdir
# from os import listdir
# import CSVexcerpt
# import exportCSV

import safetyWarning

if __name__ == '__main__':
	process(dataFolder,dirtyFname)

def process(dataFolder,dirtyFname,exportData,exportDataString):
	fieldDimensions = {'foundIt':False}
	warn('\nUPDATE REQUIRED.\nShould embed a rotation of the rawData based on the field dimensions if necessary\n************\n************\n************\n')

	if not exists(dataFolder + 'FieldDimensions'):
		fieldDimensions.update({'X_bot_left': -50,'X_top_left': -50,'X_bot_right': 50,'X_top_right': 50,'Y_bot_left': -32.5,'Y_bot_right': -32.5,'Y_top_left': 32.7,'Y_top_right': 32.7}) # Values are given in meters
		warn('\nWARNING: no field dimensions found. Taking the default dimensions:\n%s\n\n--------------------\nIf you want to specify the fieldDimensions, create a CSV file in the folder %sFieldDimensions with the name <fieldDimensions.csv> that looks like <fieldDimensions.csv>.\nThis file should contain a column <Filename> with the string that corresponds to dirtyFname <%s>.\nFurthermore, the file should contain the following columns with the respective coordinates:\n<X_top_left>\n<Y_top_left>\n<X_top_right>\n<Y_top_right>\n<X_bot_left>\n<Y_bot_left>\n<X_bot_right>\n<Y_bot_right>.\n' %(fieldDimensions,dataFolder,dirtyFname))
		return fieldDimensions

	fieldDimFname = dataFolder + 'FieldDimensions' + sep + 'fieldDimensions.csv'
	if not isfile(fieldDimFname):
		fieldDimensions.update({'X_bot_left': -50,'X_top_left': -50,'X_bot_right': 50,'X_top_right': 50,'Y_bot_left': -32.5,'Y_bot_right': -32.5,'Y_top_left': 32.7,'Y_top_right': 32.7}) # Values are given in meters
		warn('\nWARNING: Although the fieldDimensions folder existed, no field dimensions found. Taking the default dimensions:\n%s\n\n--------------------\nIf you want to specify the fieldDimensions, create a CSV file in the folder %sFieldDimensions with the name <fieldDimensions.csv> that looks like <fieldDimensions.csv>.\nThis file should contain a column <Filename> with the string that corresponds to dirtyFname <%s>.\nFurthermore, the file should contain the following columns with the respective coordinates:\n<X_top_left>\n<Y_top_left>\n<X_top_right>\n<Y_top_right>\n<X_bot_left>\n<Y_bot_left>\n<X_bot_right>\n<Y_bot_right>.\n' %(fieldDimensions,dataFolder,dirtyFname))
		return fieldDimensions

	fieldDims = pd.read_csv(fieldDimFname)
	foundIt = False
	for idx,existingDimsFname in enumerate(fieldDims['Filename']):
		if existingDimsFname == dirtyFname:
			fieldDimensions['foundIt'] = True
			fieldDimensions.update({'X_bot_left': fieldDims['X_bot_left'][idx],'X_top_left': fieldDims['X_top_left'][idx],'X_bot_right': fieldDims['X_bot_right'][idx],'X_top_right': fieldDims['X_top_right'][idx],'Y_bot_left': fieldDims['Y_bot_left'][idx],'Y_bot_right': fieldDims['Y_bot_right'][idx],'Y_top_left': fieldDims['Y_top_left'][idx],'Y_top_right': fieldDims['Y_top_right'][idx]}) # Values are given in meters			
			return fieldDimensions

	if 'School' in exportDataString and 'Test' in exportDataString:
		curSchool = exportData[exportDataString.index('School')]
		sameSchoolRows = fieldDims['School'] == curSchool
		
		curTest = exportData[exportDataString.index('Test')]
		sameTest_and_sameSchoolRows = fieldDims.loc[sameSchoolRows]['Test'] == curTest

		if any(sameTest_and_sameSchoolRows):
			idx = sameTest_and_sameSchoolRows.index[0]
			fieldDimensions['foundIt'] = True
			fieldDimensions.update({'X_bot_left': fieldDims['X_bot_left'][idx],'X_top_left': fieldDims['X_top_left'][idx],'X_bot_right': fieldDims['X_bot_right'][idx],'X_top_right': fieldDims['X_top_right'][idx],'Y_bot_left': fieldDims['Y_bot_left'][idx],'Y_bot_right': fieldDims['Y_bot_right'][idx],'Y_top_left': fieldDims['Y_top_left'][idx],'Y_top_right': fieldDims['Y_top_right'][idx]}) # Values are given in meters			

			return fieldDimensions
		# print(sameSchoolRows)
		
		# print(exportData)
	# pdb.set_trace()
	# if not foundIt:

	# print(fieldDimensions)
	# pdb.set_trace()
	# fieldDimensions = {'X_bot_left': -50,'X_top_left': -50,'X_bot_right': 50,'X_top_right': 50,'Y_bot_left': -32.5,'Y_bot_right': -32.5,'Y_top_left': 32.7,'Y_top_right': 32.7} # Values are given in meters
	# warn('\nWARNING: no field dimensions found. Taking the default dimensions:\n%s\n\n--------------------\nIf you want to specify the fieldDimensions, create a CSV file in the folder %sFieldDimensions with the name <fieldDimensions.csv> that looks like <fieldDimensions.csv>.\nThis file should contain a column <Filename> with the string that corresponds to dirtyFname <%s>.\nFurthermore, the file should contain the following columns with the respective coordinates:\n<X_top_left>\n<Y_top_left>\n<X_top_right>\n<Y_top_right>\n<X_bot_left>\n<Y_bot_left>\n<X_bot_right>\n<Y_bot_right>.\n' %(fieldDimensions,dataFolder,dirtyFname))

	# # print(dirtyFname)
	# # Make it dependent on the school??
	# fieldDimensions = {'x1': 145467.5435,'x2': 145492.8546,'y1': 11546224.98,'y2': 11546267.51} # Values are given in meters
	# # print(fname)
	# pdb.set_trace()


	# # FDP
	# fieldDimensions = {'x1': -50,'x2': 50,'y1': -32.5,'y2': 32.7} # Values are given in meters

	
	return fieldDimensions
