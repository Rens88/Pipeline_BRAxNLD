# 13/03/2018 Rens Meerhoff
# Function that initializes the pipeline.

from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs, sep
import sys, inspect
import pdb; #pdb.set_trace()
from warnings import warn
import time

if __name__ == '__main__':
	addLibrary()
	checkFolders(folder,aggregateEvent)

def addLibrary(studentFolder):

	####### Adding the library ######
	# The folder and relevant subfolders where you store the python library with all the custom modules.
	current_folder = path.realpath(path.abspath(path.split(inspect.getfile( inspect.currentframe() ))[0]))
	library_folder = current_folder + str(sep + "LibraryRENS")
	library_subfolders = [library_folder + str(sep + "FDP")] # FDP = Football Data Project
	library_subfolders.append(library_folder + str(sep + "" + studentFolder)) # Contributions of a Bachelor student
	# library_subfolders.append(library_folder + str(sep + "LTcontributions")) # Contributions of a Bachelor student

	if library_folder not in sys.path:
		sys.path.insert(0, library_folder) 
	for subf in library_subfolders:
		if subf not in sys.path:
			sys.path.insert(0, subf) # idea: could loop over multiple subfolders and enter as a list

	## Uncomment this line to open the function in the editor (matlab's ctrl + d)
	# subprocess.call(cmd_folder + "\\callThisFunction.py", shell=True)
	#########################################


def checkFolders(folder,aggregateEvent):
	if folder[-1:] != sep:
		warn('\n<folder> did not end with <%s>. \nOriginal input <%s>\nReplaced with <%s>' %(sep,folder,folder+sep))
		folder = folder + sep

	dataFolder = folder + 'Data' + sep
	tmpFigFolder = folder + 'Figs' + sep + 'Temp' + sep + aggregateEvent + sep
	outputFolder = folder + 'Output' + sep# Folder where tabular output will be stored (aggregated spatially and temporally)
	cleanedFolder = dataFolder + 'Cleaned' + sep    

	# Verify if folders exists
	if not exists(dataFolder):
		warn('\nWARNING: dataFolder not found.\nMake sure that you put your data in the folder <%s>\n' %dataFolder)
		exit()
	if not exists(outputFolder):
		makedirs(outputFolder)
	if not exists(tmpFigFolder):
		makedirs(tmpFigFolder)
	if not exists(cleanedFolder):
		makedirs(cleanedFolder)

	timeString = time.strftime("%Hh%Mm_%d_%B_%Y")
	outputFilename = outputFolder + 'output_' + aggregateEvent + '_' + timeString +  '.csv'
	outputDescriptionFilename = outputFolder + 'output_Description_' + aggregateEvent + '_' + timeString +  '.txt'
	return dataFolder,tmpFigFolder,outputFolder,cleanedFolder,outputFilename,outputDescriptionFilename

# >>>>>>> d6c15d944b1168972454264b6f2fc40ddffffa10
