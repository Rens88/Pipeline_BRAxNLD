from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs
import sys, inspect


if __name__ == '__main__':
	addLibrary()
	checkFolders(folder,aggregateEvent)

def addLibrary():

	####### Adding the library ######
	# The folder and relevant subfolders where you store the python library with all the custom modules.
	current_folder = path.realpath(path.abspath(path.split(inspect.getfile( inspect.currentframe() ))[0]))
	library_folder = current_folder + str("\\LibraryRENS")
	library_subfolders = [library_folder + str("\\FDP")] # FDP = Football Data Project
	library_subfolders.append(library_folder + str("\\VPcontributions")) # Contributions of a Bachelor student
	library_subfolders.append(library_folder + str("\\LTcontributions")) # Contributions of a Bachelor student

	if library_folder not in sys.path:
		sys.path.insert(0, library_folder) 
	for subf in library_subfolders:
		if subf not in sys.path:
			sys.path.insert(0, subf) # idea: could loop over multiple subfolders and enter as a list

	## Uncomment this line to open the function in the editor (matlab's ctrl + d)
	# subprocess.call(cmd_folder + "\\callThisFunction.py", shell=True)
	#########################################


def checkFolders(folder,aggregateEvent):
	if folder[-1:] != '\\':
		warn('\n<folder> did not end with <\\\\>. \nOriginal input <%s>\nReplaced with <%s>' %(folder,folder+'\\'))
		folder = folder + '\\'

	dataFolder = folder + 'Data\\'
	tmpFigFolder = folder + 'Figs\\Temp\\' + aggregateEvent + '\\'
	outputFolder = folder + 'Output\\' # Folder where tabular output will be stored (aggregated spatially and temporally)
	cleanedFolder = dataFolder + 'Cleaned\\'    

	# Verify if folders exists
	if not exists(dataFolder):
		warn('\nWARNING: dataFolder not found.')
		exit()
	if not exists(outputFolder):
		makedirs(outputFolder)
	if not exists(tmpFigFolder):
		makedirs(tmpFigFolder)
	if not exists(cleanedFolder):
		makedirs(cleanedFolder)

	return dataFolder,tmpFigFolder,outputFolder,cleanedFolder

=======
from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs
import sys, inspect


if __name__ == '__main__':
	addLibrary()
	checkFolders(folder,aggregateEvent)

def addLibrary():

	####### Adding the library ######
	# The folder and relevant subfolders where you store the python library with all the custom modules.
	current_folder = path.realpath(path.abspath(path.split(inspect.getfile( inspect.currentframe() ))[0]))
	library_folder = current_folder + str("\\LibraryRENS")
	library_subfolders = [library_folder + str("\\FDP")] # FDP = Football Data Project
	library_subfolders.append(library_folder + str("\\VPcontributions")) # Contributions of a Bachelor student
	library_subfolders.append(library_folder + str("\\LTcontributions")) # Contributions of a Bachelor student

	if library_folder not in sys.path:
		sys.path.insert(0, library_folder) 
	for subf in library_subfolders:
		if subf not in sys.path:
			sys.path.insert(0, subf) # idea: could loop over multiple subfolders and enter as a list

	## Uncomment this line to open the function in the editor (matlab's ctrl + d)
	# subprocess.call(cmd_folder + "\\callThisFunction.py", shell=True)
	#########################################


def checkFolders(folder,aggregateEvent):
	if folder[-1:] != '\\':
		warn('\n<folder> did not end with <\\\\>. \nOriginal input <%s>\nReplaced with <%s>' %(folder,folder+'\\'))
		folder = folder + '\\'

	dataFolder = folder + 'Data\\'
	tmpFigFolder = folder + 'Figs\\Temp\\' + aggregateEvent + '\\'
	outputFolder = folder + 'Output\\' # Folder where tabular output will be stored (aggregated spatially and temporally)
	cleanedFolder = dataFolder + 'Cleaned\\'    

	# Verify if folders exists
	if not exists(dataFolder):
		warn('\nWARNING: dataFolder not found.')
		exit()
	if not exists(outputFolder):
		makedirs(outputFolder)
	if not exists(tmpFigFolder):
		makedirs(tmpFigFolder)
	if not exists(cleanedFolder):
		makedirs(cleanedFolder)

	return dataFolder,tmpFigFolder,outputFolder,cleanedFolder

>>>>>>> d6c15d944b1168972454264b6f2fc40ddffffa10
