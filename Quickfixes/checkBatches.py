# If you want to edit something in the code and you're not sure where it is, 
# just ask. l.a.meerhoff@liacs.leidenuniv.nl

#########################
# USER INPUT ############
#########################
## CHANGE THIS all these variables until 'END USER INPUT'
# Here, you provide the string name of the student folder that you want to include.
studentFolder = 'XXcontributions' 

# Temporary inputs (whilst updating to using pandas)
debuggingMode = True # whether yo want to print the times that each script took

# dataType is used for dataset specific parts of the analysis (in the preparation phase only)
dataType =  "FDP" # "FDP" or or "NP" --> so far, only used to call the right cleanup script. Long term goal would be to have a generic cleanup script

# This folder should contain a folder with 'Data'. The tabular output and figure will be stored in this folder as well.
folder = 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\'
# folder = '/local/rens/Repositories/BRAxNLD repository_newStyle/'

# String representing the different teams
# NB: not necessary for FDP (and other datasets where teamstring can be read from the filename, should be done in discetFilename.py)
TeamAstring = 'Provide the string that represents one team' 
TeamBstring = 'Provide the string that represents the other team'

# Input of raw data, indicate at least timestamp, entity and Location info
timestampString = 'Timestamp' 						#'enter the string in the header of the column that represents TIMESTAMP' 	# 'Video time (s)'
PlayerIDstring = 'Player Name' 							#'enter the string in the header of the column that represents PLAYERID' 	# 'jersey n.'
TeamIDstring = 'Team' 								#'enter the string in the header of the column that represents TEAMID' 			# Optional
XPositionString = 'X' 								#'enter the string in the header of the column that represents X-POSITION'			# 'x'
YPositionString = 'Y' 								#'enter the string in the header of the column that represents Y-POSITION'			# 'y'

# Case-sensitive string rawHeaders of attribute columns that already exist in the data (optional). NB: String also sensitive for extra spaces.
readAttributeCols = ['Speed','Acceleration'] # NB: should correspond directly with readAttributeLabels
readAttributeLabels = ['Speed (m/s)','Acceleration (m/s^2)','Distance to closest home (m)','Distance to cloesest visitor (m)'] # NB: should correspond directly with readAttributeCols

# When event columns exist in the raw data, they can be read to export an event file
readEventColumns = []

# If the raw data is not given in meters, provide the conversion.
conversionToMeter = 1 #111111 # https://gis.stackexchange.com/questions/8650/measuring-accuracy-of-latitude-and-longitude/8674#8674

# Here you can determine which event is aggregated before. 
# 'Full' and 'Random' always work. 
# 'Regular' works as long as you don't choose a window larger than your file.
# Other keywords depend on which events you import and/or compute.
aggregateEvent = 'Turnovers' # Event that will be used to aggregate over (verified for 'Goals' and for 'Possession')
allWindows_and_Lags = [(30,0),(25,0),(20,0),(15,0),(10,0),(5,0)] # input tuple of window and corresponding lag. # a negative lag indicates tEnd as after the event

aggregatePerPlayer = [] # a list of outcome variables that you want to aggregated per player. For example: ['vNorm','distFrame']

# Strings need to correspond to outcome variables (dict keys). 
# Individual level variables ('vNorm') should be included as a list element.
# Group level variables ('LengthA','LengthB') should be included as a tuple (and will be plotted in the same plot).
# plotTheseAttributes = ['vNorm',('Surface_ref','Surface_oth')]#,('Spread_ref','Spread_oth'),('stdSpread_ref','stdSpread_oth'),'vNorm']#,'LengthB',('LengthA','LengthB'),('SurfaceA','SurfaceB'),('SpreadA','SpreadB'),('WidthA','WidthB')] # [('LengthA','LengthB'),('WidthA','WidthB'),('SurfaceA','SurfaceB'),('SpreadA','SpreadB')] # teams that need to be compared as tuple
# This trialVisualization plots the selected outcome variables variable for the given window for the temporal aggregation. Useful to verify if your variables are as excpected.
includeTrialVisualization = False
plotTheseAttributes_atTrialLevel = ['vNorm'] #
# This datasetVisualization compares all events of all files in the dataset. Useful for datasetlevel comparisons
includeDatasetVisualization = False
plotTheseAttributes_atDatasetLevel = ['vNorm',('Surface_ref','Surface_oth'),('Spread_ref','Spread_oth')]

# Parts of the pipeline can be skipped
skipCleanup = False # Only works if cleaned file exists. NB: if False, all other skips become ineffective.
skipSpatAgg = False # Only works if spat agg export exists. NB: if False, skipEventAgg and skipToDataSetLevel become ineffective.
skipComputeEvents = False #

# If both True, then files are not verified to be analyzed previously
# If skipToDataSetLevel == False, then it is verified that every match exists in eventAggregate
skipEventAgg = False # For now, don't skip unless longest window was computed. # Only works if current file already exists in eventAgg. NB: if False, skipToDataSetLevel becomes ineffective.
skipToDataSetLevel = False # Only works if corresponding AUTOMATIC BACKUP exists. NB: Does not check if all raw data files are in automatic backup. NB2: does not include any changes in cleanup, spatagg, or eventagg NB2: may not work after implementing iteratoverwindows thingy

# Choose between append (= True) or overwrite (= False) (the first time around only of course) the existing (if any) eventAggregate CSV.
# NB: This could risk in adding duplicate data. There is no warning for that at the moment (could use code from cleanupData that checks if current file already exist in eventAggregate)
appendEventAggregate = False

# Restrict the analysis to files of which event data exists
onlyAnalyzeFilesWithEventData = True

#########################
# END USER INPUT ########
#########################

## Advanced user input (i.e., it might not work if you change it):
# Currently, it's possible to turn off interpolation (saves time), but it may cause other parts of the pipeline to malfunction
includeEventInterpolation = False # may cause problems at the plotting level, 
includeCleanupInterpolation = True # When not interpolating at all, plotting procedure becomes less reliable as it uses an un-aligned index (and it may even fail)
datasetFramerate = 10 # (Hz) This is the framerate with which the whole dataset will be aggregated.

parallelProcess = (1,1) # (nth process,total n processes) # default = (1,1)

#########################
# INITIALIZATION ########
#########################

# pre-initialization
from shutil import copyfile	
import pdb; #pdb.set_trace()
from os.path import isfile, join, exists, realpath, abspath, split,dirname, isdir#, isdir, exists
from os import listdir, stat, sep, rename#, path, makedirs
import inspect
from warnings import warn
import numpy as np

cdir = realpath(abspath(split(inspect.getfile( inspect.currentframe() ))[0]))
pdir = dirname(cdir) # parent directory
library_folder = cdir + str(sep + "LibraryRENS")

if not isdir(library_folder):
	# if the process.py is in a subfolder, then copy the initialization module
	copyfile(pdir + sep + 'initialization.py', cdir + sep + 'initialization.py')

import initialization
# In this module, the library is added to the system path. 
# This allows Python to import the custom modules in our library. 
# If you add new subfolders in the library, they need to be added in addLibary (in initialization.py) as well.

nBatches = 15
logText = []
totalNfiles = 0
for i in np.arange(1,16):
	tmp = '\n\n************************'
	# print(tmp)
	logText.append(tmp)

	parallelProcess = (i,nBatches)

	# In this module, the library is added to the system path. 
	# This allows Python to import the custom modules in our library. 
	# If you add new subfolders in the library, they need to be added in addLibary (in initialization.py) as well.
	dataFolder,tmpFigFolder,outputFolder,cleanedFolder,spatAggFolder,eventAggFolder,aggregatedOutputFilename,outputDescriptionFilename,eventAggFname,backupEventAggFname,DirtyDataFiles,aggregateLevel,t,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization,rawHeaders, attrLabel,skipComputeEvents,DirtyDataFiles_backup = \
	initialization.process(studentFolder,folder,aggregateEvent,allWindows_and_Lags,skipToDataSetLevel,skipCleanup,skipSpatAgg,skipEventAgg,includeTrialVisualization,timestampString,PlayerIDstring,TeamIDstring,XPositionString,YPositionString,readAttributeCols,readAttributeLabels,onlyAnalyzeFilesWithEventData,parallelProcess,skipComputeEvents)

	tmp = '\nparallelProcess = %s of %s\n' %parallelProcess
	# print(tmp)
	
	logText.append(tmp)

	[logText.append(t) for t in DirtyDataFiles]

	curNfiles = len(DirtyDataFiles)
	logText.append('\ncurNfiles = <%s>' %curNfiles)

	totalNfiles = curNfiles + totalNfiles

logText.append('\ntotalNfiles = <%s>' %totalNfiles)

logFname = 'log_checkBatches.txt'
text_file = open(logFname, "w")
for l in logText:
	text_file.write(l + '\n')
	print(l)
text_file.close()
