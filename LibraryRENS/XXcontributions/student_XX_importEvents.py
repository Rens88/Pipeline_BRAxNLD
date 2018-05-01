# A student's template.
# To make your module function with the pipeline:
# 0) copy the whole example folder, and replace XX with your initials
# 1) edit the name of the student's function in cleanupData.py (around line 137)
# 2) edit the student function that's imported at the top of importEvents.py
# 	(where it now says "import student_XX_importEvents")
# 3) change the string content of <studentFolder> in the 'process_Template' (NB: you can make a copy of process_Template and call it process_StudentXX)

import numpy as np
import math
import pandas as pd
from warnings import warn
import pdb; #pdb.set_trace()
from os import listdir, path, makedirs, sep
from os.path import isfile, join, isdir, exists


## Here, you can clarify which functions exist in this module.
if __name__ == '__main__': 

	process(targetEvents,cleanFname,TeamAstring,TeamBstring,dataFolder)
	importFromXML(targetEvents,cleanFname,TeamAstring,TeamBstring,dataFolder)

## Here, you specifiy what each function does
def process(targetEvents,cleanFname,TeamAstring,TeamBstring,dataFolder):

	targetEvents = importFromXML(targetEvents,cleanFname,TeamAstring,TeamBstring,dataFolder)

	# Always return the targetEvents as a dictionary (referencing to the type of event you're importing).
	# The dict contains a tuple that has: (1,2,3,i,..)
	# 1 --> Time of event (i.e., the end of the event)
	# 2 --> Reference Team
	# 3 --> varies per event, but usually it's the start of the event
	# 4 --> can be anything, for example, the label corresponding to the attack (e.g., 1 = no shot, 2 = shot off target, 3 = shot on target, 4 = goals)
	return targetEvents


def importFromXML(targetEvents,cleanFname,TeamAstring,TeamBstring,dataFolder):

	if not exists(dataFolder + 'existingTargets'):
		warn('\nWARNING: folder <existingTargets> in dataFolder <%s> NOT found.\nIf you want to import target events, put them in that folder.' %dataFolder)
		return targetEvents

	existingTargetsFname = dataFolder + 'existingTargets' + sep + cleanFname + '_existingTargets.xml'
	if not isfile(existingTargetsFname):
		warn('\nWARNING: Although the existingTarget\'s folder existed in dataFolder <%s>, no <%s> found.\nNo existing targets imported.\n' %(dataFolder,cleanFname + '_existingTargets.xml'))
		return existingTargets

	rawTargetEvents = pd.read_csv(existingTargetsFname)


	## HERE YOU PROCESS THE rawTargetEvents
	#
	#	NB --> this module assumes that you have a folder in the dataFolder that 
	#	contains all the .xml files that have the same filename as the corresponding 
	#	cleanFname, but with the addition '_existingTargets.xml'. For example:
	#	dataFolder = 'dataFolder'
	#	cleanFname = 'this_is_the_first_clean_match.csv'
	#	Means that:
	# 	existingTargetsFname = 'dataFolder\existingTargets\this_is_the_first_clean_match_existingTargets.xml'
	#
	# This is a made up example:
	tEnd = targetEvents['Full'][0] 				# Instant the event occurred (or ended)
	refTeam = TeamAstring						# The team to which the event relates (if note applicable: None)
	tStart = targetEvents['Full'][0] - (targetEvents['Full'][2] - targetEvents['Full'][0]) # Instant at which the event started (if applicable)
	eventLabel = 3 # (e.g., 1 = no shot, 2 = shot off target, 3 = shot on target, 4 = goals)
	made_up_target = (tEnd,refTeam,tStart,eventLabel)
	## \ HERE YOU PROCESS THE rawTargetEvents


	targetEvents = {**targetEvents,'exampleTarget':[made_up_target]}

	return targetEvents
