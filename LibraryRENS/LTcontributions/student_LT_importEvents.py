# A student's template.
# To make your module function with the pipeline:
# 0) copy the whole example folder, and replace XX with your initials
# 1) edit the name of the student's function in cleanupData.py (around line 137)
# 2) edit the student function that's imported at the top of importEvents.py
# 	(where it now says "import student_XX_importEvents")
# 3) change the string content of <studentFolder> in the 'process_Template' (NB: you can make a copy of process_Template and call it process_StudentXX)

import numpy as np
import math
import re
import pandas as pd
from warnings import warn
import xml.etree.ElementTree as ET
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
	def XMLToDF(root,iterate,childCols,teamName):
		dfList2 = []

		for child in root.iter(iterate):
			dfList = []
			if teamName != '':
				dfList.append(teamName)
			for key in child.keys():
				# print(key,child.attrib.get(key))
				if key in childCols and child.attrib.get(key) != '':
					idxChildCols = childCols.index(key)
					# print(key,child.attrib.get(key),childCols.index(key))
					dfList.insert(idxChildCols,child.attrib.get(key))

			if len(dfList) == len(childCols):
				dfList2.append(dfList)

		return dfList2 #pd.DataFrame(dfList2,columns=childCols)

     
	if not exists(dataFolder + 'existingTargets'):
		warn('\nWARNING: folder <existingTargets> in dataFolder <%s> NOT found.\nIf you want to import target events, put them in that folder.' %dataFolder)
		return targetEvents

	existingTargetsFname = dataFolder + 'existingTargets' + sep + cleanFname[:-12] + '_Events.xml'
	if not isfile(existingTargetsFname):
		warn('\nWARNING: Although the existingTarget\'s folder existed in dataFolder <%s>, no <%s> found.\nNo existing targets imported.\n' %(dataFolder,cleanFname + '_existingTargets.xml'))
		return targetEvents

	#Read XML file
	tree = ET.parse(existingTargetsFname)
	root = tree.getroot()
	ballEventCols = ['Time', 'BallEventCode', 'NumAmiscoJ1']#'Id', 'WithWhat', 'How', 'NumAmiscoJ1']
	matchEventCols = ['Time', 'MatchEventCode', 'NumAmiscoJ1']
	# playerCols = ['Team','Id','NumAmisco','SecondName', 'FirstName', 'ShirtNumber', 'Poste']#old firstname not always filled
	playerCols = ['Team','NumAmisco']

	# for child in root.iter('EVENT'):
	# 	print(child.attrib)
		# for key in child.keys():
		# 		if key == 'MatchEventCode' and child.attrib.get(key) != '' :
		# 			# goalsEvents.append(child.attrib)
		# 			print(child.attrib,child.attrib.get(key))

	# pdb.set_trace()

	#read Events
	dfBallEvents = pd.DataFrame(XMLToDF(root, 'EVENT', ballEventCols, ''), columns=ballEventCols)
	dfBallEvents.rename(columns={'BallEventCode': 'Event'}, inplace=True)
	dfMatchEvents = pd.DataFrame(XMLToDF(root, 'EVENT', matchEventCols, ''), columns=matchEventCols)
	dfMatchEvents.rename(columns={'MatchEventCode': 'Event'}, inplace=True)

	# print(dfBallEvents)
	# print(dfMatchEvents)
	# pdb.set_trace()

	# dfEvents = pd.concat([dfBallEvents, dfMatchEvents])
	# print(dfEvents['Time'].count())
	# print(dfEvents[dfEvents.index.duplicated()])
	dfEvents = dfBallEvents.append(dfMatchEvents, ignore_index=True)
	# print(dfBallEvents['Time'].count())
	# print(dfEvents['Time'].count())
	# print(dfEvents[dfEvents.index.duplicated()])

	# dfBallEvents.to_csv('D:\\KNVB\\ballevents.csv')
	# dfMatchEvents.to_csv('D:\\KNVB\\matchevents.csv')
	# dfEvents.to_csv('D:\\KNVB\\events.csv')
	# pdb.set_trace()

	#read Team and Players
	dfList = []
	for team in root.iter('tTEAM'):
		for teamKey in team.keys():
			if teamKey == 'Name':
				teamName = team.attrib.get(teamKey)
				dfList = dfList + XMLToDF(team, 'PLAYER', playerCols, teamName)
				# print(dfList)

	# print(dfList)
	# pdb.set_trace()

	dfPlayers = pd.DataFrame(dfList,columns=playerCols)
	# dfPlayers.to_csv('D:\\KNVB\\players.csv')

	# print(dfPlayers)
	# print(dfEvents)
	# pdb.set_trace()

	dfEvents = timestampToSeconds(dfEvents,'Time')

	dfEvents.rename(columns={'NumAmiscoJ1': 'NumAmisco'}, inplace=True)

	# print(dfEvents[dfEvents['Id'] == '1469'])

	dfMerged = pd.merge(dfEvents, dfPlayers, on='NumAmisco')
	# print(dfMerged[dfMerged['Time'].isnull()])
	# pdb.set_trace()
	# dfMerged = dfMerged[pd.notnull(dfMerged['Time'])]
	# print(dfMerged['Time'].count())

	# print(dfMerged)

	# pdb.set_trace()

	#Ball events
	passEvents = []
	receptionEvents = []
	runningEvents = []
	neutralEvents = []
	crossEvents = []
	highCatchGkEvents = []
	clearanceEvents = []
	shotOnTargetEvents = []
	holdOfBallGkEvents = []
	shotNotOnTargetEvents = []
	footClearGkEvents = []
	highDeflGkEvents = []
	lowCatchGkEvents = []
	lowDeflGkEvents = []
	#Match events
	dirFreeKickEvents = []
	goalEvents = []
	offsideEvents = []
	yellowCardEvents = []
	redCardEvents = []
	playerInEvents = []
	playerOutEvents = []

	for idx,i in enumerate(pd.unique(dfMerged['Time'])):
		curTime = i
		curTeam = dfMerged.loc[dfMerged['Time'] == curTime,'Team'].values[0]
		curEvent = dfMerged.loc[dfMerged['Time'] == curTime,'Event'].values[0]
		#BALL EVENTS
		if curEvent == 'Pass':
			passEvents.append((curTime,curTeam)) 
		elif curEvent == 'Reception':
			receptionEvents.append((curTime,curTeam))
		elif curEvent == 'Running with ball':
			runningEvents.append((curTime,curTeam))
		elif curEvent == 'Neutral contact':
			neutralEvents.append((curTime,curTeam))
		elif curEvent == 'Cross':
			crossEvents.append((curTime,curTeam))
		elif curEvent == 'Clearance':
			clearanceEvents.append((curTime,curTeam))
		elif curEvent == 'Shot on target':
			shotOnTargetEvents.append((curTime,curTeam))
		elif curEvent == 'Shot not on target':
			shotNotOnTargetEvents.append((curTime,curTeam))
		elif curEvent == 'Hold of ball gk':
			holdOfBallGkEvents.append((curTime,curTeam))
		elif curEvent == 'Foot clearance gk':
			footClearGkEvents.append((curTime,curTeam))
		elif curEvent == 'High deflection gk':
			highDeflGkEvents.append((curTime,curTeam))
		elif curEvent == 'Low deflection gk':
			lowDeflGkEvents.append((curTime,curTeam))
		elif curEvent == 'High catch gk':
			highCatchGkEvents.append((curTime,curTeam))
		elif curEvent == 'Low catch gk':
			lowCatchGkEvents.append((curTime,curTeam))
		#MATCH EVENTS
		elif curEvent == 'Foul - dir. free-kick':
			dirFreeKickEvents.append((curTime,curTeam))
		elif curEvent == 'Goal':
			goalEvents.append((curTime,curTeam))
		elif curEvent == 'Offside':
			offsideEvents.append((curTime,curTeam))
		elif curEvent == 'Yellow card':
			yellowCardEvents.append((curTime,curTeam))
		elif curEvent == 'Red card':
			redCardEvents.append((curTime,curTeam))
		elif curEvent == 'Player in':
			playerInEvents.append((curTime,curTeam))
		elif curEvent == 'Player out':
			playerOutEvents.append((curTime,curTeam))


	# print(pd.unique(dfEvents['BallEventCode']))
	# print(dfEvents[dfEvents['BallEventCode'] == 'Pass'])
	# print('#############################',receptionEvents,'#############################',runningEvents,'#############################',neutralEvents,'#############################',crossEvents,'#############################',highCatchGkEvents,'#############################',clearanceEvents,'#############################',shotOnTargetEvents,'#############################',holdOfBallGkEvents,'#############################',shotNotOnTargetEvents,'#############################',footClearGkEvents,'#############################',highDeflGkEvents,'#############################',lowCatchGkEvents,'#############################',lowDeflGkEvents)

	targetEvents = {**targetEvents,'pass':passEvents,'reception':receptionEvents,'runningWithBall':runningEvents,'neutralContact':neutralEvents,'cross':crossEvents,'clearance':clearanceEvents,'shotOnTarget':shotOnTargetEvents,'shotNotOnTarget':shotNotOnTargetEvents,'holdOfBallGk':holdOfBallGkEvents,'footClearanceGk':footClearGkEvents,'highDeflectionGk':highDeflGkEvents,'lowDelfectionGk':lowDeflGkEvents,'highCatchGk':highCatchGkEvents,'lowCatchGk':lowCatchGkEvents,'directFreeKick':dirFreeKickEvents,'goal':goalEvents,'offside':offsideEvents,'yellowCard':yellowCardEvents,'redCard':redCardEvents,'playerIn':playerInEvents,'playerOut':playerOutEvents}

	# print(targetEvents['shotOnTarget'])
	# pdb.set_trace()

	#LT: added! Anders pakt hij het example mee.
	return targetEvents

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

def timestampToSeconds(df,ts):
	newdf = pd.DataFrame(data = [],index = df.index, columns = [ts],dtype = 'int32')
	for idx,tmpTs in enumerate(df[ts]):
		newdf[ts][idx] = (int(tmpTs) / 10)

	df[ts] = newdf[ts]

	return df