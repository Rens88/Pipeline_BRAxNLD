# 08-02-2018 Rens Meerhoff
# Script can be generically used to import information from eventsPanda. If the data is not organized similarly, it will simply export empty targetEvents. In the future, we can include LMP-based import of these kinds of data (or perhaps standardize the format earlier in the pipeline).


# 05-02-2018 Rens Meerhoff
# Based on countExistingEvents.py:
# Script now only exports timestamp (in seconds) and identifier of existing events.
#
# Previously:
# 06-12-2017 Rens Meerhoff
# Script to obtain game/trial-based results of existing events (goals, possession changes and passes)
# NP specific, as there is no event / attribute data for FDP

import pdb; #pdb.set_trace()
from warnings import warn
import numpy as np
import pandas as pd
# from os.path import isfile, join, isdir
# from os import listdir
# import CSVexcerpt
# import exportCSV

import safetyWarning
import student_LT_importEvents
import student_VP_importEvents
import time

from os import listdir, path, makedirs, sep
from os.path import isfile, join, isdir, exists

if __name__ == '__main__':
	process(eventsPanda,TeamAstring,TeamBstring)

	# 1 --> Time of event
	# 2 --> Reference Team
	# 3 --> varies per event:

	goals(eventsPanda,TeamAstring,TeamBstring,targetEvents)
	# no 3

	possession(eventsPanda,TeamAstring,TeamBstring,targetEvents)
	# 3 --> START time of the possession

	passes(eventsPanda,TeamAstring,TeamBstring,possessionCharacteristics,targetEvents)
	# 3 --> nth possession this pass was a part of (to determine number of consqutive passes)

	full(targetEvents)
	# 3 --> START time of the possession


def process(rawPanda,eventsPanda,TeamAstring,TeamBstring,cleanFname,dataFolder,debuggingMode,skipComputeEvents_curFile,xmlFolder):
	tImportEvents = time.time()
	targetEvents = {}

	if skipComputeEvents_curFile:
		if debuggingMode:
			elapsed = str(round(time.time() - tImportEvents, 2))
			print('***** Time elapsed during importEvents: %ss' %elapsed)

		return targetEvents
	########################################################################################
	####### Import existing events ######################################################
	########################################################################################

	## goals
	targetEvents = goals(eventsPanda,TeamAstring,TeamBstring,targetEvents)
	## possession / turnovers
	targetEvents = possession(eventsPanda,TeamAstring,TeamBstring,targetEvents)
	## Runs
	targetEvents = runs(eventsPanda,TeamAstring,TeamBstring,targetEvents)
	## Pass
	# NB: If available, possession should be computed before passes to incorporate number of consecutive passes
	# Could implement a safer procedure by starting with an empty dictionary and then adding the eventspecific dictionary after it's done the module (exporting an empty dictionary if info not available), then, the possession dictionary HAS to exist before going through
	targetEvents = passes(eventsPanda,TeamAstring,TeamBstring,targetEvents)
	targetEvents = full(eventsPanda,targetEvents,TeamAstring)

	importFromFile = False # temporarily turned off
	if importFromFile:
		targetEvents = fromFile(targetEvents,rawPanda,TeamAstring,TeamBstring,cleanFname,dataFolder)

	targetEvents = \
	student_LT_importEvents.process(targetEvents,cleanFname,TeamAstring,TeamBstring,dataFolder,xmlFolder)

	if debuggingMode:
		elapsed = str(round(time.time() - tImportEvents, 2))
		print('***** Time elapsed during importEvents: %ss' %elapsed)

	return targetEvents

def fromFile(targetEvents,rawPanda,TeamAstring,TeamBstring,cleanFname,dataFolder):

	# it's not so tidy and perhaps not very generic.
	# This function creates some useful targets.

	if not exists(dataFolder + sep + 'existingTargets'):
		warn('\nWARNING: folder <existingTargets> in dataFolder <%s> NOT found.\nIf you want to import target events, put them in that folder.' %dataFolder)
		return targetEvents

	existingTargetsFname = dataFolder + 'existingTargets' + sep + cleanFname[:-12] + '_Event.xml' #changed to xml

	print(existingTargetsFname)
	pdb.set_trace()

	if not isfile(existingTargetsFname):
		warn('\nWARNING: Although the existingTarget\'s folder existed in dataFolder <%s>, no <%s> found.\nNo existing targets imported.\nSet onlyAnalyzeFilesWithEventData to True in User input if you want to limit the analysis to files that have targetEvents.\n' %(dataFolder,cleanFname[:-12] + '_Event.csv'))
		return targetEvents

	rawTargetEvents = pd.read_csv(existingTargetsFname, low_memory = False)

	## All events that typically occur:
	theseAreTheKnownEvents = ['Near ball', 'Transition A-H', 'BP Home', 'Pass', 'Reception', 'Running with ball', 'Transition H-A', 'BP Away', 'Interception']
	# Near ball --> all players close to the ball
	# Transition --> instant new team has possession
	# Pass --> Pass from 'Player' to 'Target'
	# Reception --> Not sure. More or less the inverse of Pass. One of 'Player' and 'Target' received and the other gave the pass
	# Interception --> By 'Player' intercepted from 'Target'
	importTheseEvents = ['BP Home', 'Pass', 'BP Away', 'Interception']

	## Check if columns exist in csv
	requiredColumns = ['End_Ts (ms)','Start_Ts (ms)','X','Player','Target','Event']
	for c in requiredColumns:
		if not c in rawTargetEvents.keys():
			warn('\nFATAL WARNING: Required column <%s> not found in rawTargetEvents:\n<%s>.\nThis will result in an error in the for-loop below.\nIf this occurs, you need to make this section more generic.\n' %(c,rawTargetEvents.keys()))
	#####

	TeamA_Players = rawPanda.loc[rawPanda['TeamID'] == TeamAstring,'PlayerID'].unique()
	TeamB_Players = rawPanda.loc[rawPanda['TeamID'] == TeamBstring,'PlayerID'].unique()

	template = False ########################################################################################################################################################################################################################################################################################################################################################################################################
	if template:
		tmp = pd.read_csv('C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Data\\Cleaned\\1_EL_XIV_cleaned.csv')

		TeamA_Players = tmp.loc[tmp['TeamID'] == TeamAstring,'PlayerID'].unique()
		TeamB_Players = tmp.loc[tmp['TeamID'] == TeamBstring,'PlayerID'].unique()

	# Check if no overlap between players
	DuplPlayers = [i for i in TeamA_Players if i in TeamB_Players]
	if DuplPlayers != []:
		warn('\nFATAL WARNING: The following players exist both in Team A <%s> and Team B <%s>:\n%s\n\nThis is fatally problematic, as some of the assumptions won\'t hold.' %(TeamAstring,TeamBstring,DuplPlayers))

	HomeTeam = []
	AwayTeam = []

	# Passes = pd.DataFrame([],columns = ['tEvent','refTeam','nthPossession','Passer','Receiver'])
	Passes = []
	# Posessions = pd.DataFrame([],columns = ['tEvent','refTeam','tStart'])
	Possessions = []
	# Turnovers = pd.DataFrame([],columns = ['tEvent','refTeam','nthPossession','Intercepter','Interceptee'])
	Turnovers = []

	couldnotfindthese = []
	skippedTheseInconsistencies = []
	for idx in rawTargetEvents.index:

		if not rawTargetEvents.loc[idx,'Event'] in importTheseEvents:
			if not rawTargetEvents.loc[idx,'Event'] in theseAreTheKnownEvents:
				warn('\nWARNING: Unknown event <%s> encountered in rawTargetEvents.\nScript not set-up to process this event.\nEdit <importTheseEvents> in <importEvents.py>.\n' %rawTargetEvents.loc[idx,'Event'])
			continue

		curTeam = []

		tEnd = rawTargetEvents.loc[idx,'End_Ts (ms)'] /1000 # conver to seconds
		tStart = rawTargetEvents.loc[idx,'Start_Ts (ms)'] /1000 # conver to seconds
		X = rawTargetEvents.loc[idx,'X']
		Y = rawTargetEvents.loc[idx,'Y']
		refPlayer = rawTargetEvents.loc[idx,'Player']
		targetPlayer = rawTargetEvents.loc[idx,'Target']
		eventString = rawTargetEvents.loc[idx,'Event']

		# Volgens mij worden de events bepaald aan de hand van het veranderen van snelheid en
		# richting van de bal in de buurt van een speler. Reception betekent dat een pass
		# ontvangen is door een teamgenoot, interception door een tegenstander. BP is de
		# start van een bal possession en transition geeft het einde van een possession weer.
		# Een possession duurt van het eerste balcontact van team A tot het eerste balcontact
		# van team B.  Running with the ball zou overeen moeten komen met een bal en speler
		# die in dezelfde richting bewegen in elkaars nabijheid.

		## Check some assumptions (within current event only)

		# Check who is the current team
		if refPlayer in TeamA_Players:
			curTeam = TeamAstring

		if refPlayer in TeamB_Players:
			if not curTeam == []:
				warn('\nFATAL WARNING: A player <%s> was linked to both Team A <%s> and Team B <%s>.\nThis will lead to problems later on.\n' %(refPlayer,TeamAstring,TeamBstring))
			curTeam = TeamBstring

		# When curTeam is failed to be allocated
		if curTeam == []:
			# if home and away team not yet allocated:
			# Report a warning and continue with the next. (should only be possible if ths happens in the first events)
			if HomeTeam == []:
				warn('\nWARNING: A player that could not be allocated to a team was found before Home and Away teams were assigned.\nTherefore, event <%s> was skipped.\n' %rawTargetEvents.loc[idx,'Event'])
				continue

			# if home and away team already allocated, just use the previous allocation
			# determine whether current event refers to home or away
			if eventString in ['Transition A-H', 'BP Home']:
				curTeam = HomeTeam
			else:
				curTeam = AwayTeam

			addCurPlayer_to_TeamX_players = False # temporarily turned off, but it does work
			if addCurPlayer_to_TeamX_players:
				# add curplayer to TeamX_players
				if curTeam == TeamAstring:
					TeamA_Players = np.append(TeamA_Players,refPlayer)

				if curTeam == TeamBstring:
					TeamB_Players = np.append(TeamB_Players,refPlayer)

			# Export the players that couldnt be found: (printed in a warning after the loop)
			if not refPlayer in couldnotfindthese:
				couldnotfindthese.append(refPlayer)
		# else:
		# 	print('Found <%s>' %refPlayer)

		# Check who is the home and away team
		if eventString in ['Transition A-H', 'BP Home']:
			if HomeTeam == []:
				HomeTeam = curTeam
				if HomeTeam == TeamAstring:
					AwayTeam = TeamBstring
				else:
					AwayTeam = TeamAstring

			elif not HomeTeam == curTeam:
				# warn('\nFATAL WARNING: Inconsistent labelling of Home and Away.\nCurrent \'Home\'-Event referred to refPlayer <%s> of team <%s>.\nBut, previously, an event was used to deduce that team <%s> was the home team...\nThis means that the event refTeam cannot be trusted.\nTo be sure, this event was skipped.\n' %(refPlayer,curTeam,HomeTeam))
				skippedTheseInconsistencies.append('Event <%s> of refPlayer <%s> and targetPlayer <%s> was skipped.' %(eventString,refPlayer,targetPlayer))
				continue

		if eventString in ['Transition H-A', 'BP Away']:
			if AwayTeam == []:
				AwayTeam = curTeam
				if AwayTeam == TeamAstring:
					HomeTeam = TeamBstring
				else:
					HomeTeam = TeamAstring

			elif not AwayTeam == curTeam:
				# warn('\nFATAL WARNING: Inconsistent labelling of Home and Away.\nCurrent \'Away\'-Event referred to refPlayer <%s> of team <%s>.\nBut, previously, an event was used to deduce that team <%s> was the away team...\nThis means that the event refTeam cannot be trusted.\nTo be sure, this event was skipped.\n' %(refPlayer,curTeam,AwayTeam))
				skippedTheseInconsistencies.append('Event <%s> of refPlayer <%s> and targetPlayer <%s> was skipped.' %(eventString,refPlayer,targetPlayer))
				continue

		# Check the consistency of the interception
		if eventString == 'Interception':
			# refPlayer = intercpeter
			if targetPlayer in TeamA_Players and curTeam == TeamAstring:
				# warn('\nFATAL WARNING: Inconsistent input of Interception.\nBoth intercepter <%s> and interceptee <%s> are in %s.\nCannot trust assumption made based on who intercepted from whom without additional coding.\n' %(refPlayer,targetPlayer,TeamAstring))
				skippedTheseInconsistencies.append('Event <%s> of refPlayer <%s> and targetPlayer <%s> was skipped.' %(eventString,refPlayer,targetPlayer))
				continue
			if targetPlayer in TeamB_Players and curTeam == TeamBstring:
				# warn('\nFATAL WARNING: Inconsistent input of Interception.\nBoth intercepter <%s> and interceptee <%s> are in %s.\nCannot trust assumption made based on who intercepted from whom without additional coding.\n' %(refPlayer,targetPlayer,TeamBstring))
				skippedTheseInconsistencies.append('Event <%s> of refPlayer <%s> and targetPlayer <%s> was skipped.' %(eventString,refPlayer,targetPlayer))
				continue

		## Export the data
		if 'BP' in eventString:


			Possessions.append([tEnd,curTeam,tStart])
			# curPossessions = pd.DataFrame([tEnd,curTeam,tStart],columns = ['tEvent','refTeam','tStart'])

			# Possessions = Posessions.append(curPossessions,ignore_index=True)

			# if len(Possessions) == 3:
			# 	print(curPossessions)
			# 	print('---')
			# 	print(Possessions)
			# 	pdb.set_trace()

			# For possession, add a duplicate with all possession changes within 16m


			continue

		if 'Pass' == eventString:
			if curTeam == TeamAstring:
				if not targetPlayer in TeamA_Players:
					# targetPlayer in different team, therefore, this pass is a turnover.
					# Skip it.
					continue
			elif curTeam == TeamBstring:
				if not targetPlayer in TeamB_Players:
					# targetPlayer in different team, therefore, this pass is a turnover.
					# Skip it.

					continue
			nthpossession = None
			Passes.append([tStart,curTeam,nthpossession,refPlayer,targetPlayer])
			# curPasses = pd.DataFrame([[tStart,curTeam,nthpossession,refPlayer,targetPlayer]],columns = ['tEvent','refTeam','nthPossession','Passer','Receiver'])
			# Passes = pd.concat([Passes,curPasses], axis =0)

			# Passes.append((tStart,curTeam,nthpossession,refPlayer,targetPlayer))

			continue

		if 'Interception' == eventString:

			nthpossession = None
			Turnovers.append([tStart,curTeam,nthpossession,refPlayer,targetPlayer,(X,Y)])
			# curTurnovers = pd.DataFrame([[tStart,curTeam,nthpossession,refPlayer,targetPlayer]],columns = ['tEvent','refTeam','nthPossession','Intercepter','Interceptee'])
			# Turnovers = pd.concat([Turnovers,curTurnovers], axis =0)

			continue

	if not couldnotfindthese == []:
		warn('\nWARNING: Could not find which team (<%s> or <%s>) the following players belonged to:\n<%s>\n\nAs a temporary work-around I\'ve added the missing players to the home or away team based on which Event they belonged to. THIS MAY LEAD TO FALSE ASSUMPTIONS ETC.\nDefinitely check this one out..\n!!!!!!!!!!!!!' %(TeamAstring,TeamBstring,couldnotfindthese))
	if not skippedTheseInconsistencies == []:
		warn('\nWARNING: Skipped the following events because of inconsistent labelling of Home and Away:\n\n%s' %skippedTheseInconsistencies)


	# Check some between-event assumptions:
	mismatchedPass = []
	matchedPass = []
	mismatchedTurnover = []
	matchedTurnover = []
	for idx,val in enumerate(Possessions):
		tStart = val[2]#Possessions.loc[idx,'tStart']
		tEnd = val[0]#Possessions.loc[idx,'tEvent']
		refTeam = val[1]#Possessions.loc[idx,'refTeam']

		# Add nth possession
		nthCurPass = 0
		lastPass = []
		for idp,p in enumerate(Passes):
			tEvent_p = p[0]
			refTeam_p = p[1]

			if tEvent_p >= tStart and tEvent_p < tEnd:

				nthCurPass = nthCurPass +1

				if Passes[idp][2] == None:
					Passes[idp][2] = idx # allocated nth possession
				else:
					warn('\nFATAL WARNING: Sometings wrong. Pass belongs to multiple possesions.\n')
					pdb.set_trace()

				if refTeam != refTeam_p:
					mismatchedPass.append('Possession refTeam = <%s>, Pass refTeam = <%s>. nth pass = <%s>.' %(refTeam,refTeam_p,nthCurPass))
					# warn('\nWARNING: refTeam based upon possession <%s> does not correspond with refTeam based on Passes.\n' %val)
					# print('Mismatched pass: %s' %p)
					lastPass = 'mismatched'
				else:
					matchedPass.append('Possession refTeam = <%s>, Pass refTeam = <%s>. nth pass = <%s>.' %(refTeam,refTeam_p,nthCurPass))
					lastPass = 'matched'
					# print('Matched pass: %s' %p)

			elif tEvent_p >= tEnd:

				# print(mismatchedPass[-1])

				if lastPass == 'mismatched':
					mismatchedPass[-1] = mismatchedPass[-1] + ' LAST PASS'
				elif lastPass == 'matched':
					matchedPass[-1] = matchedPass[-1] + ' LAST PASS'

				break # assumes that data is ordered by time

		for idt,t in enumerate(Turnovers):
			tEvent_t = t[0]
			refTeam_t = t[1]
			if tEvent_t >= tStart and tEvent_t < tEnd:
				if Turnovers[idt][2] == None:
					Turnovers[idt][2] = idx # allocated nth possession
				else:
					warn('\nFATAL WARNING: Sometings wrong. Turnover belongs to multiple possesions.\n')
					pdb.set_trace()

				if refTeam != refTeam_t:
					# warn('\nWARNING: refTeam based upon possession <%s> does not correspond with refTeam based on Turnovers.\n' %val)
					# print('Mismatched turnover: %s' %t)
					mismatchedTurnover.append('Possession refTeam = <%s>, Turnover refTeam = <%s>.' %(refTeam,refTeam_t))
				else:
					# print('Matched turnover: %s' %t)
					matchedTurnover.append('Possession refTeam = <%s>, Turnover refTeam = <%s>.' %(refTeam,refTeam_t))

	# Check if the teams match that the event references to.
	if mismatchedPass != []:
		mismatchedPassFirst = [m for m in mismatchedPass if 'nth pass = <1>' in m]
		matchedPassFirst = [m for m in matchedPass if 'nth pass = <1>' in m]
		mismatchedPassLast = [m for m in mismatchedPass if 'LAST' in m]
		matchedPassLast = [m for m in matchedPass if 'LAST' in m]
		warn('\n<%s> out of <%s> passes were mismatched in terms of refTeam.' %(len(mismatchedPass),len(mismatchedPass)+len(matchedPass)))
		print('\n<%s> out of <%s> FIRST passes were mismatched.' %(len(mismatchedPassFirst),len(mismatchedPassFirst)+len(matchedPassFirst)))
		print('\n<%s> out of <%s> LAST passes were mismatched.' %(len(mismatchedPassLast),len(mismatchedPassLast)+len(matchedPassLast)))
	if mismatchedTurnover != []:
		warn('\n<%s> out of <%s> turnovers were mismatched in terms of refTeam.' %(len(mismatchedTurnover),len(mismatchedTurnover)+len(matchedTurnover)))


		## Could also check for consistency with to and from etc.
		# for pidx in passIdx:
		# 	# Passer to receiver etc.

	# Change into tuple
	Possessions_tuple = [tuple(x) for x in Possessions]
	Passes_tuple = [tuple(x) for x in Passes]
	Turnovers_tuple = [tuple(x) for x in Turnovers]

	# Add to targetEvents
	# Check if  targetEvents does not have 'Pass' as an event (with data)
	if targetEvents['Possession'] != []:
		warn('\nWARNING: targetEvents[\'Possession\'] was not empty. Possession events imported from CSV stored as Possession_csv.\n')
		targetEvents = targetEvents.update({'Possession_csv':Possessions_tuple})
	else:
		targetEvents['Possession'] = Possessions_tuple

	if targetEvents['Passes'] != []:
		warn('\nWARNING: targetEvents[\'Passes\'] was not empty. Passes events imported from CSV stored as Passes_csv.\n')
		targetEvents = targetEvents.update({'Passes_csv':Passes_tuple})
	else:
		targetEvents['Passes'] = Passes_tuple

	if targetEvents['Turnovers'] != []:
		warn('\nWARNING: targetEvents[\'Turnovers\'] was not empty. Turnovers events imported from CSV stored as Turnovers_csv.\n')
		targetEvents = targetEvents.update({'Turnovers_csv':Turnovers_tuple})
	else:
		targetEvents['Turnovers'] = Turnovers_tuple

	return targetEvents

def full(eventsPanda,targetEvents,TeamAstring):
	targetEvents = {**targetEvents,'Full':[]}
	# If an error occurs here, then this may be a problem with Linux.
	# Replace with: (and an if statement to check if targetEvents isempty)
	# targetEvents = targetEvents.update({'Full':[]})
	TsS = eventsPanda['Ts']
	targetEvents['Full'] = [(float(max(TsS)),None,float(min(TsS)))]
	return targetEvents

def goals(eventsPanda,TeamAstring,TeamBstring,targetEvents):
	targetEvents = {**targetEvents,'Goal':[]}
	# If an error occurs here, then this may be a problem with Linux.
	# Replace with: (and an if statement to check if targetEvents isempty)
	# targetEvents = targetEvents.update({'Goal':[]})
	if not 'Goal' in eventsPanda.keys():
		# No Goal information, so return immediately
		return targetEvents

	count = 0
	goals = [i for i in eventsPanda['Goal'] if i  != '' ]

	goalsOut = np.ones((len(goals),2),dtype='int')*-1

	for idx,i in enumerate(eventsPanda['Goal']):
		if not i == '' and pd.notnull(i):
			goalsOut[count,1] = eventsPanda['Ts'][idx]

			if TeamAstring in i:
				goalsOut[count,0] = 0
				targetEvents['Goal'].append((float(eventsPanda['Ts'][idx]),TeamAstring))
			elif TeamBstring in i:
				goalsOut[count,0] = 1
				targetEvents['Goal'].append((float(eventsPanda['Ts'][idx]),TeamBstring))
			else:
				warn('\n\nCould not recognize team:\n<<%s>>' %i)
			count = count + 1

	return targetEvents

#################################################################
#################################################################

def runs(eventsPanda,TeamAstring,TeamBstring,targetEvents):
	targetEvents = {**targetEvents,'Run':[]}
	# If an error occurs here, then this may be a problem with Linux.
	# Replace with: (and an if statement to check if targetEvents isempty)
	# targetEvents = targetEvents.update({'Run':[]})
	if not 'Run' in eventsPanda.keys():
		# No Run information, so return immediately
		return targetEvents

	runEvent = [(i,val) for i,val in enumerate(eventsPanda['Run']) if val  != '' and pd.notnull(val)]
	dt = []
	in1 = []
	in2 = []
	for idx,i in enumerate(runEvent):
		curFrame = i[0] # frame
		curTime = eventsPanda['Ts'][i[0]]
		curStatus = i[1]
		# Determine per event who has possession from that frame onward
		if curStatus[:3].lower() == 'run':
			in1 = float(eventsPanda['Ts'][curFrame])

		elif curStatus[:7].lower() == 'end run':
			in2 = float(eventsPanda['Ts'][curFrame])

		if in2 != []:
			if in1 == []:
				# Weird, found an end without a start.
				warn('\nWARNING: Found an end without a start. Ignored it. \nConsider checking the raw data.')
				in2 = []
			else:
				# Found a start and an end
				targetEvents['Run'].append((in1,None,in2))
				in1 = []
				in2 = []

	return targetEvents

def possession(eventsPanda,TeamAstring,TeamBstring,targetEvents):
	targetEvents = {**targetEvents,'Possession':[],'Turnovers':[]}

	# If an error occurs here, then this may be a problem with Linux.
	# Replace with: (and an if statement to check if targetEvents isempty)
	# targetEvents = targetEvents.update({'Possession':[],'Turnovers':[]})
	if not 'Possession/Turnover' in eventsPanda.keys():
		# No Possession/Turnover information, so return immediately
		return targetEvents


	# possessionEvents either have starts, ends or turnovers (starts and ends)

	possessionEvents = eventsPanda[eventsPanda['Possession/Turnover'].notnull()]
	# look for all starts
	possessionStarts_idx = [idx for idx in possessionEvents.index if 'start' in possessionEvents.loc[idx,'Possession/Turnover'].lower() or 'turnover' in possessionEvents.loc[idx,'Possession/Turnover'].lower()]

	# look for all ends
	possessionEnds_idx = [idx for idx in possessionEvents.index if 'end' in possessionEvents.loc[idx,'Possession/Turnover'].lower() or 'turnover' in possessionEvents.loc[idx,'Possession/Turnover'].lower()]
	# make sure all events are included
	missingIdx = [i for i in possessionEvents.index if not i in possessionStarts_idx+possessionEnds_idx]
	if missingIdx != []:
		warn('\nWARNING: could not identify possession/turnover event <%s>.\n!!!!!!!!!!!!!!!!! Maybe write an exception.' %possessionEvents.loc[missingIdx])

	# for all starts, look for the corresponding ends. make sure they're later in time and only used once
	for s in possessionStarts_idx:
		tStart = possessionEvents.loc[s,'Ts']

		# look for first end with a larger Ts
		# difference in time
		dt = possessionEvents.loc[possessionEnds_idx,'Ts'] - possessionEvents.loc[s,'Ts']
		# index of smallest positive difference in time
		correspondingEnd_idx = dt[dt == min(dt[dt > 0])].index
		# and now in 'Ts'
		tEnd = possessionEvents.loc[correspondingEnd_idx[0],'Ts']

		# determine refTeam
		# based on start
		curStatus = possessionEvents.loc[s,'Possession/Turnover']

		if 'start' in curStatus.lower(): # 'start'-based start (and not turnover based)
			if TeamAstring.lower() in curStatus.lower():
				# it's about Team A
				startBased_currentPossession = TeamAstring
			elif TeamBstring.lower() in curStatus.lower():
				# it's about Team B
				startBased_currentPossession = TeamBstring
			else:
				# exception added:
				if curStatus == 'Start A possession ':
					startBased_currentPossession = TeamAstring
				else:
					warn('\nCouldnt identify possession.\n<%s>' %curStatus)
					pdb.set_trace()
		elif 'turnover' in curStatus.lower():
			if TeamAstring.lower()+ ' to' in curStatus.lower():
				# team a to team b
				startBased_currentPossession = TeamBstring
				# targetEvents['Turnovers'].append((curTime,TeamAstring))
			elif TeamBstring.lower() + ' to' in curStatus.lower():
				# team b to team a
				startBased_currentPossession = TeamAstring
				# targetEvents['Turnovers'].append((curTime,TeamBstring))
			else:
				warn('\nCould not infer possession based on Turnover statement:\n<%s>\n\nNeed to infer possession based on previous event.\n' %curStatus)
				pdb.set_trace()
		else:
			warn('\nCould not identify what kind of start the current status was:\n<%s>' %curStatus)
			pdb.set_trace()
		# based on end
		endStatus = possessionEvents.loc[correspondingEnd_idx[0],'Possession/Turnover']

		if 'end' in endStatus.lower(): # 'start'-based start (and not turnover based)
			if TeamAstring in endStatus:
				# it's about Team A
				endBased_currentPossession = TeamAstring
			elif TeamBstring in endStatus:
				# it's about Team B
				endBased_currentPossession = TeamBstring
			else:
				warn('\nCouldnt identify possession.\n<%s>' %endStatus)
				pdb.set_trace()
		elif 'turnover' in endStatus.lower():
			if TeamAstring.lower() + ' to' in endStatus.lower():
				# team a to team b
				endBased_currentPossession = TeamAstring
				targetEvents['Turnovers'].append((tEnd,TeamBstring))
			elif TeamBstring.lower() + ' to' in endStatus.lower():
				# team b to team a
				endBased_currentPossession = TeamBstring
				targetEvents['Turnovers'].append((tEnd,TeamAstring))
			else:
				warn('\nCould not infer possession based on Turnover statement:\n<%s>\n\nNeed to infer possession based on previous event.\n' %endStatus)
				pdb.set_trace()
		else:
			warn('\nCould not identify what kind of end the current status was:\n<%s>' %endStatus)
			pdb.set_trace()

		if startBased_currentPossession != endBased_currentPossession:
			warn('\nWARNING: Mismatch in determining refteam for possession.\nStartPossession = <%s>\nEndPossession = <%s>.\n!!!!!!!!!!!!!!!' %((curStatus,endStatus)))

		targetEvents['Possession'].append((tEnd,startBased_currentPossession,tStart))

	# ###########################################
	# # the old way
	# possessionEvent = [(i,val) for i,val in enumerate(eventsPanda['Possession/Turnover']) if val  != '' and pd.notnull(val)]
	# dt = []

	# for idx,i in enumerate(possessionEvent):
	# 	curFrame = i[0] # frame
	# 	curTime = eventsPanda['Ts'][i[0]]
	# 	curStatus = i[1]
	# 	# Determine per event who has possession from that frame onward

	# 	if not idx == len(possessionEvent)-1:
	# 		endPossession = possessionEvent[idx+1][0]-1
	# 	else:
	# 		# CHECK THIS could be incorrect. When in doubt, double check:
	# 		# As there is no record at the end saying that possession ended, I just assumed that the last frame was the end of teh current possession
	# 		endPossession = None#[iq for iq,iv in enumerate(eventsPanda['Entity']['TeamID']) if iv == '' and eventsPanda['Ts'][iq] == max(eventsPanda['Ts'])]


	# 	if 'Start' in curStatus:
	# 		if TeamAstring in curStatus:
	# 			# it's about Team A
	# 			currentPossession = TeamAstring
	# 		elif TeamBstring in curStatus:
	# 			# it's about Team B
	# 			currentPossession = TeamBstring
	# 		else:
	# 			# exception added:
	# 			if curStatus == 'Start A possession ':
	# 				currentPossession = TeamAstring
	# 			else:
	# 				warn('\nCouldnt identify possession.\n%s' %curStatus)

	# 	elif 'End' in curStatus:
	# 		currentPossession = None

	# 	elif 'Turnover' in curStatus:
	# 		dt.append(float(eventsPanda['Ts'][curFrame+1]-eventsPanda['Ts'][curFrame])) # a cheecky way to read frame rate from data
	# 		if currentPossession == TeamAstring:
	# 			currentPossession = TeamBstring
	# 			targetEvents['Turnovers'].append((curTime,TeamAstring))
	# 		elif currentPossession == TeamBstring:
	# 			currentPossession = TeamAstring
	# 			targetEvents['Turnovers'].append((curTime,TeamBstring))
	# 		else:
	# 			currentPossession = None

	# 	else:
	# 		# Based on next status
	# 		if possessionEvent[idx+1][1][0:8] == 'Turnover' and possessionEvent[idx+1][1][-len(TeamAstring)-1:-1] == TeamAstring:
	# 			# The next turnover goes to TeamA, so:
	# 			currentPossession = TeamBstring
	# 			warn('\nIndirectly assessed event (based on next event):\n<<%s>>\nas <<%s>>' %(curStatus,currentPossession))

	# 		elif possessionEvent[idx+1][1][0:8] == 'Turnover' and possessionEvent[idx+1][1][-len(TeamBstring)-1:-1] == TeamBstring:
	# 			currentPossession = TeamAstring
	# 			warn('\nIndirectly assessed event (based on next event):\n<<%s>>\nas <<%s>>' %(curStatus,currentPossession))
	# 		else:
	# 			warn('\nCouldnt identify event:\n%s\n\n' %curStatus)
	# 			currentPossession = None

	# 	if curFrame == None:
	# 		in1 = None
	# 	else:
	# 		in1 = float(eventsPanda['Ts'][curFrame])
	# 	if endPossession == None:
	# 		in2 = None
	# 	else:
	# 		in2 = float(eventsPanda['Ts'][endPossession])
	# 	targetEvents['Possession'].append((in2,currentPossession,in1))
	# 	if not in2 == None and in1-in2<1:
	# 		print('end = %s' %in1)
	# 		print('start = %s\n' %in2)
	# 		print(curFrame)
	# 		pdb.set_trace()

	# if dt == []:
	# 	warn('\n!!!!\nExisting attributes seem to be missing.\nCouldnt find turnovers to estimate dt.\nOutput set as 999.')
	# 	frameTime = 0.1
	# 	overwriteOutput = True
	# else:
	# 	if round(sum(dt) / len(dt),7) == round(dt[0],7):
	# 		# Safe to assume that:
	# 		frameTime = dt[0]
	# 	else:
	# 		frameTime = round(sum(dt) / len(dt),7)
	# 		warn('\nNot sure about frameTime. Check that:\n frameTime = %f' %frameTime)

	return targetEvents

#################################################################
#################################################################

def passes(eventsPanda,TeamAstring,TeamBstring,targetEvents):
	targetEvents = {**targetEvents,'Passes':[]}
	# If an error occurs here, then this may be a problem with Linux.
	# Replace with: (and an if statement to check if targetEvents isempty)
	# targetEvents = targetEvents.update({'Passes':[]})
	if not 'Pass' in eventsPanda.keys():
		# No Pass information, so return immediately
		return targetEvents

	count = 0
	passes = [i for i in eventsPanda['Pass'] if i  != '' ]

	for idx,i in enumerate(eventsPanda['Pass']):
		if not i == '' and pd.notnull(i):
			if TeamAstring in i:
				targetEvents['Passes'].append((float(eventsPanda['Ts'][idx]),TeamAstring,None))
			elif TeamBstring in i:
				targetEvents['Passes'].append((float(eventsPanda['Ts'][idx]),TeamBstring,None))
			else:
				warn('\n\nCould not recognize team:\n<<%s>>' %i)
			if 'oal' in i:
				# ITS A GOAL NOT A PASS, so skip this file
				warn('\n!!!!!!!!!!!\nInconsistent data input: Goal was found in the passing column:\nEither improve code or clean up data.\n!!!!!!!!!!!')
				overwriteOutput = True
				break
			count = count + 1

	ind = 0
	if targetEvents['Possession'] != []:
		for idx,i in enumerate(targetEvents['Possession']):
			while targetEvents['Passes'][ind][0] >= i[2]: # after the start
				if targetEvents['Passes'][ind][0] <= i[0]: # before the end
					# Passing sequence is indicated by storing the nth possession per pass. This way, the number of consecutive passes can be easily deduced.
					targetEvents['Passes'][ind] = (targetEvents['Passes'][ind][0],targetEvents['Passes'][ind][1],idx)
					# current pass falls within current
					if len(targetEvents['Passes']) > ind+1:
						ind = ind + 1
					else:
						break
				else:
					# pass belongs in next section
					break
	else: # no possession info available
		if not 'Possession' in targetEvents.keys():
			warn('\nERROR: You should first import possession before importing passes (even if possesion is empty).')
		else:
			warn('\nWARNING: No possession information availabe: \nConsidering implementing a module that derives possession information from passes.')

	return targetEvents
#################################################################
#################################################################
