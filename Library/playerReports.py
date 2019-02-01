# dangerousity score rapporteren: elke 5 sec max dangerousity pakken, som nemen per kwartier
# grafiek over hele wedstrijd per speler en team weergeven.

import pandas as pd
import numpy as np
import math
import os
import pdb
import matplotlib.pyplot as plt
from scipy import stats
from warnings import warn

def process(rawPanda,attrPanda,TeamAstring,TeamBstring,playerReportFolder,matchName,debuggingMode,skipPlayerReports,allDict,checkVictor,checkLars):
	#Load CSV file with events
	# loadFilePath = "C:\\Users\\Lars\\Documents\\GitHub\\Pipeline_BRAxNLD\\Analyse\\Data\\SpatAgg\\TimeseriesAttributes_20180326_Portugal_Netherlands_1_cleaned.csv"
	# playerReportFolder = "D:\\KNVB\\Output\\Figs\\Player Reports\\"
	#TODO: check for dangerousity
	if checkLars:
		reportDanger(rawPanda,attrPanda,TeamAstring,TeamBstring,playerReportFolder,matchName,debuggingMode,skipPlayerReports,allDict,'dangerousity')
	if checkVictor:
		reportOffBall(rawPanda,attrPanda,TeamAstring,TeamBstring,playerReportFolder,matchName,debuggingMode,skipPlayerReports,allDict,'passPopRank')

	# print(attrPanda)
	# pdb.set_trace()
	# print(type(attrPanda[playerField].iloc[0]))
	# pdb.set_trace()
	# allDict = pd.concat([rawPanda, attrPanda], axis=1)
	# allDict = allDict.loc[:,~allDict.columns.duplicated()]

def reportDanger(rawPanda,attrPanda,TeamAstring,TeamBstring,playerReportFolder,matchName,debuggingMode,skipPlayerReports,allDict,fieldToPlot):
	playerField = 'PlayerID'
	teamField = 'TeamID'
	#create folder
	playerReportFolder = playerReportFolder + matchName + os.sep
	if not os.path.exists(playerReportFolder):
		os.makedirs(playerReportFolder)

	dfPlayers = allDict[playerField][(allDict[playerField] != 'ball') & (allDict[playerField] != 'groupRow')].unique()
	#type string gemaakt, omdat hier straks spelernamen in komen te staan
	dfPlayersA = allDict[playerField][(allDict[playerField] != 'ball') & (allDict[playerField] != 'groupRow') & (allDict[teamField] == TeamAstring)].unique()
	dfPlayersA = np.sort(dfPlayersA)
	dfPlayersB = allDict[playerField][(allDict[playerField] != 'ball') & (allDict[playerField] != 'groupRow') & (allDict[teamField] == TeamBstring)].unique()
	dfPlayersB = np.sort(dfPlayersB)
	dfTeams = allDict[teamField].bfill().ffill().unique()

	# maxTs = max(allDict['Ts'])
	# nrOfPeriods = (maxTs // 900) + 1

	dfDanger = allDict[allDict[fieldToPlot] > 0].sort_values(by=['Ts'])
	dfDanger = dfDanger.assign(fiveSeconds=dfDanger['Ts']//5,fifteenMinutes=dfDanger['Ts']//900)

	# dfDanger = dfDanger.assign(fiveSeconds=dfDanger['Ts']//5,fifteenMinutes=dfDanger['Ts']//900)

	#count timestamps in final third per player

	secondsInFinalThirdPlayer = dfDanger[dfDanger['inAttack']>0].groupby([teamField,playerField,'fifteenMinutes'])['Ts'].count()/10 #LT: fifteenMinutes hier ook toevoegen en dan in groupby toevoegen
	# secondsInFinalThirdPlayer = secondsInFinalThirdPlayer.rename(columns={'Ts': 'Seconds'})
	secondsInFinalThirdTeam = dfDanger[dfDanger['inAttack']>0].groupby([teamField,'fifteenMinutes'])['Ts'].count()/10
	# secondsInFinalThirdTeam = secondsInFinalThirdTeam.rename(columns={'Ts': 'Seconds'})

	# print(secondsInFinalThirdPlayer)

	#add every fifteenMinutes a zero value so that every team has always a value in fifteen minutes
	for team in dfTeams:
		for quart in range(0,4):#loop till 4 because of stoppage time
			dfDanger = dfDanger.append({teamField:team,'Ts':quart*900,fieldToPlot:0,'fiveSeconds':quart*180,'fifteenMinutes':quart},ignore_index=True)

	#Dangerousity waarden per team bepalen
	try:
		teamDataA = dfDanger[dfDanger[teamField] == TeamAstring].groupby([teamField,'fifteenMinutes','fiveSeconds',playerField])[fieldToPlot].max().groupby([teamField,'fifteenMinutes']).sum()
	except:
		teamDataA = []
		warn('\nWARNING: No dangerousity data for team A found.\n')
	try:
		teamDataB = dfDanger[dfDanger[teamField] == TeamBstring].groupby([teamField,'fifteenMinutes','fiveSeconds',playerField])[fieldToPlot].max().groupby([teamField,'fifteenMinutes']).sum()
	except:
		teamDataB = []
		warn('\nWARNING: No dangerousity data for team B found.\n')

	#add the stopage time to the last fifteen minutes
	if len(teamDataA) > 3:
		fourPeriods = True
		teamDataA[2] = teamDataA[2] + teamDataA[3]
		teamDataA = teamDataA.drop(teamDataA.index[3])
	else:
		fourPeriods = False
	if len(teamDataB) > 3:
		teamDataB[2] = teamDataB[2] + teamDataB[3]
		teamDataB = teamDataB.drop(teamDataB.index[3])

	#find max value as integer
	if len(teamDataA) == 0:
		maxDangerA = 1
	else:
		maxDangerA = math.ceil(max(teamDataA))

	if len(teamDataB) == 0:
		maxDangerB = 1
	else:
		maxDangerB = math.ceil(max(teamDataB))

	if maxDangerA > maxDangerB:
		maxDangerTeam = maxDangerA
	else:
		maxDangerTeam = maxDangerB

	plotTeam(teamDataA,teamDataB,dfTeams,playerReportFolder,maxDangerTeam,matchName,TeamAstring,TeamBstring, 'Dangerousity')
	# print(dfPlayers)
	# print(allDict[teamField][allDict[playerField]=='1214'])
	# pdb.set_trace()
	#add every fifteenMinutes a zero value so that every player has always a value in fifteen minutes
	for player in dfPlayers:
		for quart in range(0,4):
			# print(player,quart,allDict[teamField][allDict[playerField]==player].iloc[0])
			# pdb.set_trace()
			dfDanger = dfDanger.append({playerField:player,teamField:allDict[teamField][allDict[playerField]==player].iloc[0],'Ts':quart*900,fieldToPlot:0,'fiveSeconds':quart*180,'fifteenMinutes':quart},ignore_index=True)

	allPlayerData = dfDanger.groupby([teamField,'fifteenMinutes','fiveSeconds',playerField])[fieldToPlot].max().groupby([teamField,playerField,'fifteenMinutes']).sum()
	allTeamData = dfDanger.groupby([teamField,'fifteenMinutes','fiveSeconds',playerField])[fieldToPlot].max().groupby([teamField,'fifteenMinutes']).sum()
	# print(allPlayerData)
	# pdb.set_trace()

	dataToCSV(allTeamData,secondsInFinalThirdTeam,playerReportFolder,'Team',matchName,fourPeriods)
	dataToCSV(allPlayerData,secondsInFinalThirdPlayer,playerReportFolder,'Player',matchName,fourPeriods)

	#max of all the players to make a good comparison between the players
	maxDangerPlayer = math.ceil(max(allPlayerData)*10)/10

	#generate player individual graphs
	# for player in dfPlayers:
	# 	playerData = dfDanger[dfDanger[playerField] == player].groupby([teamField,'fifteenMinutes','fiveSeconds',playerField])[fieldToPlot].max().groupby([teamField,'fifteenMinutes',playerField]).sum()
	# 	#add the stopage time to the last fifteen minutes
	# 	playerData[2] = playerData[2] + playerData[3]
	# 	playerData = playerData.drop(playerData.index[3])
	# 	team = allDict[teamField][allDict[playerField]==player].iloc[0]
	# 	plotPlayer(playerData,player,playerReportFolder,maxDangerPlayer,team,matchName,TeamAstring)
	
	#generate graph for team A
	nrOfPlayersA = len(dfPlayersA)
	for player in dfPlayersA:
		playerData = dfDanger[dfDanger[playerField] == player].groupby([teamField,'fifteenMinutes','fiveSeconds',playerField])[fieldToPlot].max().groupby([teamField,'fifteenMinutes',playerField]).sum()
		#add the stopage time to the last fifteen minutes
		if fourPeriods:
			playerData[2] = playerData[2] + playerData[3]
			playerData = playerData.drop(playerData.index[3])
		plotAllPlayers(playerData,player,playerReportFolder,maxDangerPlayer,TeamAstring,dfPlayersA,player==dfPlayersA[nrOfPlayersA-1],matchName)

	#generate graph for Opponnent
	nrOfPlayersB = len(dfPlayersB)
	for player in dfPlayersB:
		playerData = dfDanger[dfDanger[playerField] == player].groupby([teamField,'fifteenMinutes','fiveSeconds',playerField])[fieldToPlot].max().groupby([teamField,'fifteenMinutes',playerField]).sum()
		#add the stopage time to the last fifteen minutes
		if fourPeriods:
			playerData[2] = playerData[2] + playerData[3]
			playerData = playerData.drop(playerData.index[3])
		plotAllPlayers(playerData,player,playerReportFolder,maxDangerPlayer,TeamBstring,dfPlayersB,player==dfPlayersB[nrOfPlayersB-1],matchName)

	return

def plotTeam(teamDataA,teamDataB,dfTeams,playerReportFolder,maxDangerTeam,matchName,TeamAstring,TeamBstring,method):
	# print(teamDataA,teamDataB)
	if len(teamDataA) == 0 and len(teamDataB) != 0:
		ax = teamDataB.plot(color='royalblue')
		plt.legend([TeamBstring])
	elif len(teamDataB) == 0:
		ax = teamDataA.plot(color='orange')
		plt.legend([TeamAstring])
	else:
		ax = teamDataA.plot(color='orange')
		teamDataB.plot(ax=ax,color='royalblue')
		plt.legend([TeamAstring,TeamBstring])

	plt.title('Team Performance')
	plt.ylabel(method + ' score')
	plt.xlabel('Fifteen Minutes')
	plt.xticks(np.arange(0, 3, step=1),['0-15','16-30','31-45+'])
	plt.ylim(bottom=0)
	plt.ylim(top=maxDangerTeam)

	strFile = playerReportFolder + matchName + ' Team ' + method + '.png'
	if os.path.isfile(strFile):
		os.remove(strFile)
	plt.savefig(strFile)

# LT: kan weg
def plotPlayer(playerData,player,playerReportFolder,maxDangerPlayer,team,matchName,TeamAstring):
	# print(playerData,player)
	# if(not playerData.empty):
	if team == TeamAstring:
		ax = playerData.plot(color='orange')
	else:
		ax = playerData.plot(color='royalblue')

	plt.title(player + ' Dangerousity Score')
	plt.ylabel('Dangerousity score')
	plt.xlabel('Fifteen Minutes')
	plt.xticks(np.arange(0, 3, step=1),['0-15','16-30','31-45+'])
	plt.ylim(bottom=0)
	plt.ylim(top=maxDangerPlayer)

	strFile = playerReportFolder + matchName +' Player ' + player +'.png'
	if os.path.isfile(strFile):
		os.remove(strFile)
	plt.savefig(strFile)
	plt.close()

def plotAllPlayers(playerData,player,playerReportFolder,maxDangerPlayer,TeamString,dfPlayers,boolLast,matchName):
	# print(player,boolLast)
	# if(not playerData.empty):
	ax = playerData.plot(figsize=(12,10))

	plt.ylabel('Dangerousity score')
	plt.xlabel('Fifteen Minutes')
	plt.xticks(np.arange(0, 3, step=1),['0-15','16-30','31-45+'])
	plt.ylim(bottom=0)
	plt.ylim(top=maxDangerPlayer)

	if boolLast:
		plt.title('Player Performance ' + TeamString )
		strFile = playerReportFolder + matchName + ' AllPlayers ' + TeamString + '.png'
		plt.legend(dfPlayers)
		# plt.show()
		# plt.close()
		
		if os.path.isfile(strFile):
			os.remove(strFile)
		plt.savefig(strFile)
		plt.close()

def plotPlayersCumulative(playerData,player,playerReportFolder,maxDangerPlayer,TeamString,dfPlayers,boolLast,matchName):
	# print(player,boolLast)
	# if(not playerData.empty):
	ax = playerData.plot(figsize=(12,10))

	plt.ylabel('Dangerousity score')
	plt.xlabel('Fifteen Minutes')
	plt.xticks(np.arange(0, 3, step=1),['0-15','16-30','31-45+'])
	plt.ylim(bottom=0)
	plt.ylim(top=maxDangerPlayer)

	if boolLast:
		plt.title('Player Performance ' + TeamString )
		strFile = playerReportFolder + matchName + ' AllPlayers ' + TeamString + '.png'
		plt.legend(dfPlayers)
		# plt.show()
		# plt.close()
		
		if os.path.isfile(strFile):
			os.remove(strFile)
		plt.savefig(strFile)
		plt.close()

def dataToCSV(allPlayerData,secondsInFinalThird,playerReportFolder,name,matchName,fourPeriods):
	pivotAllPlayerData = allPlayerData.unstack()
	pivotAllPlayerData.fillna(0,inplace=True)
	pivotSecInFinalThird = secondsInFinalThird.unstack()
	pivotSecInFinalThird.fillna(0,inplace=True)
	pivotAllPlayerData['Total Dangerousity'] = pivotAllPlayerData.select_dtypes(float).sum(1)
	pivotSecInFinalThird['Total Seconds'] = pivotSecInFinalThird.select_dtypes(float).sum(1)

	#add the stopage time to the last fifteen minutes and rename the columns
	if fourPeriods:
		pivotAllPlayerData[2] = pivotAllPlayerData[2] + pivotAllPlayerData[3]
		pivotAllPlayerData = pivotAllPlayerData.drop(pivotAllPlayerData.columns[3],axis=1)
		pivotSecInFinalThird[2] = pivotSecInFinalThird[2] + pivotSecInFinalThird[3]
		pivotSecInFinalThird = pivotSecInFinalThird.drop(pivotSecInFinalThird.columns[3],axis=1)

	#add seconds to series and sort
	pivotAllPlayerData = pivotAllPlayerData.join(pivotSecInFinalThird,lsuffix='_Danger', rsuffix='_Seconds')
	# print(pivotAllPlayerData)
	pivotAllPlayerData['Dangerousity per Second'] = pivotAllPlayerData['Total Dangerousity'] / pivotAllPlayerData['Total Seconds']
	pivotAllPlayerData = pivotAllPlayerData.sort_values('Total Dangerousity',ascending=False)

	pivotAllPlayerData = pivotAllPlayerData.rename(columns={'0.0_Danger':'0-15 Danger','1.0_Danger':'16-30 Danger','2.0_Danger':'31-45+ Danger'})
	pivotAllPlayerData.to_csv(playerReportFolder + matchName + ' ' + name +'.csv',header=True)
	# pdb.set_trace()

def reportOffBall(rawPanda,attrPanda,TeamAstring,TeamBstring,playerReportFolder,matchName,debuggingMode,skipPlayerReports,allDict,fieldToPlot):
	playerField = 'PlayerID'
	teamField = 'TeamID'
	#create folder
	playerReportFolder = playerReportFolder + matchName + os.sep
	if not os.path.exists(playerReportFolder):
		os.makedirs(playerReportFolder)

	dfPlayers = allDict[playerField][(allDict[playerField] != 'ball') & (allDict[playerField] != 'groupRow')].unique()
	#type string gemaakt, omdat hier straks spelernamen in komen te staan
	dfPlayersA = allDict[playerField][(allDict[playerField] != 'ball') & (allDict[playerField] != 'groupRow') & (allDict[teamField] == TeamAstring)].unique()
	dfPlayersA = np.sort(dfPlayersA)
	dfPlayersB = allDict[playerField][(allDict[playerField] != 'ball') & (allDict[playerField] != 'groupRow') & (allDict[teamField] == TeamBstring)].unique()
	dfPlayersB = np.sort(dfPlayersB)
	dfTeams = allDict[teamField].bfill().ffill().unique()

	# maxTs = max(allDict['Ts'])
	# nrOfPeriods = (maxTs // 900) + 1

	dfDanger = allDict[allDict[fieldToPlot] > 0].sort_values(by=['Ts'])
	dfDanger = dfDanger.assign(fiveSeconds=dfDanger['Ts']//5,fifteenMinutes=dfDanger['Ts']//900)

	# dfDanger = dfDanger.assign(fiveSeconds=dfDanger['Ts']//5,fifteenMinutes=dfDanger['Ts']//900)

	#count timestamps in final third per player

	# secondsInFinalThirdPlayer = dfDanger[dfDanger['inAttack']>0].groupby([teamField,playerField,'fifteenMinutes'])['Ts'].count()/10 #LT: fifteenMinutes hier ook toevoegen en dan in groupby toevoegen
	# secondsInFinalThirdPlayer = secondsInFinalThirdPlayer.rename(columns={'Ts': 'Seconds'})
	# secondsInFinalThirdTeam = dfDanger[dfDanger['inAttack']>0].groupby([teamField,'fifteenMinutes'])['Ts'].count()/10
	# secondsInFinalThirdTeam = secondsInFinalThirdTeam.rename(columns={'Ts': 'Seconds'})

	# print(secondsInFinalThirdPlayer)

	#add every fifteenMinutes a zero value so that every team has always a value in fifteen minutes
	for team in dfTeams:
		for quart in range(0,4):#loop till 4 because of stoppage time
			dfDanger = dfDanger.append({teamField:team,'Ts':quart*900,fieldToPlot:0,'fiveSeconds':quart*180,'fifteenMinutes':quart},ignore_index=True)

	#Dangerousity waarden per team bepalen
	try:
		teamDataA = dfDanger[dfDanger[teamField] == TeamAstring].groupby([teamField,'fifteenMinutes'])[fieldToPlot].sum()
	except:
		teamDataA = []
		warn('\nWARNING: No off-ball data for team A found.\n')
	try:
		teamDataB = dfDanger[dfDanger[teamField] == TeamBstring].groupby([teamField,'fifteenMinutes'])[fieldToPlot].sum()
	except:
		teamDataB = []
		warn('\nWARNING: No off-ball data for team B found.\n')

	#add the stopage time to the last fifteen minutes
	if len(teamDataA) > 3:
		fourPeriods = True
		teamDataA[2] = teamDataA[2] + teamDataA[3]
		teamDataA = teamDataA.drop(teamDataA.index[3])
	else:
		fourPeriods = False
	if len(teamDataB) > 3:
		teamDataB[2] = teamDataB[2] + teamDataB[3]
		teamDataB = teamDataB.drop(teamDataB.index[3])

	#find max value as integer
	if len(teamDataA) == 0:
		maxDangerA = 1
	else:
		maxDangerA = math.ceil(max(teamDataA))

	if len(teamDataB) == 0:
		maxDangerB = 1
	else:
		maxDangerB = math.ceil(max(teamDataB))

	if maxDangerA > maxDangerB:
		maxDangerTeam = maxDangerA
	else:
		maxDangerTeam = maxDangerB

	plotTeam(teamDataA,teamDataB,dfTeams,playerReportFolder,maxDangerTeam,matchName,TeamAstring,TeamBstring, 'Off-ball performance')
	print("GEHAAALDD!!!!")
	return
	# print(dfPlayers)
	# print(allDict[teamField][allDict[playerField]=='1214'])
	# pdb.set_trace()
	#add every fifteenMinutes a zero value so that every player has always a value in fifteen minutes
	for player in dfPlayers:
		for quart in range(0,4):
			# print(player,quart,allDict[teamField][allDict[playerField]==player].iloc[0])
			# pdb.set_trace()
			dfDanger = dfDanger.append({playerField:player,teamField:allDict[teamField][allDict[playerField]==player].iloc[0],'Ts':quart*900,fieldToPlot:0,'fiveSeconds':quart*180,'fifteenMinutes':quart},ignore_index=True)

	allPlayerData = dfDanger.groupby([teamField,'fifteenMinutes','fiveSeconds',playerField])[fieldToPlot].max().groupby([teamField,playerField,'fifteenMinutes']).sum()
	allTeamData = dfDanger.groupby([teamField,'fifteenMinutes','fiveSeconds',playerField])[fieldToPlot].max().groupby([teamField,'fifteenMinutes']).sum()
	# print(allPlayerData)
	# pdb.set_trace()

	dataToCSV(allTeamData,secondsInFinalThirdTeam,playerReportFolder,'Team',matchName,fourPeriods)
	dataToCSV(allPlayerData,secondsInFinalThirdPlayer,playerReportFolder,'Player',matchName,fourPeriods)

	#max of all the players to make a good comparison between the players
	maxDangerPlayer = math.ceil(max(allPlayerData)*10)/10

	#generate player individual graphs
	# for player in dfPlayers:
	# 	playerData = dfDanger[dfDanger[playerField] == player].groupby([teamField,'fifteenMinutes','fiveSeconds',playerField])[fieldToPlot].max().groupby([teamField,'fifteenMinutes',playerField]).sum()
	# 	#add the stopage time to the last fifteen minutes
	# 	playerData[2] = playerData[2] + playerData[3]
	# 	playerData = playerData.drop(playerData.index[3])
	# 	team = allDict[teamField][allDict[playerField]==player].iloc[0]
	# 	plotPlayer(playerData,player,playerReportFolder,maxDangerPlayer,team,matchName,TeamAstring)
	
	#generate graph for team A
	nrOfPlayersA = len(dfPlayersA)
	for player in dfPlayersA:
		playerData = dfDanger[dfDanger[playerField] == player].groupby([teamField,'fifteenMinutes','fiveSeconds',playerField])[fieldToPlot].max().groupby([teamField,'fifteenMinutes',playerField]).sum()
		#add the stopage time to the last fifteen minutes
		if fourPeriods:
			playerData[2] = playerData[2] + playerData[3]
			playerData = playerData.drop(playerData.index[3])
		plotAllPlayers(playerData,player,playerReportFolder,maxDangerPlayer,TeamAstring,dfPlayersA,player==dfPlayersA[nrOfPlayersA-1],matchName)

	#generate graph for Opponnent
	nrOfPlayersB = len(dfPlayersB)
	for player in dfPlayersB:
		playerData = dfDanger[dfDanger[playerField] == player].groupby([teamField,'fifteenMinutes','fiveSeconds',playerField])[fieldToPlot].max().groupby([teamField,'fifteenMinutes',playerField]).sum()
		#add the stopage time to the last fifteen minutes
		if fourPeriods:
			playerData[2] = playerData[2] + playerData[3]
			playerData = playerData.drop(playerData.index[3])
		plotAllPlayers(playerData,player,playerReportFolder,maxDangerPlayer,TeamBstring,dfPlayersB,player==dfPlayersB[nrOfPlayersB-1],matchName)

	return