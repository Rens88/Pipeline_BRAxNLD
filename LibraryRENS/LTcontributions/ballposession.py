def ballPossession(rawDict,attributeDict,attributeLabel,TeamAstring,TeamBstring):
	TeamVals = attributeDict[rawDict['PlayerID'] == 'groupRow'].set_index('Ts')
	newAttributes = pd.DataFrame(index = attributeDict.index, columns = ['ballPosession'])
	lowestDistance = 999999;
	playerInPossession = 0;

	#Search the ball in de DataFrame (Kan dit zonder for-loop)?
	for idx,i in enumerate(pd.unique(rawDict['PlayerID'])):
		ball = rawDict[rawDict['PlayerID'] == i]
		ballDict = ball.set_index('Ts')
		if all(ball['PlayerID'] == 'ball'):
			for idx,j in enumerate(pd.unique(rawDict['PlayerID'])):
				curPlayer = rawDict[rawDict['PlayerID'] == j]
				curPlayerDict = curPlayer.set_index('Ts')
				if all(ball['PlayerID'] == 'ball'):
					continue
				elif all(ball['PlayerID'] == 'groupRow'):
					continue
				else:
					curPlayer_distToBall = np.sqrt((curPlayerDict['X'] - ballDict['X'])**2 + (curPlayerDict['Y'] - ballDict['Y'])**2)
					if(curPlayer_distToBall < lowestDistance):
						lowestDistance = curPlayer_distToBall
						playerInPossession = curPlayer['PlayerID'];

	for idx,k in enumerate(pd.unique(rawDict['PlayerID'])):
		currentPlayer = rawDict[rawDict['PlayerID'] == k]
		currentPlayer = currentPlayer.set_index('Ts')
		if all(currentPlayer['PlayerID'] == playerInPossession):
			newAttributes['ballPosession'][currentPlayer.index] = 1;
		#elif all(currentPlayer['PlayerID'] != playerInPossession):
			#newAttributes['ballPosession'][currentPlayer.index] = 0;

	# Combine the pre-existing attributes with the new attributes:
	attributeDict = pd.concat([attributeDict, newAttributes], axis=1)

	##### THE STRINGS #####
	# Export a string label of each new attribute in the labels dictionary (useful for plotting purposes)
	tmpPossessionString = 'Bool Player in Possession (m)'
	attributeLabel.update({'InPossession':tmpPossessionString})

	return attributeDict,attributeLabel
