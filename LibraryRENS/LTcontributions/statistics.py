import pandas as pd
import numpy as np
import pdb
import matplotlib.pyplot as plt
from scipy import stats

def start():
	#Load CSV file with events
	loadFilePath, target, targetValues, var1, var2, boxPlotTitle = input()
	dfFull = pd.read_csv(loadFilePath)

	# plot(dfFull,target,targetValues,var1, boxPlotTitle)
	statistics(dfFull,target,var1,var2)

def input():
	loadFilePath = "D:\\KNVB\\Output\\Samenvoegen\\Result\\output_attack_window(None)_lag(0)_11h06m_03_July_201combined_without_players.csv"
	target = 'eventClassification'
	targetValues = ['no shot','shot off target','shot on target','goal']
	var1 = 'Link_Total'
	var2 = 'distToGoal_min'
	boxPlotTitle = 'Boxplot Link'

	return loadFilePath, target, targetValues, var1, var2, boxPlotTitle

def plot(dfFull,target,targetValues,var1,boxPlotTitle):
	def boxPlot(dfFull,target,targetValues,var1,boxPlotTitle):
		#Data to plot
		data = dfFull[var1]
		noShotPlot = data[dfFull[target] == 0]
		shotOffPlot = data[dfFull[target] == 1]
		shotOnPlot = data[dfFull[target] == 2]
		goalPlot = data[dfFull[target] == 3]

		dataToPlot = [noShotPlot,shotOffPlot,shotOnPlot,goalPlot]

		#Make boxplot and set layout
		fig1, ax1 = plt.subplots()
		ax1.set_title(boxPlotTitle)
		ax1.boxplot(dataToPlot)
		# ax1.set_xlabel('test')
		# ax1.set_ylabel('Link Total')
		ax1.set_ylim(0, 1)
		ax1.set_xticklabels(targetValues)#, rotation=45)#, fontsize=8)

		plt.show()

		# dfPart = dfFull[['eventClassification','Link_Total']]
		# dfPivot = dfPart.pivot(columns=dfPart.columns[0],index=dfPart.index)

		# dfPivot.boxplot(labels=['A','B','C','D'])
		# fig = plt.figure()
		# fig.suptitle('bold figure suptitle', fontsize=14, fontweight='bold')
		# ax = fig.add_subplot(111)
		# ax.boxplot(dfPivot)

		# ax.set_title('axes title')
		# ax.set_xlabel('xlabel')
		# ax.set_ylabel('ylabel')
		# plt.show()

	def freqDistrPlot(dfFull,target,var1):
		data = dfFull[var1][dfFull[target] == 3]
		plt.hist(data, bins='auto')
		plt.show()

	boxPlot(dfFull,target,targetValues,var1,boxPlotTitle)
	freqDistrPlot(dfFull,target,var1)

def statistics(dfFull,target,var1,var2):
	def skewness(dfFull,target,var1):
		print('Skewness:')
		data = dfFull[var1]
		noShot = list(data[dfFull[target] == 0])
		shotOff = list(data[dfFull[target] == 1])
		shotOn = list(data[dfFull[target] == 2])
		goal = list(data[dfFull[target] == 3])
		skew = stats.skew(noShot, axis=0, bias=True)
		print('NoShot:       ',skew)
		skew = stats.skew(shotOff, axis=0, bias=True)
		print('ShotOffTarget:',skew)
		skew = stats.skew(shotOn, axis=0, bias=True)
		print('ShotOnTarget: ',skew)
		skew = stats.skew(goal, axis=0, bias=True)
		print('Goal:         ',skew)
		print('')

	def correlation():
		#wss Spearmens rho gebruiken omdat ik ordinal target heb
		return

	def normalTest(dfFull,target,var1):
		print('Normal Test:')
		data = dfFull[var1]
		alpha = 0.05
		noShot = list(data[dfFull[target] == 0])
		shotOff = list(data[dfFull[target] == 1])
		shotOn = list(data[dfFull[target] == 2])
		goal = list(data[dfFull[target] == 3])
		s2, p = stats.normaltest(noShot)
		if p < alpha:  # null hypothesis: x comes from a normal distribution
			print('NoShot:       ',p,"The null hypothesis can be rejected. Significant that x does not come from a normal distribution. So use Kruskal-Wallis.")
		else:
			print('NoShot:       ',p, "The null hypothesis cannot be rejected. Significant that x comes from a normal distribution. So use ANOVA.")
		s2, p = stats.normaltest(shotOff)
		if p < alpha:  # null hypothesis: x comes from a normal distribution
			print('ShotOffTarget:',p,"The null hypothesis can be rejected. Significant that x does not come from a normal distribution. So use Kruskal-Wallis.")
		else:
			print('ShotOffTarget:',p, "The null hypothesis cannot be rejected. Significant that x comes from a normal distribution. So use ANOVA.")
		s2, p = stats.normaltest(shotOn)
		if p < alpha:  # null hypothesis: x comes from a normal distribution
			print('ShotOnTarget: ',p,"The null hypothesis can be rejected. Significant that x does not come from a normal distribution. So use Kruskal-Wallis.")
		else:
			print('ShotOnTarget: ',p, "The null hypothesis cannot be rejected. Significant that x comes from a normal distribution. So use ANOVA.")
		s2, p = stats.normaltest(goal)
		if p < alpha:  # null hypothesis: x comes from a normal distribution
			print('Goal:         ',p,"The null hypothesis can be rejected. Significant that x does not come from a normal distribution. So use Kruskal-Wallis.")
		else:
			print('Goal:         ',p, "The null hypothesis cannot be rejected. Significant that x comes from a normal distribution. So use ANOVA.")
		print('')

	def kruskalWallis(dfFull,target,var1):
		data = dfFull[var1]
		noShot = list(data[dfFull[target] == 0])
		shotOff = list(data[dfFull[target] == 1])
		shotOn = list(data[dfFull[target] == 2])
		goal = list(data[dfFull[target] == 3])

		H, pval = stats.mstats.kruskal(noShot,shotOff,shotOn,goal)
		print('Kruskal-Wallis:')
		print("H-statistic:", H)
		print("P-Value:    ", pval)

		if pval < 0.05:
		    print("Reject NULL hypothesis - Significant differences exist between groups.")
		if pval > 0.05:
		    print("Accept NULL hypothesis - No significant difference between groups.")

	skewness(dfFull,target,var1)
	# correlation()
	normalTest(dfFull,target,var1)
	kruskalWallis(dfFull,target,var1)

start()