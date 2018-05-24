import pandas as pd

attributeDict = pd.read_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/attributeDict.csv')
rawDict = pd.read_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/rawDict.csv')

inPossession = rawDict[attributeDict['inPossession'] == 1]
print(type(rawDict))
print(type(attributeDict))
print(inPossession)
print(inPossession.shape)
inPossession.to_csv('/Users/Victor/Desktop/Universiteit/AnalyseKNVB/inPossessionPractice.csv')
