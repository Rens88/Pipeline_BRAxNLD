import sys, inspect
library_folder = "C:\\Users\\rensm\\Documents\\GitHub\\Pipeline_BRAxNLD\\LibraryRENS\\"
sys.path.insert(0, library_folder) 

import datasetVisualization
import numpy as np
import pandas as pd
import pdb; #pdb.set_trace()



###########
# User input
eventAggFolder = "C:\\Users\\rensm\\Documents\\PostdocLeiden\\NP repository\\Data\\EventAgg\\"
eventAggFname = 'eventExcerpt_Goal_window(10)_lag(0).csv'
cleanFname = 'StPt_X1A_3v4_POS_NP.csv'
plotTheseAttributes = [('SurfaceA', 'SurfaceB'),'vNorm' ]
aggregateLevel = ('Goal', 10, 0)
attrLabel = {}
tmpFigFolder = "C:\\Users\\rensm\\Documents\\PostdocLeiden\\NP repository\\Figs\\Temp\\Goal\\"
TeamAstring = 'Team A'
TeamBstring = 'Team B'
debuggingMode = True
dataType =  "NP" 

fileIdentifiers = ['School', 'Class', 'Group', 'Test', 'Exp']

outputFolder = 'C:\\Users\\rensm\\Documents\\PostdocLeiden\\NP repository\\Output\\'
attrLabelFname = 'attributeLabel.csv'


######################
# Code to be embedded

if attrLabel == {}:
	attrLabel_asPanda = pd.read_csv(outputFolder+attrLabelFname,low_memory=False)
else:
	attrLabel_asPanda = pd.DataFrame.from_dict([attrLabel],orient='columns')

eventExcerptPanda = pd.read_csv(eventAggFolder+eventAggFname,low_memory=False)

# In attrLabel_asPanda, create a column that identifies each event by combining
tmp = pd.DataFrame([], index = eventExcerptPanda.index, columns = ['UID'])
# eventExcerptPanda["EventUID"] = eventExcerptPanda[0] + eventExcerptPanda['temporalAggregate']
for idx,val in enumerate(fileIdentifiers):
	if idx == 0:
		tmp['UID'] = eventExcerptPanda[val]
	else:
		tmp['UID'] = tmp['UID'] + eventExcerptPanda[val]

# Unless the filename was already similar before, this should create unique identifiers per event
eventExcerptPanda["EventUID"] = tmp['UID'] + eventExcerptPanda['temporalAggregate']
# TO DO:
# Write a check to verify that the fileidentifiers combine into a unique ID...
# If not, this would be very problematic!!
# But.. It would also mean that the filenames in the raw data are the same / very similar (a space difference for example, or capitalization)


# !!!!!!!!! THIS IS WHERE YOU LEFT IT !!!!!!!!
# !!!!!!!!! THIS IS WHERE YOU LEFT IT !!!!!!!!
# !!!!!!!!! THIS IS WHERE YOU LEFT IT !!!!!!!!
# !!!!!!!!! THIS IS WHERE YOU LEFT IT !!!!!!!!

# Plotting overall data now has basic functionality --> it can plot the A and B version against each other.
# re-write it to plot different experiments against each other
# also to plot the scoring and defending team against each other etc.
pltFname = 'OVERALL PLOT_' + dataType
datasetVisualization.process(plotTheseAttributes,aggregateLevel,eventExcerptPanda,attrLabel_asPanda,tmpFigFolder,pltFname,TeamAstring,TeamBstring,debuggingMode)
