import pandas as pd
from os.path import isfile, join, isdir, exists
from os import listdir, path, makedirs, sep, walk
import numpy as np
import pdb; #pdb.set_trace()

outputFolder = 'C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\Results\\'

file = 'Turnovers_allWindowsCombined_NOMISSING_noOverlap5s.csv'
file2 = 'Turnovers_allWindowsCombined_noOverlap5s.csv'
file3 = 'Turnovers_allWindowsCombined_NOMISSING.csv'
file4 = 'Turnovers_allWindowsCombined.csv'


df = pd.read_csv(outputFolder+file4,low_memory=True) # NB: low_memory MUST be True, otherwise it results in problems later on.

[print(k) for k in df.keys()]
