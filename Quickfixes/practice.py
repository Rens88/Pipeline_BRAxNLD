import numpy as np
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import colors as mcolors
import math
import pdb; #pdb.set_trace()
from os import listdir, stat, sep, rename#, path, makedirs
from os.path import isfile, join, exists, realpath, abspath, split,dirname, isdir#, isdir, exists

outputFolders = ['C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\BRAxNLD repository_newStyle\\Output\\27-05-2018\\']

for outputFolder in outputFolders:
	parallelOutputFiles = [f for f in listdir(outputFolder) if isfile(join(outputFolder, f)) if '.txt' in f]

	for f in parallelOutputFiles:
		rename(outputFolder + f,outputFolder + f[:-4] + '_beforeMemoryCrash.txt')