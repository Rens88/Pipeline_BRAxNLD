# 17-11-2017, Rens Meerhoff
# This function reads positional data.
# The variables are based on football data from inmotio.
# Other variable names can be added if you want.

import csv
import pdb; #pdb.set_trace()
import pandas as pd
import pydoc
from string import ascii_letters
from warnings import warn
import numpy as np


if __name__ == '__main__':
	readPosData(fname,readCols)

def readPosData(fname,readCols):
	# Read the CSV file
	Col = []
	with open(fname, 'r') as f:
    		reader = csv.reader(f)
    		headers = list(next(reader))
    		result = [[c for c in row] for row in reader]
    		for i, header_name in enumerate(headers):
        		print(header_name, [row[i] for row in result])
        		Col.append([row[i] for row in result])

    # Find columns of which the headers correspond to the columns to be read (readCols)
	dataOut = []
	storedInd = []
	duplYes = 'False'
	duplReported = 'False'
	for idx, val in enumerate(readCols):		
		for idx2, val2 in enumerate(headers):
			if val == val2:
				if not next((x for x in headers if x == val),None) == None:
					# A duplicate exists!
					duplYes = 'True'
				if not next((x for x in storedInd if x == idx2),None) == None:
					# A duplicate was reportedin input
					duplReported = 'True'
					'warn('Duplicate header, check which one was selected') # so continue and take the next
				else:
					dataOut.append(Col[idx2])
					storedInd.append(idx2)
					break
	if duplYes == 'True' and duplReported == 'False':
		# This is problematic, because it's unclear which column was intended
	elif duplYes == 'False' and duplReported == 'True':
		# This is also problematic, because now the same column will be exported twice

	return dataOut
	# print(dataOut)

	# 				# print((x for idx,x in storedInd if x == idx2))

				
	# 			# print(dataOut)
	# 			# # # Link it to panda

	# 			# # # print(val2)
	# 			# # # print(val)
	# 			# print('--')
	# 			# print(headers)
	# 			# print(tmpHeaders)
	# 			# tmpHeaders.remove(val2)
	# 			# print(headers)
	# 			# print(tmpHeaders)
				
	# 			# if not next((x for x in tmpHeaders if x == val),None) == None:
	# 			# 	# Duplicate column header	
	# 			# 	print('Duplicate!')
	# 			# 	dataOut.append(Col[idx2+1])
	# 			# 	print(dataOut)
	# 			# 	tmpHeaders.remove(val2)


	# 			# 	# Write solution for 'Timestamp' (search for milliseconds)
	# 			# 	# Otherwise, return warning 
	# 			# break








	# pdb.set_trace()

 #        			# First, read the headers
	# with open(fname, 'r') as f:
	# 	reader = csv.reader(f, delimiter=",")
	# 	headers = next(reader) # skips the first line as the header

	# # Then, import CSV using a panda with generic header names
	# colABC = ascii_letters[:len(headers)]
	# if len(headers) > len(ascii_letters):
	# 	warn('Too many (i.e., more than ascii_letters) columns in CSV file. Code skips the last columns. Solution is to find a set of strings that has other letter variations as well (e.g., AA, or A1).')
	# df = pd.read_csv(fname,sep = ',',header = 0,names=(colABC))

	# allData = df.values.tolist()
	# print(len(allData))
	# # print(allData.shape())
	# print(allData[0][0:1])
	# print(allData[0][0:2])
	# print(allData[0][0:3])
	# print(allData[:][0])
	# # print(next((x for x in headers if x.value == readCols[0]), None))
	# for idx, val in enumerate(readCols):		
	# 	for idx2, val2 in enumerate(headers):
	# 		if val == val2:
	# 			# This is the column you're after

	# 			# # Link it to panda

	# 			# # print(val2)
	# 			# # print(val)
	# 			# print('--')
	# 			# print(headers)
	# 			headers.remove(val2)
	# 			print('--After removing--')
	# 			if not next((x for x in headers if x == val),None) == None:
	# 				# Duplicate column header	
	# 				print('dosomething')
	# 				# Write solution for 'Timestamp' (search for milliseconds)
	# 				# Otherwise, return warning 
	# 			break

	# print(headers)

	# Col = []
	# with open(fname, 'r') as f:
 #    		reader = csv.reader(f)
 #    		headers = list(next(reader))
 #    		result = [[c for c in row] for row in reader]
 #    		for i, header_name in enumerate(headers):
 #        		print(header_name, [row[i] for row in result])
 #        		Col.append([row[i] for row in result])
 #        		# print(header_name, result)

	# print('Time',Col[0])
 #    # # df = pd.read_csv(fname,sep = ',',header = 0,usecols=['LPMTime', 'Y','X'])
	


	# # print('---')
	# # print(df.a[0:5])
	# # # print(headers)
	# # # print(df)
	# # print('---')
	

	# # # # df = pd.read_csv(fname,sep = ',',header = 0,usecols=lambda x: x() in ['X', 'Y'])
	# # # # print(df)
	# # # # print('---')
	# # df = pd.read_csv(fname,sep = ',',header = 0,names = ('A','B','C','D','F'))
	# # # print(df)
	# # print('---')
	# # tmp = list(df.A)
	# # print(tmp[:])
	# pdb.set_trace()
	# # saved_column = df.Timestamp
	# # print(saved_column)


	# # with open(fname, 'r') as f:
	# # 	reader = csv.reader(f, delimiter=",")
	# # 	# for row in reader:
	# # 	# 	print(row["Timestamp"])
	# # 	# rows = list(reader)	
	# # 	headers = next(reader) # skips the first line as the header
	# # 	for h in headers:
	# # 		print(h)
	# # 	var1,var2,*_ = zip(*reader)
	# # 	print(var1,var2)

	# # 	pdb.set_trace()
	# # 	print(headers)
	# # 	x = zip("Timestamp")
	# # 	print(x)
	# # 	print('---')

	# # 	#"Timestamp","Timestamp","Date/Time","LPMTime","X","Y","Direction","Speed","Speed2","dist to closest home","dist to closest visitor","Name","PlayerRole","Name","Area","Perimeter","centroid X","centroid Y","Avg dist group","LPW Ratio","Group length","Group Width","Inter team dist. X","Inter team dist. Y","Relative SI"
	# # 	TsIM, TsMS, TsDT, TsLPM, X, Y, Direction, Speed, Speed2,MinDistOwn, \
	# # 	MinDistOpp,PlayerID,Role,Team,Area,Perimeter,CentX,CentY,AvgDist,LPWratio, \
	# # 	GroupLength,GroupWidth,InterteamX,InterteamY,RelativeSI = zip(*reader)

	# # for row in rows:
	# # 	print(row)
	# # fname.close()
