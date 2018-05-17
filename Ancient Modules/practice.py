import numpy as np
import pandas as pd
import numpy as np

DirtyDataFiles = np.arange(0,103)
chuncks = 10
parallelProcess = (0,chuncks)


nFiles = len(DirtyDataFiles)
# print(nFiles / parallelProcess[1])
nPerProcess = np.floor(nFiles / parallelProcess[1])

if nPerProcess % parallelProcess[1] == 0:
	# need to divide the leftover trials evenly
	nLeftover = nFiles - nPerProcess * parallelProcess[1]
# 	if 
# 	print(nLeftover)

# 	nPerIthProcess = []
# 	for q in np.arange(0,parallelProcess[1]):
# 		if parallelProcess[1] % q == 0:
# 			print('hi')
# 			nPerIthProcess.append(nPerProcess+1)
# 		else:
# 			nPerIthProcess.append(nPerProcess)

# print(nPerIthProcess)
# print(sum(nPerIthProcess))

for i in np.arange(1,chuncks+1):
	# print(i)
	parallelProcess = (i,chuncks)
	if 

	if not parallelProcess[1] == 1:
		start = int((parallelProcess[0] * nPerProcess) - nPerProcess)
		end = int(((parallelProcess[0]+1) * nPerProcess) - nPerProcess -1)

	if parallelProcess[0] == parallelProcess[1]:
		end = len(DirtyDataFiles)

	# print(type(start))
	print(end-start)
