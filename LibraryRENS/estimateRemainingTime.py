import time

## Here, you can clarify which functions exist in this module.
if __name__ == '__main__': 
	# t => (time started,nth file,total number of files)
	printProgress(t)

## Here, you specifiy what each function does
def printProgress(t):
	# t => (time started,nth file,total number of files)
	nthFile = t[1]-1
	totalFiles = t[2]
	timeStarted = t[0]

	if timeStarted == []: # First file
		timeStarted = time.time()	# do stuff
		percentageProgress = 0
		elapsedTime = time.time() - timeStarted

		remainingTimeInHHMMSS = 'Unknown'
	else:

		percentageProgress = (nthFile) / totalFiles * 100
		elapsedTime = time.time() - timeStarted		
		estTimeRemaining_inSeconds = elapsedTime / percentageProgress * 100
		remainingTimeInHHMMSS = time.strftime('%H:%M:%S', time.gmtime(estTimeRemaining_inSeconds))

	percentageProgress  = str(round(percentageProgress, 2))	
	elapsedTime = str(round(elapsedTime, 2))
	print('%s out of %s files (%s%%) analyzed in %ss.\nEstimated time remaining: %s\n-------------------------------\n' %(nthFile,totalFiles,percentageProgress,elapsedTime,remainingTimeInHHMMSS))
	
	t = (timeStarted,nthFile + 2,totalFiles)
	return t
