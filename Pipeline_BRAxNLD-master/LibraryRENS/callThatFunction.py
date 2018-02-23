# 14-11-2017, Rens Meerhoff
# Based on "https://stackoverflow.com/questions/279237/import-a-module-from-a-relative-path"

import sys
import subprocess

############
# USER INPUT
############
# Provide the path where the python files are stored
cmd_folder = str("C:\\Users\\rensm\\Dropbox\\PYTHON\\LibraryRENS")

############
# END INPUT
############

if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder) 

############
# USER INPUT
############

## Uncomment this line to open the function in the editor (matlab's ctrl + d)
# subprocess.call(cmd_folder + "\\callThisFunction.py", shell=True)

# Now you can import any function in the library folder
import callThisFunction

# You can run a specific attribute of that function
print('-some_func-')
callThisFunction.some_func('print this',' and that')
# Or another attribute of that same function
print('-some_other_func-')
callThisFunction.some_other_func('print this',' and that')

# You can also export the variables that are returned. 
# Output arguments are returned in order.
# And again, you can specify which attribute in the function should be run.
argOut = callThisFunction.some_func('print this', ' and that')
print('In main: ' + argOut[0] + argOut[1])

# Different attributes of the same function can be exported and appended to the same var
argOut = []
argOut.append(callThisFunction.some_func('print this', ' and that'))
argOut.append(callThisFunction.some_other_func('print this', ' and that'))

print('----')
for idx, val in enumerate(argOut):
	print('As output argument ' + str(idx+1) + ': ' + str(val[0]))

############
# END INPUT
############