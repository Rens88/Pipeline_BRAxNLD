09-03-2018, Rens Meerhoff
Another big update complete. The pipeline now uses pandas as dataframes and has two dedicated sections for students to work in. The update is functional up until spatialAggregation. temporalAggregation will follow soon. If you want to test the pipeline, follow these steps:
1) Make a local copy of the csv file that you can find in the root folder. Put it in your 'folder'.
2) Edit the location of this folder in the 'process_Template.py' user input section.
3) Build and run 'process_Template.py'. You will now run the code on the example CSV file. It will create a cleaned copy in the subfolder 'Cleaned' of your 'folder'. It will export a .csv file with the (not yet temporally aggregated) features that were computed.

TO THE STUDENTS:
You can easily work in this dataset by following the instructions in the files that you can find in XXcontributions, a subfolder of the library.
Please create your own branch on GitHub to share your progress. Using GitHub's desktop app avoids version conflicts and makes uploading new files very easy.

When in doubt, contact me at l.a.meerhoff@leidenuniv.liacs.nl





###################################################################################################################
###################################################################################################################
###################################################################################################################


09-02-2018, Rens Meerhoff
The big update is complete, but still needs to be finalized to be adapted for the LPM data. However,
this shouldn't require any big changes in the infrastructure (i.e., where which information is computed etc.).
In general, the template goes through the following steps:

USER INPUT, PREPARATION, ANALYSIS (file by file), IMPORT existing data, 
COMPUTE new attributes, EXPORT aggregated (temporally and spatially) data to CSV, VISUALIZATION

Below, I'll outline the idea behind this infrastructure and some important do's and don'ts. Also, note that
it is still very much a work in progress. Whenever you're bored, feel free to work on some of the 'CHANGE THIS',
'TO DO', or 'Idea'.

USER INPUT
Contains all info that needs to be specified before an analysis. It's almost complete, except for
decoding the filenames (which is still at the start of the big for loop in ANALYSIS.
Eventually, it would be nice to make this as a GUI where we also provide all possibilities for 
string inputs and where we immediately say whether an input is required or not.

PREPARATION
Reasonably straighforward preparation of variables that are the same for all files. Only one component,
cleanupData.py, will need some thorough editing for positional data from LPM. In this file, inconsistencies
like unnecssary spaces and different spelling forms (Team A and team A) are cleaned.

ANALYSIS
From here onward, the script goes through each file and exports the data. The order is quite important,
as in some cases the variables need to be available for the next.

IMPORT
This is where all existing data (including attributes) can be imported. This should work reasonably well.

COMPUTE
This is where all the computation is done. Note that we first do the spatial aggregation, which will
always make an aggregation that still has a value for every existing time frame.
During the temporal aggregation, it should be avoided to still use rawData (except for time).
I've always used time in seconds as a reference.
Note that targetEvents could be replaced by something that we enter manually (or read from an excel file).
Note that the dictionary attributeLabel contains the labels (string) that correspond to attributeDict
with the same strings for the keys of that dictionary.

EXPORT
Automatically adds the data from the file to a CSV file with all the aggregated data.

VISUALIZATION
Optional. Not entirely functional. Still need to allow for selection of which attributes are plotted.

--------------------------------------------------------------------------
--------------------------------------------------------------------------
--------------------------------------------------------------------------

14-11-2017, Rens Meerhoff

This folder contains all the working Python code.
The root folder contains scripts that are (or at least should be) for generic use.
The subfolder 'FDP' (and in future other subfolders) contains useful function that are tailored to a specific project.

Some scripts are examples that can be used as a template:
Calling another function - callThatFunction.py en callThisFunction.py serve as an example for calling a script in this folder as a function

21-11-2017
Convenient sublime shortcuts:
ctrl + shift + L --> edit multiple lines
alt + F3 --> edit all instances of the selected word
More here: http://www.sublimetext.com/docs/selection
