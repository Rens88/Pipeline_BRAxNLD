# 06-03-2018 Rens Meerhoff
# When working with large files, it can be easy to quickly reduce the filesize.
# NB: Check the lines after 'EDIT THIS'

import pandas as pd

# The folder that contains the file (should end with \\)
## EDIT THIS ##################################################################
dataFolder = 'C:\\Users\\rensm\\Documents\\PostdocLeiden\\BRAxNLD repository\\Data\\Cleaned\\'

# The filename
## EDIT THIS ##################################################################
fname = 'AA114105_AA1001_v_AA1012_vPP_SpecialExport_cleaned.csv'

# The strings of the column headers that you want to import.
## EDIT THIS ##################################################################
colHeaders = ['Ts','X','Y','Snelheid','Acceleration','PlayerID','TeamID']

df = pd.read_csv(dataFolder+fname,usecols=(colHeaders),low_memory=False)

# Make sure this is referencing the key that represents the timestamp. 
# Make sure the unit of measurement is appropriate.
## EDIT THIS ##################################################################
statement = (df['Ts'] < 20)# & df['Ts'] < 50)

# The new cropped dataframe
df_cropped = df[statement]

# Export the CSV
df_cropped.to_csv(dataFolder + 'CROPPED_' + fname)