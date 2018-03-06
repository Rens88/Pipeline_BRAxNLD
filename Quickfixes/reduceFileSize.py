# When working with large files, it can be easy to quickly reduce the filesize.
import pandas as pd


dataFolder = 'C:\\Users\\rensm\\Documents\\PostdocLeiden\\BRAxNLD repository\\Data\\Cleaned\\'
fname = 'AA114105_AA1001_v_AA1012_vPP_SpecialExport_cleaned.csv'
colHeaders = ['Ts','X','Y','Snelheid','Acceleration','PlayerID','TeamID']

df = pd.read_csv(dataFolder+fname,usecols=(colHeaders),low_memory=False)


statement = (df['Ts'] < 20)# & df['Ts'] < 50)

df_cropped = df[statement]

df_cropped.to_csv(dataFolder + fname)
