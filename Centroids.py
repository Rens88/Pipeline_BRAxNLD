# -*- coding: utf-8 -*-
"""
Created on Mon Feb 19 15:59:09 2018

@author: Floris Goes
"""
import pandas as pd

""" 
The TeamCentroid function calculates the centroid, length and width of both teams. 
It takes 2 cleaned TeamPosition DataFrames as input. DataFrames are pivotted into 
multiple wide, single value, DataFrames for analysis. Every variable is temporarily stored 
in a seperate pivotted DataFrame. The output of the function is one DataFrame (Result)
with the X and Y Centroid positions and team length & width of both teams on every timestamp
"""
def TeamCentroid(df1, df2):
    #pivot X and Y dataframes for Team A
    Team_A_X = df1.pivot(columns='Naam', values='X')
    Team_A_Y = df1.pivot(columns='Naam', values='Y')
    #pivot X and Y dataframes for Team B
    Team_B_X = df2.pivot(columns='Naam', values='X')
    Team_B_Y = df2.pivot(columns='Naam', values='Y')   
    #Create empty DataFrame to store results
    Result = pd.DataFrame()
    #Append results as Series to empty DataFrame for team A
    Result['CentroidX Team A'] = Team_A_X.mean(axis=1, skipna=True)
    Result['CentroidY Team A'] = Team_A_Y.mean(axis=1, skipna=True)
    Result['Length Team A'] = Team_A_X.max(axis=1, skipna=True) - Team_A_X.min(axis=1, skipna=True)
    Result['Width Team A'] = Team_A_Y.max(axis=1, skipna=True) - Team_A_Y.min(axis=1, skipna=True)
    #Append results as Series to empty DataFrame for team B
    Result['CentroidX Team B'] = Team_B_X.mean(axis=1, skipna=True)
    Result['CentroidY Team B'] = Team_B_Y.mean(axis=1, skipna=True)
    Result['Length Team B'] = Team_B_X.max(axis=1, skipna=True) - Team_B_X.min(axis=1, skipna=True)
    Result['Width Team B'] = Team_B_Y.max(axis=1, skipna=True) - Team_B_Y.min(axis=1, skipna=True)
    #Return final dataframe for further analysis
    return Result

"""
The function below calculates the centroid for each line in a team for every timeframe in a match.
For this function to work you need to know the formation and positions of individual players and 
concat them to create Position DataFrames for every line. This function takes 3 Position DataFrames for each team
as input (Defenders, Midfielders & Attackers). The output of the function is returned as a DataFrame Result containing
line centroids for both teams in every timeframe
"""
def LineCentroids(df1, df2, df3, df4, df5, df6):
    #pivot X and Y dataframes for every line on Team A
    Team_A_DEF_X = df1.pivot(columns='Naam', values='X')
    Team_A_DEF_Y = df1.pivot(columns='Naam', values='Y')
    Team_A_MID_X = df2.pivot(columns='Naam', values='X')
    Team_A_MID_Y = df2.pivot(columns='Naam', values='Y')
    Team_A_ATT_X = df3.pivot(columns='Naam', values='X')
    Team_A_ATT_Y = df3.pivot(columns='Naam', values='Y')
    #pivot X and Y dataframes for every line on Team B
    Team_B_DEF_X = df4.pivot(columns='Naam', values='X')
    Team_B_DEF_Y = df4.pivot(columns='Naam', values='Y')
    Team_B_MID_X = df5.pivot(columns='Naam', values='X')
    Team_B_MID_Y = df5.pivot(columns='Naam', values='Y')
    Team_B_ATT_X = df6.pivot(columns='Naam', values='X')
    Team_B_ATT_Y = df6.pivot(columns='Naam', values='Y')
    #Create empty DataFrame
    Result = pd.DataFrame()
    #Append results as Series to empty DataFrame for team A
    Result['CentroidX DEF A'] = Team_A_DEF_X.mean(axis=1, skipna=True)
    Result['CentroidY DEF A'] = Team_A_DEF_Y.mean(axis=1, skipna=True)
    Result['CentroidX MID A'] = Team_A_MID_X.mean(axis=1, skipna=True)
    Result['CentroidY MID A'] = Team_A_MID_Y.mean(axis=1, skipna=True)
    Result['CentroidX ATT A'] = Team_A_ATT_X.mean(axis=1, skipna=True)
    Result['CentroidY ATT A'] = Team_A_ATT_Y.mean(axis=1, skipna=True)
    #Append results as Series to empty DataFrame for team B
    Result['CentroidX DEF B'] = Team_B_DEF_X.mean(axis=1, skipna=True)
    Result['CentroidY DEF B'] = Team_B_DEF_Y.mean(axis=1, skipna=True)
    Result['CentroidX MID B'] = Team_B_MID_X.mean(axis=1, skipna=True)
    Result['CentroidY MID B'] = Team_B_MID_Y.mean(axis=1, skipna=True)
    Result['CentroidX ATT B'] = Team_B_ATT_X.mean(axis=1, skipna=True)
    Result['CentroidY ATT B'] = Team_B_ATT_Y.mean(axis=1, skipna=True)
    #Return final dataframe for further analysis
    return Result

"""
The InterTeam_Dist functions computes the distances between the team centroids and all the
line centroids of Team A and B. It takes the 2 centroid DataFrames as input(one for team centroids and
one for line centroids) and outputs a single DataFrame with 20 columns for all X and Y
inter-team distances
"""
def InterTeam_Dist(df1, df2):
    #Start with an empty Dataframe
    Result = pd.DataFrame()
    #Append longitudinal inter-team distances as series to the empty DataFrame
    Result['InterTeam X'] = df1['CentroidX Team A'] - df1['CentroidX Team B']
    Result['X DEF A - ATT B'] = df2['CentroidX DEF A'] - df2['CentroidX ATT B']
    Result['X DEF A - MID B'] = df2['CentroidX DEF A'] - df2['CentroidX MID B']
    Result['X DEF A - DEF B'] = df2['CentroidX DEF A'] - df2['CentroidX DEF B']
    Result['X MID A - ATT B'] = df2['CentroidX MID A'] - df2['CentroidX ATT B']
    Result['X MID A - MID B'] = df2['CentroidX MID A'] - df2['CentroidX MID B']
    Result['X MID A - DEF B'] = df2['CentroidX MID A'] - df2['CentroidX DEF B']
    Result['X ATT A - ATT B'] = df2['CentroidX ATT A'] - df2['CentroidX ATT B']
    Result['X ATT A - MID B'] = df2['CentroidX ATT A'] - df2['CentroidX MID B']
    Result['X ATT A - DEF B'] = df2['CentroidX ATT A'] - df2['CentroidX DEF B']
    #Append horizontal inter-team distances as series to the empty DataFrame
    Result['InterTeam Y'] = df1['CentroidY Team A'] - df1['CentroidY Team B']
    Result['Y DEF A - ATT B'] = df2['CentroidY DEF A'] - df2['CentroidY ATT B']
    Result['Y DEF A - MID B'] = df2['CentroidY DEF A'] - df2['CentroidY MID B']
    Result['Y DEF A - DEF B'] = df2['CentroidY DEF A'] - df2['CentroidY DEF B']
    Result['Y MID A - ATT B'] = df2['CentroidY MID A'] - df2['CentroidY ATT B']
    Result['Y MID A - MID B'] = df2['CentroidY MID A'] - df2['CentroidY MID B']
    Result['Y MID A - DEF B'] = df2['CentroidY MID A'] - df2['CentroidY DEF B']
    Result['Y ATT A - ATT B'] = df2['CentroidY ATT A'] - df2['CentroidY ATT B']
    Result['Y ATT A - MID B'] = df2['CentroidY ATT A'] - df2['CentroidY MID B']
    Result['Y ATT A - DEF B'] = df2['CentroidY ATT A'] - df2['CentroidY DEF B']
    #Return final DataFrame
    return Result

"""
The IntraTeam_Dist functions computes the distances between the line centroids within a single Team. 
It takes the linecentroids DataFrame as input and outputs 2 DataFrames with 6 columns per DataFrame 
for all X and Y inter-line distances
"""
def IntraTeam_Dist(df):
    #Start with an empty Dataframe for Team A and one for Team B
    Result_A = pd.DataFrame()
    Result_B = pd.DataFrame()
    #Append longitudinal intra-line distances as series to the empty DataFrame    
    Result_A['X DEF - ATT'] = df['CentroidY DEF A'] - df['CentroidY ATT A']
    Result_A['X DEF - MID'] = df['CentroidY DEF A'] - df['CentroidY MID A']
    Result_A['X MID - ATT'] = df['CentroidY MID A'] - df['CentroidY ATT A']
    Result_A['Y DEF - ATT'] = df['CentroidY DEF A'] - df['CentroidY ATT A']
    Result_A['Y DEF - MID'] = df['CentroidY DEF A'] - df['CentroidY MID A']
    Result_A['Y MID - ATT'] = df['CentroidY MID A'] - df['CentroidY ATT A'] 
    Result_B['X DEF - ATT'] = df['CentroidY DEF B'] - df['CentroidY ATT B']
    Result_B['X DEF - MID'] = df['CentroidY DEF B'] - df['CentroidY MID B']
    Result_B['X MID - ATT'] = df['CentroidY MID B'] - df['CentroidY ATT B']
    Result_B['Y DEF - ATT'] = df['CentroidY DEF B'] - df['CentroidY ATT B']
    Result_B['Y DEF - MID'] = df['CentroidY DEF B'] - df['CentroidY MID B']
    Result_B['Y MID - ATT'] = df['CentroidY MID B'] - df['CentroidY ATT B']    
    #return final dataframes
    return [Result_A, Result_B]