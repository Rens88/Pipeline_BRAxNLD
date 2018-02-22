# -*- coding: utf-8 -*-
"""
Created on Tue Feb 20 10:48:46 2018
The functions in this module can be used to import position data as a pandas DataFrame,
and subsequently clean, transform and slice into several DataFrames
@author: Floris Goes
"""
import pandas as pd

"""
Load a *.csv file with position data exported from LPM as a pandas DataFrame
"""
def LoadPosData(file):
    Loaded_Pos_Data = pd.read_csv(file, index_col='Timestamp',  
                         usecols=(['Timestamp', 'X', 'Y', 'Snelheid', 'Acceleration', 'Naam']), 
                         low_memory=False)
    
    return Loaded_Pos_Data 

"""
The Loaded_Data is cleaned in 4 steps. First, the player names are categorized to save memory space.
Second, rows with NaN values for X or Y coordinates are deleted to prevent errors during later analysis. 
Third, X and Y coordinates are limited to "realistic" coordinates on football field. Especially ball data 
often has malicious coordinates that need to be deleted. Finally the DataFrame is reindexed in order to create
a timeseries DataFrame and to match the index of exported event data. 

"""
def CleanPosData(df):
    df['Naam'] = df['Naam'].astype('category')
    clean = (df.X.notnull()) & (df.Y.notnull())
    Cleaned_1 = df[clean]
    Cleaned_2 = (Cleaned_1['X'] > -60) & (Cleaned_1['X'] < 60) & (Cleaned_1['Y'] > -40) & (Cleaned_1['Y'] < 40)
    Cleaned_Pos_Data = Cleaned_1[Cleaned_2]
    Cleaned_Pos_Data.index = pd.to_datetime(Cleaned_Pos_Data.index, unit='ms')
    
    return Cleaned_Pos_Data

"""
Create a seperate DataFrame with solely ball data. Input comes from Cleaned_Data
"""
def SliceData_ball(df):
    Ball_Position = pd.DataFrame(df[df['Naam'] == 'ball'])
    
    return Ball_Position

"""
Create seperate DataFrames for Team A and the 3 lines in team A.
Input comes from Cleaned_Data. NOTE: For this function to work you need to manually change 
player numbers and player positions/formation in the body of the function. 
"""
def SliceData_A(df, team='AA1001'):
    TeamA_Position1 = pd.DataFrame(df[df['Naam'] == '01_' + team])
    TeamA_Position2 = pd.DataFrame(df[df['Naam'] == '02_' + team])
    TeamA_Position4 = pd.DataFrame(df[df['Naam'] == '04_' + team])
    TeamA_Position5 = pd.DataFrame(df[df['Naam'] == '05_' + team])
    TeamA_Position6 = pd.DataFrame(df[df['Naam'] == '06_' + team])
    TeamA_Position7 = pd.DataFrame(df[df['Naam'] == '07_' + team])
    TeamA_Position9 = pd.DataFrame(df[df['Naam'] == '09_' + team])
    TeamA_Position10 = pd.DataFrame(df[df['Naam'] == '10_' + team])
    TeamA_Position11 = pd.DataFrame(df[df['Naam'] == '11_' + team])
    TeamA_Position15 = pd.DataFrame(df[df['Naam'] == '15_' + team])
    TeamA_Position17 = pd.DataFrame(df[df['Naam'] == '17_' + team])
    TeamA_Position18 = pd.DataFrame(df[df['Naam'] == '18_' + team])
    TeamA_Position29 = pd.DataFrame(df[df['Naam'] == '29_' + team])

    TeamA_Players = [TeamA_Position1, TeamA_Position2, TeamA_Position4, TeamA_Position5, TeamA_Position6,
                     TeamA_Position7, TeamA_Position9, TeamA_Position10, TeamA_Position11, TeamA_Position15,
                     TeamA_Position17, TeamA_Position18, TeamA_Position29]

    TeamA_Defenders = [TeamA_Position2, TeamA_Position4, TeamA_Position5, TeamA_Position15]
    TeamA_Midfielders = [TeamA_Position6, TeamA_Position10, TeamA_Position18, TeamA_Position29]
    TeamA_Attackers = [TeamA_Position7, TeamA_Position9, TeamA_Position11, TeamA_Position17]

    TeamA_Position = pd.concat(TeamA_Players)
    TeamA_Position_DEF = pd.concat(TeamA_Defenders)
    TeamA_Position_MID = pd.concat(TeamA_Midfielders)
    TeamA_Position_ATT = pd.concat(TeamA_Attackers)

    return [TeamA_Position, TeamA_Position_DEF, TeamA_Position_MID, TeamA_Position_ATT]

"""
Create seperate DataFrames for Team B and the 3 lines in team B.
Input comes from Cleaned_Data. NOTE: For this function to work you need to manually change 
player numbers and player positions/formation in the body of the function. 
"""
def SliceData_B(df, team='AA1003'):
    TeamB_Position1 = pd.DataFrame(df[df['Naam'] == '01_' + team])
    TeamB_Position2 = pd.DataFrame(df[df['Naam'] == '02_' + team])
    TeamB_Position3 = pd.DataFrame(df[df['Naam'] == '03_' + team])
    TeamB_Position5 = pd.DataFrame(df[df['Naam'] == '05_' + team])
    TeamB_Position10 = pd.DataFrame(df[df['Naam'] == '10_' + team])
    TeamB_Position11 = pd.DataFrame(df[df['Naam'] == '11_' + team])
    TeamB_Position12 = pd.DataFrame(df[df['Naam'] == '12_' + team])
    TeamB_Position16 = pd.DataFrame(df[df['Naam'] == '16_' + team])
    TeamB_Position19 = pd.DataFrame(df[df['Naam'] == '19_' + team])
    TeamB_Position20 = pd.DataFrame(df[df['Naam'] == '20_' + team])
    TeamB_Position21 = pd.DataFrame(df[df['Naam'] == '21_' + team])
    TeamB_Position25 = pd.DataFrame(df[df['Naam'] == '25_' + team])
    TeamB_Position26 = pd.DataFrame(df[df['Naam'] == '26_' + team])
    TeamB_Position27 = pd.DataFrame(df[df['Naam'] == '27_' + team])

    TeamB_Players = [TeamB_Position1, TeamB_Position2, TeamB_Position3, TeamB_Position5, TeamB_Position10,
                     TeamB_Position11, TeamB_Position12, TeamB_Position16, TeamB_Position19, TeamB_Position20,
                     TeamB_Position21, TeamB_Position25, TeamB_Position26, TeamB_Position27]

    TeamB_Defenders = [TeamB_Position2, TeamB_Position3, TeamB_Position5, TeamB_Position26]
    TeamB_Midfielders = [TeamB_Position10, TeamB_Position12, TeamB_Position25, TeamB_Position27]
    TeamB_Attackers = [TeamB_Position11, TeamB_Position16, TeamB_Position19, TeamB_Position20, TeamB_Position21]

    TeamB_Position = pd.concat(TeamB_Players)
    TeamB_Position_DEF = pd.concat(TeamB_Defenders)
    TeamB_Position_MID = pd.concat(TeamB_Midfielders)
    TeamB_Position_ATT = pd.concat(TeamB_Attackers)
    
    return [TeamB_Position, TeamB_Position_DEF, TeamB_Position_MID, TeamB_Position_ATT]