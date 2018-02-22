# -*- coding: utf-8 -*-
"""
Created on Tue Feb 20 10:32:07 2018
The functions in this module computer stretch indices for individual players
@author: Floris Goes
"""
import pandas as pd
import numpy as np

""" 
The StretchIndex function calculates the individual distance to the team centroid for every player on every timestamp.
It takes 2 cleaned TeamPosition DataFrames as input and requires the input of the globally stored DataFrame df_Centroid. 
The output of the StretchIndex function is a X and Y distance from the team centroid. 
A vector (v) is subsequently defined with v**2 = x**2 + y**2
""" 
def StretchIndex(df1, df2, dfC):
    #pivot X and Y dataframes for Team A
    Team_A_X = df1.pivot(columns='Naam', values='X')
    Team_A_Y = df1.pivot(columns='Naam', values='Y')
    #pivot X and Y dataframes for Team B
    Team_B_X = df2.pivot(columns='Naam', values='X')
    Team_B_Y = df2.pivot(columns='Naam', values='Y') 
    #Change pivotted DataFrames so that they represent stretch indices instead of positions
    for i in Team_A_X.columns:
        Team_A_X[i] = Team_A_X[i].subtract(dfC['CentroidX Team A'])       
    for i in Team_A_Y.columns:
        Team_A_Y[i] = Team_A_Y[i].subtract(dfC['CentroidY Team A'])          
    for i in Team_B_X.columns:
        Team_B_X[i] = Team_B_X[i].subtract(dfC['CentroidX Team B'])        
    for i in Team_B_Y.columns:
        Team_B_Y[i] = Team_B_Y[i].subtract(dfC['CentroidY Team B'])        
    #Compute vectors of Stretch Indices
    tempA = Team_A_X**2 + Team_A_Y**2
    tempB = Team_B_X**2 + Team_B_Y**2
    TeamA_Vector = tempA.apply(np.sqrt)
    TeamB_Vector = tempB.apply(np.sqrt)
    #return output in seperate dataframes
    return [Team_A_X, Team_A_Y, Team_B_X, Team_B_Y, TeamA_Vector, TeamB_Vector]

""" 
The LineStretchIndex function calculates the individual distance to the line centroid for every player on every timestamp in a
specific line. It takes 3 cleaned Line Position DataFrames per team as input and requires the input of the 
globally stored DataFrame df_LineCentroids. The output of the LineStretchIndex function is a X and Y distance from the line centroid. 
A vector (v) is subsequently defined with v**2 = x**2 + y**2
""" 
def LineStretchIndex(df1, df2, df3, df4, df5, df6, dfLC):
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
    #Change pivotted DataFrames so that they represent stretch indices instead of positions
    for i in Team_A_DEF_X.columns:
        Team_A_DEF_X[i] = Team_A_DEF_X[i].subtract(dfLC['CentroidX DEF A'])
    for i in Team_A_DEF_Y.columns:
        Team_A_DEF_Y[i] = Team_A_DEF_Y[i].subtract(dfLC['CentroidY DEF A']) 
    for i in Team_A_MID_X.columns:
        Team_A_MID_X[i] = Team_A_MID_X[i].subtract(dfLC['CentroidX MID A'])
    for i in Team_A_MID_Y.columns:
        Team_A_MID_Y[i] = Team_A_MID_Y[i].subtract(dfLC['CentroidY MID A'])   
    for i in Team_A_ATT_X.columns:
        Team_A_ATT_X[i] = Team_A_ATT_X[i].subtract(dfLC['CentroidX ATT A'])
    for i in Team_A_ATT_Y.columns:
        Team_A_ATT_Y[i] = Team_A_ATT_Y[i].subtract(dfLC['CentroidY ATT A']) 
        
    for i in Team_B_DEF_X.columns:
        Team_B_DEF_X[i] = Team_B_DEF_X[i].subtract(dfLC['CentroidX DEF B'])
    for i in Team_B_DEF_Y.columns:
        Team_B_DEF_Y[i] = Team_B_DEF_Y[i].subtract(dfLC['CentroidY DEF B']) 
    for i in Team_B_MID_X.columns:
        Team_B_MID_X[i] = Team_B_MID_X[i].subtract(dfLC['CentroidX MID B'])
    for i in Team_B_MID_Y.columns:
        Team_B_MID_Y[i] = Team_B_MID_Y[i].subtract(dfLC['CentroidY MID B'])   
    for i in Team_B_ATT_X.columns:
        Team_B_ATT_X[i] = Team_B_ATT_X[i].subtract(dfLC['CentroidX ATT B'])
    for i in Team_B_ATT_Y.columns:
        Team_A_ATT_Y[i] = Team_B_ATT_Y[i].subtract(dfLC['CentroidY ATT B']) 
    #Compute vectors of Stretch Indices
    tempA_DEF = Team_A_DEF_X**2 + Team_A_DEF_Y**2
    tempA_MID = Team_A_MID_X**2 + Team_A_MID_Y**2
    tempA_ATT = Team_A_ATT_X**2 + Team_A_ATT_Y**2
    DEF_A_Vector = tempA_DEF.apply(np.sqrt)
    MID_A_Vector = tempA_MID.apply(np.sqrt)
    ATT_A_Vector = tempA_ATT.apply(np.sqrt)
    
    tempB_DEF = Team_B_DEF_X**2 + Team_B_DEF_Y**2
    tempB_MID = Team_B_MID_X**2 + Team_B_MID_Y**2
    tempB_ATT = Team_B_ATT_X**2 + Team_B_ATT_Y**2
    DEF_B_Vector = tempB_DEF.apply(np.sqrt)
    MID_B_Vector = tempB_MID.apply(np.sqrt)
    ATT_B_Vector = tempB_ATT.apply(np.sqrt)
    #return output in seperate dataframes
    return [DEF_A_Vector, MID_A_Vector, ATT_A_Vector, DEF_B_Vector, MID_B_Vector, ATT_B_Vector]