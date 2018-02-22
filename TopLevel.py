# -*- coding: utf-8 -*-
"""
Created on Fri Feb 16 15:16:25 2018
This is a process template for a package that can be used for the analysis of
spatiotemporal position and event data of one match at a time.
As it is now, the package can be used to:
    - import & clean LPM Data
    - Compute spatial summary measures per timeframe for a full match (Centroid, Width, Length, Stretch Index, 
    Line Centroids, Line Stretch Indices)
    - Compute intra-team and inter-team distance measures per timeframe for a full match.
TO DO:
    - Computer surface area using the convex hull
    - Load, Clean & Analyze event data
    - Data visualization
    - Statistical analysis/Comparison
    - Entropy analysis
    - Relative phase analysis
    - Saving results to a file

Author: Floris Goes
"""

import pandas as pd
import numpy as np
import Centroids
import StretchIndices
import Load_Clean


#USER INPUT = a *.csv file with a SpecialExport file with LPM Data
file='AA11531_AA1001_v_AA1003_vPP_SpecialExport.csv'

#Load & Clean the LPM position data
Loaded_Pos_Data = Load_Clean.LoadPosData(file)
Cleaned_Pos_Data = Load_Clean.CleanPosData(Loaded_Pos_Data)

#Slice cleaned DataFrame in several DataFrames for further analysis
Ball_Position = Load_Clean.SliceData_ball(Cleaned_Pos_Data)
[TeamA_Position, TeamA_Position_DEF, TeamA_Position_MID, TeamA_Position_ATT] = Load_Clean.SliceData_A(Cleaned_Pos_Data, 'AA1001')
[TeamB_Position, TeamB_Position_DEF, TeamB_Position_MID, TeamB_Position_ATT] = Load_Clean.SliceData_B(Cleaned_Pos_Data, 'AA1003')

#Compute team and line centroids
df_Centroid = Centroids.TeamCentroid(TeamA_Position, TeamB_Position)
df_LineCentroids = Centroids.LineCentroids(TeamA_Position_DEF, TeamA_Position_MID, TeamA_Position_ATT, TeamB_Position_DEF, TeamB_Position_MID, TeamB_Position_ATT)

#Compute inter-team distances
InterTeam_Dist = Centroids.InterTeam_Dist(df_Centroid, df_LineCentroids)

#Computer intra-team distance
[InterLine_Dist_A, InterLine_Dist_B] = Centroids.IntraTeam_Dist(df_LineCentroids)

#compute stretch indices relative to the team and line centroids
[TeamA_StretchX, TeamA_StretchY, TeamB_StretchX, TeamB_StretchY, TeamA_Vector, TeamB_Vector] = StretchIndices.StretchIndex(TeamA_Position, TeamB_Position, df_Centroid)
[DEF_A_Vector, MID_A_Vector, ATT_A_Vector, DEF_B_Vector, MID_B_Vector, ATT_B_Vector] = StretchIndices.LineStretchIndex(TeamA_Position_DEF, TeamA_Position_MID, TeamA_Position_ATT, 
TeamB_Position_DEF, TeamB_Position_MID, TeamB_Position_ATT, df_LineCentroids)

