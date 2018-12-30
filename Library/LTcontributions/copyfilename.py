import os
import shutil
import pdb

def navigate_and_rename(src,i):
	for i in range(0,67):
		if i % 2 == 0:
			shutil.copy("/vol/home/s1697994/Pipeline_BRAxNLD-LT/Analyse/Data/_AAA_BBB_.csv", os.path.join(src, str(i)+"_AAA_BBB_1.csv"))
		else:
			shutil.copy("/vol/home/s1697994/Pipeline_BRAxNLD-LT/Analyse/Data/_AAA_BBB_.csv", os.path.join(src, str(i)+"_AAA_BBB_2.csv")) 

dir_src = "/vol/home/s1697994/Pipeline_BRAxNLD-LT/Analyse/Data/"
navigate_and_rename(dir_src,0)