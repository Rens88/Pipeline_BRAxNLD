import csv

folder = 'C:\\Users\\rensm\\Documents\\PostdocLeiden\\NP repository\\Output\\'
filename = 'attributeLabel.csv'

with open(folder+filename, 'r') as f:
	reader = csv.reader(f)
	tmpHeaders = list(next(reader))

print(tmpHeaders)