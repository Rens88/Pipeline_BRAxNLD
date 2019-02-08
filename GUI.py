import tkinter as tk
from tkinter import ttk
from tkinter import filedialog
from tkinter import messagebox
from os import sep
from os.path import isdir
import csv
import process_Template

textboxWidth = 50
labelWidth = 14
buttonWidth = 10
csvFile = "settings.csv"

def openSettingsFile():
	"""Initializes the dirInfo[] with in the information stored in settings.csv."""
	csvData = readCSV()
	dirInfo[0].set(csvData[0][0])
	dirInfo[1].set(csvData[0][1])
	dirInfo[2].set(csvData[0][2])
	parameterInfo[0].set(csvData[0][3])
	parameterInfo[1].set(csvData[0][4])
	parameterInfo[2].set(csvData[0][5])
	parameterInfo[3].set(csvData[0][6])
	parameterInfo[4].set(csvData[0][7])
	parameterInfo[5].set(csvData[0][8])
	parameterInfo[6].set(csvData[0][9])
	parameterInfo[7].set(csvData[0][10])
	parameterInfo[8].set(csvData[0][11])

def saveSettingsFile():
	"""Store the information from dirInfo[] to the settings.csv file."""
	writeData = []

	if isdir(dirInfo[0].get()):
		writeData.append(str(dirInfo[0].get()))
	else:
		messagebox.showerror("CSV locatie bestaat niet", "Opgegeven CSV locatie bestaat niet.")
		return False

	if isdir(dirInfo[1].get()):
		writeData.append(str(dirInfo[1].get()))
	else:
		messagebox.showerror("XML locatie bestaat niet", "Opgegeven XML locatie bestaat niet.")
		return False

	if isdir(dirInfo[2].get()):
		writeData.append(str(dirInfo[2].get()))
	else:
		messagebox.showerror("Opslaan locatie bestaat niet", "Opgegeven Opslaan locatie bestaat niet.")
		return False
		
	writeData.append(str(parameterInfo[0].get()))
	writeData.append(str(parameterInfo[1].get()))
	writeData.append(str(parameterInfo[2].get()))
	writeData.append(str(parameterInfo[3].get()))
	writeData.append(str(parameterInfo[4].get()))
	writeData.append(str(parameterInfo[5].get()))
	writeData.append(str(parameterInfo[6].get()))
	writeData.append(str(parameterInfo[7].get()))
	writeData.append(str(parameterInfo[8].get()))
	writeCSV(writeData)
	return True

def readCSV():
	"""Reads the information from .csv file and returns this as a list"""
	data = []
	try:
		with open(csvFile, 'r') as read:
			reader = csv.reader(read)
			for row in reader:
				data.append(row)
		return data
	except IOError:#beta
		return [["","","","","","","","","","","",""]]

def writeCSV(writeData):
	"""The list given as parameter is written to the .csv file"""
	with open(csvFile, "w") as write:
		writer = csv.writer(write)
		writer.writerows([writeData])

def simplifyDirInfo():
	"""Converts all variables in dirInfo[] to strings and returns these strings as a list."""
	simpleDirInfo = []
	for item in dirInfo:
		simpleDirInfo.append(item.get())
	return simpleDirInfo

def openFolder(idx):
	"""Opens a filedialog which asks the user for a default directory to open.

	The filepath is saved to the dirInfo[0] variable."""
	openFilePath = filedialog.askdirectory(initialdir = dirInfo[idx].get(),title = "Open")
	if not openFilePath:
		return
	dirInfo[idx].set(openFilePath)

def saveFolder():
	"""Opens a filedialog which asks the user for a default directory to save to.

	The filepath is saved to the dirInfo[1] variable."""
	saveFilePath = filedialog.askdirectory(initialdir = dirInfo[2].get(),title = "Opslaan")
	if not saveFilePath:
		return
	dirInfo[2].set(saveFilePath)
	
def analyse(checkVictor,checkLars):#,checkCleanup,checkSpatAgg):
	# print(checkVictor,checkLars)
	if not checkLars and not checkVictor:
		messagebox.showwarning("Geen selectie", "Maak een selectie voor Off-ball Performance of Dangerousity.")
		return
	if saveSettingsFile():
		dirOpenCSV = dirInfo[0].get().replace('/',sep).replace('\\',sep) + sep
		dirOpenXML = dirInfo[1].get().replace('/',sep).replace('\\',sep) + sep
		dirSave = dirInfo[2].get().replace('/',sep).replace('\\',sep) + sep
		# print(dirOpenCSV,dirOpenXML,dirSave)
		if process_Template.process(dirOpenCSV,dirOpenXML,dirSave,checkVictor,checkLars,checkCleanup.get(),checkSpatAgg.get(),parameterInfo[0],parameterInfo[1],parameterInfo[2],parameterInfo[3],parameterInfo[4],parameterInfo[5],parameterInfo[6],parameterInfo[7],parameterInfo[8]):
			messagebox.showinfo("Export klaar", "Export gelukt!")

root = tk.Tk()
masterFrame = tk.Frame()
masterFrame.winfo_toplevel().title("Speler Analyse")
note = ttk.Notebook(root)

tab1 = ttk.Frame(note)
tab2 = ttk.Frame(note)
tab3 = ttk.Frame(note)

note.add(tab1, text = "Locaties")
note.add(tab2, text = "Parameters")
note.add(tab3, text = "Instellingen")
note.pack()

########################   TAB 1   ########################
dirInfo = []
csvLabel = tk.Label(tab1,width = textboxWidth, text="Locatie CSV bestanden")
dirInfo.append(tk.StringVar())
csvTextbox = tk.Entry(tab1,width = textboxWidth, textvariable=dirInfo[0])
csvButton = tk.Button(tab1,text='Open', width = buttonWidth, command= lambda: openFolder(0))

xmlLabel = tk.Label(tab1,width = textboxWidth, text="Locatie XML bestanden")
dirInfo.append(tk.StringVar())
xmlTextbox = tk.Entry(tab1,width = textboxWidth, textvariable=dirInfo[1])
xmlButton = tk.Button(tab1,text='Open', width = buttonWidth, command= lambda: openFolder(1))

saveLabel = tk.Label(tab1,width = textboxWidth, text="Locatie om op te slaan")
dirInfo.append(tk.StringVar())
saveTextbox = tk.Entry(tab1,width = textboxWidth, textvariable=dirInfo[2])
saveButton = tk.Button(tab1,text='Opslaan', width = buttonWidth, command=saveFolder)

checkVictor = tk.IntVar()
checkLars = tk.IntVar()

checkButtonVictor = tk.Checkbutton(tab1,text = "Off-ball Performance", variable=checkVictor, onvalue = 1, offvalue = 0, height=1, width = 20, anchor="w")
checkButtonLars = tk.Checkbutton(tab1,text = "Dangerousity", variable=checkLars, onvalue = 1, offvalue = 0, height=1, width = 20, anchor="w")

#Werkt nog niet!
# progressMsgBox = messagebox.showinfo("Export bezig", "!")
# progressBar = ttk.Progressbar(progressMsgBox,orient="horizontal",length=250, mode="determinate")


########################   TAB 2   ########################
parameterInfo = []
tsLabel = tk.Label(tab2,width = labelWidth, anchor='w', text="Timestamp")
parameterInfo.append(tk.StringVar())
tsTextbox = tk.Entry(tab2,width = textboxWidth, textvariable=parameterInfo[0])

playerLabel = tk.Label(tab2,width = labelWidth, anchor='w', text="Player")
parameterInfo.append(tk.StringVar())
playerTextbox = tk.Entry(tab2,width = textboxWidth, textvariable=parameterInfo[1])

teamLabel = tk.Label(tab2,width = labelWidth, anchor='w', text="Team")
parameterInfo.append(tk.StringVar())
teamTextbox = tk.Entry(tab2,width = textboxWidth, textvariable=parameterInfo[2])

xLabel = tk.Label(tab2,width = labelWidth, anchor='w', text="X")
parameterInfo.append(tk.StringVar())
xTextbox = tk.Entry(tab2,width = textboxWidth, textvariable=parameterInfo[3])

yLabel = tk.Label(tab2,width = labelWidth, anchor='w', text="Y")
parameterInfo.append(tk.StringVar())
yTextbox = tk.Entry(tab2,width = textboxWidth, textvariable=parameterInfo[4])

shirtLabel = tk.Label(tab2,width = labelWidth, anchor='w', text="Shirt Nr.")
parameterInfo.append(tk.StringVar())
shirtTextbox = tk.Entry(tab2,width = textboxWidth, textvariable=parameterInfo[5])

possLabel = tk.Label(tab2,width = labelWidth, anchor='w', text="Ball Possession")
parameterInfo.append(tk.StringVar())
possTextbox = tk.Entry(tab2,width = textboxWidth, textvariable=parameterInfo[6])

speedLabel = tk.Label(tab2,width = labelWidth, anchor='w', text="Speed")
parameterInfo.append(tk.StringVar())
speedTextbox = tk.Entry(tab2,width = textboxWidth, textvariable=parameterInfo[7])

distClosestAwayLabel = tk.Label(tab2,width = labelWidth, anchor='w', text="Dist Closest Away")
parameterInfo.append(tk.StringVar())
distClosestAwayTextbox = tk.Entry(tab2,width = textboxWidth, textvariable=parameterInfo[8])

########################   TAB 3   ########################
checkCleanup = tk.IntVar()
checkSpatAgg = tk.IntVar()

checkButtonCleanup = tk.Checkbutton(tab3,text = "Skip Cleanup", variable=checkCleanup, onvalue = 1, offvalue = 0, height=1, width = 20, anchor="w")
checkButtonSpatAgg = tk.Checkbutton(tab3,text = "Skip Spatagg", variable=checkSpatAgg, onvalue = 1, offvalue = 0, height=1, width = 20, anchor="w")

#######################   START    #########################
startButton = tk.Button(tab1,text='Start', width = buttonWidth, command= lambda: analyse(checkVictor.get(),checkLars.get()))
openSettingsFile()

csvLabel.grid(row=0,column=0)
csvButton.grid(row=1,column=2)
csvTextbox.grid(row=1,column=0)
xmlLabel.grid(row=2,column=0)
xmlButton.grid(row=3,column=2)
xmlTextbox.grid(row=3,column=0)
saveLabel.grid(row=4,column=0)
saveButton.grid(row=5,column=2)
saveTextbox.grid(row=5,column=0)
checkButtonVictor.grid(row=6,column=0)
checkButtonLars.grid(row=7,column=0)
#progressBar.grid(row=7,column=0)
startButton.grid(row=8,column=0)

tsLabel.grid(row=0,column=0)
tsTextbox.grid(row=0,column=1)
playerLabel.grid(row=1,column=0)
playerTextbox.grid(row=1,column=1)
teamLabel.grid(row=2,column=0)
teamTextbox.grid(row=2,column=1)
xLabel.grid(row=3,column=0)
xTextbox.grid(row=3,column=1)
yLabel.grid(row=4,column=0)
yTextbox.grid(row=4,column=1)
shirtLabel.grid(row=5,column=0)
shirtTextbox.grid(row=5,column=1)
possLabel.grid(row=6,column=0)
possTextbox.grid(row=6,column=1)
speedLabel.grid(row=7,column=0)
speedTextbox.grid(row=7,column=1)
distClosestAwayLabel.grid(row=8,column=0)
distClosestAwayTextbox.grid(row=8,column=1)

checkButtonCleanup.grid(row=0,column=0)
checkButtonSpatAgg.grid(row=1,column=0)

root.mainloop()
exit()