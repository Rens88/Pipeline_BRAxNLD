import tkinter as tk
from tkinter import ttk
from tkinter import filedialog
from tkinter import messagebox
from os import sep
from os.path import isdir
import csv
import process_Template

textboxWidth = 50
buttonWidth = 10
csvFile = "settings.csv"

def openSettingsFile():
	"""Initializes the dirInfo[] with in the information stored in settings.csv."""
	csvData = readCSV()
	dirInfo[0].set(csvData[0][0])
	dirInfo[1].set(csvData[0][1])
	dirInfo[2].set(csvData[0][2])

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
		return [["","",""]]

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
	openFilePath = filedialog.askdirectory(initialdir = dirInfo[idx].get(),
		title = "Open")
	if not openFilePath:
		return
	dirInfo[idx].set(openFilePath)

def saveFolder():
	"""Opens a filedialog which asks the user for a default directory to save to.

	The filepath is saved to the dirInfo[1] variable."""
	saveFilePath = filedialog.askdirectory(initialdir = dirInfo[2].get(),
		title = "Opslaan")
	if not saveFilePath:
		return
	dirInfo[2].set(saveFilePath)
	
def analyse():
	if saveSettingsFile():
		dirOpenCSV = dirInfo[0].get().replace('/',sep).replace('\\',sep) + sep
		dirOpenXML = dirInfo[1].get().replace('/',sep).replace('\\',sep) + sep
		dirSave = dirInfo[2].get().replace('/',sep).replace('\\',sep) + sep
		# print(dirOpenCSV,dirOpenXML,dirSave)
		if process_Template.process(dirOpenCSV,dirOpenXML,dirSave):
			messagebox.showinfo("Export klaar", "Export gelukt!")

root = tk.Tk()
frame = tk.Frame()
frame.winfo_toplevel().title("Speler Analyse")

dirInfo = []
csvLabel = tk.Label(width = textboxWidth, text="Locatie CSV bestanden")
dirInfo.append(tk.StringVar())
csvTextbox = tk.Entry(width = textboxWidth, textvariable=dirInfo[0])
csvButton = tk.Button(text='Open', width = buttonWidth, command= lambda: openFolder(0))

xmlLabel = tk.Label(width = textboxWidth, text="Locatie XML bestanden")
dirInfo.append(tk.StringVar())
xmlTextbox = tk.Entry(width = textboxWidth, textvariable=dirInfo[1])
xmlButton = tk.Button(text='Open', width = buttonWidth, command= lambda: openFolder(1))

saveLabel = tk.Label(width = textboxWidth, text="Locatie om op te slaan")
dirInfo.append(tk.StringVar())
saveTextbox = tk.Entry(width = textboxWidth, textvariable=dirInfo[2])
saveButton = tk.Button(text='Opslaan', width = buttonWidth, command=saveFolder)

#Werkt nog niet!
# progressMsgBox = messagebox.showinfo("Export bezig", "!")
# progressBar = ttk.Progressbar(progressMsgBox,orient="horizontal",length=250, mode="determinate")

startButton = tk.Button(text='Start', width = buttonWidth, command=analyse)
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
#progressBar.grid(row=7,column=0)
startButton.grid(row=8,column=0)

# tk.Button(root,text='Exit', command=root.destroy)

root.mainloop()
exit()