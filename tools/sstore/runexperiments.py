import Queue
import os, sys
import time


commandQueue = Queue()
INFILE = "experiments/experiment-queue.txt"
OUTFILE = "experiments/experiment-log.txt"
STILL_RUNNING = "--PYTHON SCRIPT STILL RUNNING--REMOVE TO ADD COMMANDS TO QUEUE--\n"
FINISHED_RUNNING = "--FINISHED_RUNNING--\n"
WAITTIME = 60
currentLine = 0

os.system(cmd)

def getCommands(filename):
	f = open(filename,'r')
	lines = f.readlines()
	for line in lines:
		if line.startswith(STILL_RUNNING) or line.startswith(FINISHED_RUNNING):
			return
		commandQueue.put(line)
	f.close()
	f = open(filename,'w')
	f.write(STILL_RUNNING)
	f.write("QUEUE SIZE: " + str(commandQueue.qsize()))
	f.write("LAST READ INTO QUEUE:")
	for line in lines:
		f.write(line + "\n")
	f.close()

def addToLog(filename, entry):
	f = open(filename, 'a')
	f.write(entry)
	f.close()

def checkForNew(filename):
	while True:
		getCommands(filename)
		time.sleep(WAITTIME)

start_new_thread(checkForNew, (INFILE))
time.sleep(5)
while True:
	if commandQueue.empty():
		f = open()





