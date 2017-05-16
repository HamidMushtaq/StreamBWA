#!/usr/bin/python
#****************************************/
#	Script:		run.py	
#	Author:		Hamid Mushtaq  			
#****************************************/
from xml.dom import minidom
import sys
import os
import time
import subprocess
import math
import glob

USE_YARN_CLIENT_FOR_HADOOP = True

if len(sys.argv) < 2:
	print("Not enough arguments!")
	print("Example of usage: ./runPart.py config.xml")
	sys.exit(1)

exeName = "target/scala-2.11/streambwa_2.11-1.0.jar"
#exeName = "streambwa_2.11-1.0.jar"
logFile = "time.txt"
configFilePath = sys.argv[1]

if not os.path.isfile(configFilePath):
	print("Config file " + configFilePath + " does not exist!")
	sys.exit(1)

doc = minidom.parse(configFilePath)
refPath = doc.getElementsByTagName("refPath")[0].firstChild.data
inputFolder = doc.getElementsByTagName("inputFolder")[0].firstChild.data
outputFolder = doc.getElementsByTagName("outputFolder")[0].firstChild.data
tmpFolder = doc.getElementsByTagName("tmpFolder")[0].firstChild.data
toolsFolder = doc.getElementsByTagName("toolsFolder")[0].firstChild.data
numInstances = doc.getElementsByTagName("numInstances")[0].firstChild.data
numTasks = doc.getElementsByTagName("numTasks")[0].firstChild.data
exe_mem = doc.getElementsByTagName("execMemGB")[0].firstChild.data + "g"
driver_mem = doc.getElementsByTagName("driverMemGB")[0].firstChild.data + "g"

def executeHadoop():	
	if not (os.path.exists(toolsFolder) and os.path.isdir(toolsFolder)):
		print "The specified tools folder (" + toolsFolder + ") doesn't exist!"
		sys.exit(1)
	else:
		bwaPath = toolsFolder + "/bwa"
		if not os.path.exists(bwaPath):
			print "The bwa executable (" + bwaPath + ") is not found!"
			sys.exit(1)
		
	if USE_YARN_CLIENT_FOR_HADOOP:
		os.system('cp ' + configFilePath + ' ./')
		if not os.path.exists(tmpFolder):
			os.makedirs(tmpFolder)
			
	tools = glob.glob(toolsFolder + '/*')
	toolsStr = ''
	for t in tools:
		toolsStr = toolsStr + t + ','
	toolsStr = toolsStr[0:-1]
	
	diff_str = ("yarn-client --files " + toolsStr) if USE_YARN_CLIENT_FOR_HADOOP else ("yarn-cluster --files " + configFilePath + "," + toolsStr)
	
	cmdStr = "$SPARK_HOME/bin/spark-submit " + \
	"--class \"StreamBWA\" --master " + diff_str + " " + \
	"--driver-memory " + driver_mem + " --executor-memory " + exe_mem + " " + \
	"--num-executors " + numInstances + " --executor-cores " + numTasks + " " + \
	exeName + " " + os.path.basename(configFilePath)
	
	print cmdStr
	addToLog("[" + time.ctime() + "] " + cmdStr)
	os.system(cmdStr)
	
	if USE_YARN_CLIENT_FOR_HADOOP:
		os.remove('./' + configFilePath[configFilePath.rfind('/') + 1:])
	
def addToLog(s):
	f = open(logFile,'a+')
	f.write(s + "\n")
	f.close() 

start_time = time.time()

addToLog("########################################\n[" + time.ctime() + "] Part1 started.")
os.system("hadoop fs -rm -r -f -skipTrash " + outputFolder)
executeHadoop()
addToLog("[" + time.ctime() + "]")
	
time_in_secs = int(time.time() - start_time)
mins = time_in_secs / 60
secs = time_in_secs % 60

print "|| Time taken = " + str(mins) + " mins " + str(secs) + " secs ||"
