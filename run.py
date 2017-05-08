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
numInstances = doc.getElementsByTagName("numInstances" + configPart)[0].firstChild.data
numTasks = doc.getElementsByTagName("numTasks" + configPart)[0].firstChild.data
exe_mem = doc.getElementsByTagName("execMemGB" + configPart)[0].firstChild.data + "g"
driver_mem = doc.getElementsByTagName("driverMemGB" + configPart)[0].firstChild.data + "g"

def executeHadoop():	
	if USE_YARN_CLIENT_FOR_HADOOP:
		os.system('cp ' + configFilePath + ' ./')
		if not os.path.exists(tmpFolder):
			os.makedirs(tmpFolder)
	
	diff_str = "yarn-client" if USE_YARN_CLIENT_FOR_HADOOP else ("yarn-cluster --files " + configFilePath + "," + dictPath)
	
	cmdStr = "$SPARK_HOME/bin/spark-submit " + \
	"--class \"StreamBWA\" --master " + diff_str + " " + \
	"--driver-memory " + driver_mem + " --executor-memory " + em + " " + \
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

def run():
		
start_time = time.time()

addToLog("########################################\n[" + time.ctime() + "] Part1 started.")
os.system("hadoop fs -rm -r -f " + outputFolder)
executeHadoop()
addToLog("[" + time.ctime() + "] Part" + str(part) + " completed.")
	
time_in_secs = int(time.time() - start_time)
mins = time_in_secs / 60
secs = time_in_secs % 60

print "|| Time taken = " + str(mins) + " mins " + str(secs) + " secs ||"
