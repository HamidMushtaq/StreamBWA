#!/usr/bin/python
#****************************************/
#	Script:		runSort.py	
#	Author:		Hamid Mushtaq  			
#****************************************/
# This part (Part2) will perform sorting of the combined sam file(s), mark the duplicates and produce bam files. 
#  This functionality is still not fully implemented.
from xml.dom import minidom
import sys
import os
import time
import subprocess
import multiprocessing
import glob

if len(sys.argv) < 3:
	print("Not enough arguments!")
	print("Example of usage: ./runSort.py streambwa_2.11-1.0.jar config.xml")
	sys.exit(1)

exeName = sys.argv[1]
logFile = "timings.txt"
configFilePath = sys.argv[2]

if not os.path.isfile(configFilePath):
	print("Config file " + configFilePath + " does not exist!")
	sys.exit(1)
	
doc = minidom.parse(configFilePath)
mode = doc.getElementsByTagName("mode")[0].firstChild.data
refPath = doc.getElementsByTagName("refPath")[0].firstChild.data
inputFolder = doc.getElementsByTagName("inputFolder")[0].firstChild.data
outputFolder = doc.getElementsByTagName("outputFolder")[0].firstChild.data
tmpFolder = doc.getElementsByTagName("tmpFolder")[0].firstChild.data
toolsFolder = doc.getElementsByTagName("toolsFolder")[0].firstChild.data
numExecutors = doc.getElementsByTagName("numExecutors")[0].firstChild.data
ignoreList = doc.getElementsByTagName("ignoreList")[0].firstChild
ignoreListPath = "" if (ignoreList == None) else ignoreList.data.strip()
numTasks = doc.getElementsByTagName("numTasks2")[0].firstChild.data
exe_mem = doc.getElementsByTagName("execMemGB")[0].firstChild.data + "g"
driver_mem = doc.getElementsByTagName("driverMemGB")[0].firstChild.data + "g"

def executeStreamBWA():	
	dictHDFSPath = refPath.replace(".fasta", ".dict")
	dictPath = './' + dictHDFSPath[dictHDFSPath.rfind('/') + 1:]
	
	if not os.path.exists(dictPath):
		os.system("hadoop fs -get " + dictHDFSPath)
	
	if not (os.path.exists(toolsFolder) and os.path.isdir(toolsFolder)):
		print "The specified tools folder (" + toolsFolder + ") doesn't exist!"
		sys.exit(1)
	else:
		bwaPath = toolsFolder + "/bwa"
		if not os.path.exists(bwaPath):
			print "The bwa executable (" + bwaPath + ") is not found!"
			sys.exit(1)
			
	tools = glob.glob(toolsFolder + '/*')
	toolsStr = ''
	for t in tools:
		toolsStr = toolsStr + t + ','
	toolsStr = toolsStr[0:-1]
	
	ignoreListStr = "" if (len(ignoreListPath) == 0) else ("," + ignoreListPath)
	
	cmdStr = "$SPARK_HOME/bin/spark-submit " + \
	"--jars lib/htsjdk-1.143.jar " + \
	"--class \"hmushtaq.streambwa.StreamBWA\" --master " + mode + " " + \
	"--files " + configFilePath + "," + dictPath + "," + toolsStr + ignoreListStr + " " + \
	"--driver-memory " + driver_mem + " --executor-memory " + exe_mem + " " + \
	"--num-executors " + numExecutors + " --executor-cores " + numTasks + " " + \
	exeName + " " + configFilePath + " 2"
	
	print cmdStr
	addToLog("[" + time.ctime() + "] " + cmdStr)
	os.system(cmdStr)
		
def addToLog(s):
	f = open(logFile,'a+')
	f.write(s + "\n")
	f.close() 

start_time = time.time()
	
executeStreamBWA()

addToLog("[" + time.ctime() + "]")
	
time_in_secs = int(time.time() - start_time)
mins = time_in_secs / 60
secs = time_in_secs % 60

print "|| Time taken = " + str(mins) + " mins " + str(secs) + " secs ||"
