#!/usr/bin/python
#****************************************/
#	Script:		runWithChunker.py	
#	Author:		Hamid Mushtaq  			
#****************************************/
from xml.dom import minidom
import sys
import os
import time
import subprocess
import multiprocessing
import glob

if len(sys.argv) < 4:
	print("Not enough arguments!")
	print("Example of usage: ./runPart.py streambwa_2.11-1.0.jar config.xml chunkerConfig.xml")
	sys.exit(1)

exeName = sys.argv[1]
chunkerExeName = "chunker_2.11-1.0.jar"
logFile = "timings.txt"
configFilePath = sys.argv[2]
chunkerConfigFilePath = sys.argv[3]

if not os.path.isfile(configFilePath):
	print("Config file " + configFilePath + " does not exist!")
	sys.exit(1)
	
if not os.path.isfile(chunkerConfigFilePath):
	print("Chunker's config file " + chunkerConfigFilePath + " does not exist!")
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
numTasks = doc.getElementsByTagName("numTasks")[0].firstChild.data
exe_mem = doc.getElementsByTagName("execMemGB")[0].firstChild.data + "g"
driver_mem = doc.getElementsByTagName("driverMemGB")[0].firstChild.data + "g"

doc = minidom.parse(chunkerConfigFilePath)
inputFileName = doc.getElementsByTagName("fastq1Path")[0].firstChild.data
fastq2Path = doc.getElementsByTagName("fastq2Path")[0].firstChild
inputFileName2 = "" if (fastq2Path == None) else fastq2Path.data
outputFolderChunker = doc.getElementsByTagName("outputFolder")[0].firstChild.data
driver_mem_chunker = doc.getElementsByTagName("driverMemGB")[0].firstChild.data + "g"

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
	"--class \"hmushtaq.streambwa.StreamBWA\" --master " + mode + " " + \
	"--files " + configFilePath + "," + dictPath + "," + toolsStr + ignoreListStr + " " + \
	"--driver-memory " + driver_mem + " --executor-memory " + exe_mem + " " + \
	"--num-executors " + numExecutors + " --executor-cores " + numTasks + " " + \
	exeName + " " + configFilePath
	
	print cmdStr
	addToLog("[" + time.ctime() + "] " + cmdStr)
	os.system(cmdStr)
	
def executeChunker():
	cmdStr = "$SPARK_HOME/bin/spark-submit " + \
	"--class \"hmushtaq.fastqchunker.Chunker\" --master local[*] --driver-memory " + driver_mem_chunker + " " + chunkerExeName + " " + chunkerConfigFilePath
	
	print cmdStr
	os.system(cmdStr)
	
def addToLog(s):
	f = open(logFile,'a+')
	f.write(s + "\n")
	f.close() 

start_time = time.time()

if outputFolderChunker != inputFolder:
	print "The output folder of chunker: " + outputFolderChunker + ", is different than the input folder: " + inputFolder
	sys.exit(1)
	
# Remove the HDFS folders
os.system("hadoop fs -rm -r -f -skipTrash " + inputFolder)
os.system("hadoop fs -rm -r -f -skipTrash " + outputFolder)
# Start chunker
job1 = multiprocessing.Process(target=executeChunker)
job1.start()
# Start streamBWA
job2 = multiprocessing.Process(target=executeStreamBWA)
job2.start()
# Wait for both jobs to finish
job1.join()
job2.join()

addToLog("[" + time.ctime() + "]")
	
time_in_secs = int(time.time() - start_time)
mins = time_in_secs / 60
secs = time_in_secs % 60

print "|| Time taken = " + str(mins) + " mins " + str(secs) + " secs ||"
