/*
 * Copyright (C) 2016-2017 Hamid Mushtaq
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.SparkFiles
import sys.process._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter

import java.io._
import java.nio.file.{Paths, Files}
import java.net._
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.sys.process.Process
import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.mutable._
import scala.util.Sorting._
import scala.concurrent.Future
import scala.concurrent.forkjoin._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.Random

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import tudelft.utils._
import utils._

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.HashPartitioner

import htsjdk.samtools.util.BufferedLineReader
import htsjdk.samtools._

object StreamBWA
{
val compressRDDs = true
//////////////////////////////////////////////////////////////////////////////
def bwaRun (chunkNum: Int, config: Configuration) : Int =
{
	val downloadNeededFiles = config.getDownloadRef.toBoolean
	val interleaved = config.getInterleaved.toBoolean
	val singleEnded = config.getSingleEnded.toBoolean
	val singleFile = interleaved || singleEnded
	val streaming = config.getStreaming.toBoolean
	val x = chunkNum.toString
	val inputFileName = if (singleFile) (x + ".fq.gz") else (x + "-1.fq.gz")
	val inputFileName2: String = if (singleFile) null else (x + "-2.fq.gz")
	val hdfsManager = new HDFSManager
	var r = 0
	
	var t0 = System.currentTimeMillis
	
	hdfsManager.create(config.getOutputFolder + "log/" + chunkNum)
	val file = new File("./bwa") 
	if (!file.exists)
	{
		LogWriter.dbgLog(x, t0, "!\tThe bwa program does not exist!", config)
		return 1
	}
	val bwaPath = SparkFiles.get("bwa")
	val bwaDir = bwaPath.split('.')(0)
	LogWriter.dbgLog(x, t0, ".\tThe bwa dir is -> " + bwaDir, config)
	file.setExecutable(true)
	if (downloadNeededFiles)
	{
		LogWriter.dbgLog(x, t0, "download\tDownloading reference files for bwa", config)
		if (DownloadManager.downloadBWAFiles(x, config) != 0)
			return 1
	}
	
	LogWriter.dbgLog(x, t0, ".\tinputFileName = " + inputFileName + ", singleFile = " + singleFile + ", streaming = " + streaming, config)
	
	if (!Files.exists(Paths.get(FilesManager.getRefFilePath(config))))
	{
		LogWriter.dbgLog(x, t0, "!\tReference file " + FilesManager.getRefFilePath(config) + " not found on this node!", config)
		return 1
	}
	
	if (streaming)
	{
		while(!hdfsManager.exists(config.getInputFolder + "ulStatus/" + chunkNum))
		{
			if (hdfsManager.exists(config.getInputFolder + "ulStatus/end.txt"))
			{
				if (!hdfsManager.exists(config.getInputFolder + "ulStatus/" + chunkNum))
				{
					LogWriter.dbgLog(x, t0, "#\tchunkNum = " + chunkNum + ", end.txt exists but this file doesn't!", config)
					return 1
				}
			}
			Thread.sleep(1000)
		}
	}
		
	var fqFileName = if (singleFile) (config.getTmpFolder + chunkNum + ".fq") else (config.getTmpFolder + chunkNum + "-1.fq")
	var fqFileName2: String = if (singleFile) null else (config.getTmpFolder + chunkNum + "-2.fq") 
	
	// Create fastq1.fq file
	hdfsManager.download(inputFileName, config.getInputFolder, config.getTmpFolder, false)
	val unzipStr = "gunzip -c " + config.getTmpFolder + inputFileName
	LogWriter.dbgLog(x, t0, "unzip\tunzipStr = " + unzipStr, config)
	unzipStr #> new java.io.File(fqFileName) !;
	new File(config.getTmpFolder + inputFileName).delete
	
	// Create fastq2.fq file if required
	if (inputFileName2 != null)
	{
		hdfsManager.download(inputFileName2, config.getInputFolder, config.getTmpFolder, false)
		val unzipStr = "gunzip -c " + config.getTmpFolder + inputFileName2
		LogWriter.dbgLog(x, t0, "unzip\tunzipStr = " + unzipStr, config)
		unzipStr #> new java.io.File(fqFileName2) !;
		new File(config.getTmpFolder + inputFileName2).delete
	}
	
	// bwa mem input_files_directory/fasta_file.fasta -p -t 2 x.fq > out_file
	val command_str = "./bwa mem " + (if (interleaved && !singleEnded) "-p " else "") + FilesManager.getRefFilePath(config) + " " + 
		config.getExtraBWAParams.replace("\t", "\\t") + " " + fqFileName + (if (fqFileName2 != null) (" " + fqFileName2) else "")
	val fqFileSize = new File(fqFileName).length
	val inputSize = if (singleFile) (fqFileSize) else (fqFileSize*2)
	
	LogWriter.dbgLog(x, t0, "1\tbwa mem started: " + command_str + ". Input file size = " + (inputSize/(1024*1024)) + " MB", config)
	//////////////////////////////////////////////////////////////////////////
	val pwMap = new scala.collection.mutable.HashMap[String, PrintWriter]
	val logger = ProcessLogger(
		//(o: String) => {bwaOutStr.append(o + '\n')}
		(o: String) => {appendSAM(o + '\n', chunkNum, config, hdfsManager, pwMap)}
		,
		(e: String) => {} // do nothing
	)
	command_str ! logger;
	//////////////////////////////////////////////////////////////////////////
	
	new File(fqFileName).delete
	if (fqFileName2 != null)
		new File(fqFileName2).delete
		
	for ((k,v) <- pwMap)
		v.close
	
	if (config.getMakeCombinedFile)
		hdfsManager.writeWholeFile(config.getOutputFolder + "ulStatus/" + chunkNum.toString, "")
	
	LogWriter.dbgLog(x, t0, "2\t" + "Content uploaded to the HDFS, r = " + r, config)
	return r
}

def appendSAM(line: String, chunkNum: Int, config: Configuration, hdfsManager: HDFSManager, pwMap: scala.collection.mutable.HashMap[String, PrintWriter]) 
{
	if (line(0) == '@')
	{
		if (!pwMap.contains("header"))
			pwMap.put("header", hdfsManager.open(config.getOutputFolder + "sam/" + chunkNum + "/header"))
		pwMap("header").write(line)
	}
	else
	{
		val fields = line.split('\t')
		var id: String = "0"
		val regionLen = config.getChrRegionLength.toInt
		val chrID = fields(2)
		
		if (regionLen != 0)
		{
			if (chrID == "*")
				id = "unmapped"
			else
			{
				if (regionLen == 1)
					id = chrID
				else
				{
					val pos = fields(3).toInt
					val region = pos / regionLen
					id = chrID + '-' + region
				}
			}
		}
		
		if (!pwMap.contains(id))
			pwMap.put(id, hdfsManager.open(config.getOutputFolder + "sam/" + chunkNum + "/" + id + ".sam"))
		pwMap(id).write(line)
	}
}

def combineChunks(config: Configuration)
{
	var chunkNum = 0
	var done = false 
	val hdfsManager = new HDFSManager
	val osMap = new scala.collection.mutable.HashMap[String, OutputStream]

	if (config.getCombinedFileIsLocal) 
	{
		new File(config.getCombinedFilesFolder).mkdirs
		new File(config.getCombinedFilesFolder + "status").mkdirs
	}				
	
	val readTimer = new SWTimer
	val writeTimer = new SWTimer
	while(!done)
	{
		while(!done && !hdfsManager.exists(config.getOutputFolder + "ulStatus/" + chunkNum))
		{
			if (hdfsManager.exists(config.getOutputFolder + "ulStatus/end.txt"))
			{
				if (!hdfsManager.exists(config.getOutputFolder + "ulStatus/" + chunkNum))
					done = true
			}
			Thread.sleep(1000)
		}
		if (!done)
		{
			val fileNames = FilesManager.getInputFileNames(config.getOutputFolder + "sam/" + chunkNum, config).filter(x => x.contains(".sam"))
			for (f <- fileNames)
			{
				if (!osMap.contains(f))
				{
					osMap.put(f, {
						if (config.getCombinedFileIsLocal) 
							new FileOutputStream(new File(config.getCombinedFilesFolder + f))
						else 
							hdfsManager.openStream(config.getCombinedFilesFolder + f)
					})
				}
				readTimer.start
				val readBytes = hdfsManager.readBytes(config.getOutputFolder + "sam/" + chunkNum + "/" + f)
				readTimer.stop
				
				writeTimer.start
				osMap(f).write(readBytes)
				writeTimer.stop
			}	
			
			hdfsManager.writeWholeFile(config.getCombinedFilesFolder + "status/" + chunkNum, "")
			chunkNum += 1
			println(s">> Read time = ${readTimer.getSecsF}, Write time = ${writeTimer.getSecsF}")
		}
	}
	
	for ((k,v) <- osMap)
		v.close
}

def main(args: Array[String]) 
{
	val config = new Configuration()
	config.initialize(args(0))
	val conf = new SparkConf().setAppName("StreamBWA")
	
	if (compressRDDs)
		conf.set("spark.rdd.compress","true")
	
	val sc = new SparkContext(conf)
	val bcConfig = sc.broadcast(config)
	val hdfsManager = new HDFSManager
	
	// Comment these two lines if you want to see more verbose messages from Spark
	//Logger.getLogger("org").setLevel(Level.OFF);
	//Logger.getLogger("akka").setLevel(Level.OFF);
	
	config.print() 
	val makeCombinedFile = config.getMakeCombinedFile
	var f: Future[Unit] = null
	
	if (!hdfsManager.exists("sparkLog.txt"))
		hdfsManager.create("sparkLog.txt")
	if (!hdfsManager.exists("errorLog.txt"))
		hdfsManager.create("errorLog.txt")
	
	var t0 = System.currentTimeMillis
	//////////////////////////////////////////////////////////////////////////
	val streaming = config.getStreaming.toBoolean
	if (makeCombinedFile)
		f = Future {combineChunks(config)}
	
	if (!streaming)
	{
		val inputFileNames = FilesManager.getInputFileNames(config.getInputFolder, config).filter(x => x.contains(".fq")) 
		if (inputFileNames == null)
		{
			println("The input directory " + config.getInputFolder() + " does not exist!")
			System.exit(1)
		}
		inputFileNames.foreach(println)
		
		// Give chunks to bwa instances
		val inputFileNumbers = inputFileNames.map(x => {val a = x.split('.'); val b = a(0).split('-'); b(0).toInt}).toSet.toArray
		scala.util.Sorting.quickSort(inputFileNumbers)
		val inputData = sc.parallelize(inputFileNumbers, inputFileNumbers.size) 
		inputData.foreach(x => bwaRun(x, bcConfig.value))
	}
	else
	{
		var done = false
		val parTasks = config.getGroupSize.toInt
		var si = 0
		var ei = parTasks
		while(!done)
		{
			var indexes = (si until ei).toArray  
			val inputData = sc.parallelize(indexes, indexes.size)
			val r = inputData.map(x => bwaRun(x, bcConfig.value))
			val finished = r.filter(_ == 1)
			if (finished.count > 0)
				done = true
			si += parTasks
			ei += parTasks
		}
	}
	//////////////////////////////////////////////////////////////////////
	var et = (System.currentTimeMillis - t0) / 1000
	LogWriter.statusLog("Execution time of streambwa:", t0, et.toString() + "\tsecs", config)
	if (makeCombinedFile)
	{
		hdfsManager.writeWholeFile(config.getOutputFolder + "ulStatus/end.txt", "")
		Await.result(f, Duration.Inf)
	}
	et = (System.currentTimeMillis - t0) / 1000
	LogWriter.statusLog("Total execution time:", t0, et.toString() + "\tsecs", config)
}
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition
