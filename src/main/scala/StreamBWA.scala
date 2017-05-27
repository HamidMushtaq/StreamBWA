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
	val regMap = new scala.collection.mutable.HashMap[String, StringBuilder]
	val pwHeader = hdfsManager.open(config.getOutputFolder + "sam/" + chunkNum + "/header")
	val logger = ProcessLogger(
		//(o: String) => {bwaOutStr.append(o + '\n')}
		(o: String) => 
		{
			if (o(0) == '@')
				pwHeader.write(o + '\n')
			else
				appendSAM(o + '\n', chunkNum, config, hdfsManager, regMap)
		}
		,
		(e: String) => {} // do nothing
	)
	command_str ! logger;
	pwHeader.close
	//////////////////////////////////////////////////////////////////////////
	
	new File(fqFileName).delete
	if (fqFileName2 != null)
		new File(fqFileName2).delete
	
	LogWriter.dbgLog(x, t0, "2\t" + "BWA finished. Now compressing and uploading the data", config)
	val infoSb = new StringBuilder
	val content = new StringBuilder(327680000)
	var si = 0
	for ((id,sb) <- regMap)
	{
		content.append(sb)
		infoSb.append(id + '\t' + si + '\t' + sb.size + '\n')
		si += sb.size
	}
	val compressedContent = new GzipCompressor(content.toString).compress
	hdfsManager.writeBytes(config.getOutputFolder + "sam/" + chunkNum + "/content.sam.gz", compressedContent)
	hdfsManager.writeWholeFile(config.getOutputFolder + "sam/" + chunkNum + "/info", infoSb.toString)
	hdfsManager.writeWholeFile(config.getOutputFolder + "ulStatus/" + chunkNum.toString, "")
	
	LogWriter.dbgLog(x, t0, "3\t" + "Content uploaded to the HDFS, r = " + r, config)
	return r
}

def appendSAM(line: String, chunkNum: Int, config: Configuration, hdfsManager: HDFSManager, 
	regMap: scala.collection.mutable.HashMap[String, StringBuilder]) 
{
	val fields = line.split('\t')
	var id: String = "0"
	val regionLen = config.getChrRegionLength
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
				val pos = fields(3).toLong
				val region = pos / regionLen
				id = chrID + '-' + region
			}
		}
	}
	
	if (!regMap.contains(id))
		regMap.put(id, new StringBuilder)
	regMap(id).append(line)
}

def getTimeStamp() : String =
{
	return new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime())	
}

def combineChunks(config: Configuration)
{
	var chunkNum = 0
	var done = false 
	val hdfsManager = new HDFSManager
	val osMap = new scala.collection.mutable.HashMap[String, OutputStream] 
	val numThreads = config.getCombinerThreads.toInt
	var ulStatusDone = false
	var ulChunksSet = Set[Int]()
	var doneSet = Set[Int]()
	var diffSet = Set[Int]()
	
	if (config.getCombinedFileIsLocal) 
	{
		new File(config.getCombinedFilesFolder).mkdirs
		new File(config.getCombinedFilesFolder + "status").mkdirs
	}				
	
	while(!(diffSet.isEmpty && ulStatusDone))
	{
		if (!ulStatusDone)
		{
			val inputFileNames = FilesManager.getInputFileNames(config.getOutputFolder + "ulStatus", config)
			if (inputFileNames.contains("end.txt"))
				ulStatusDone = true
			ulChunksSet = scala.collection.mutable.Set(inputFileNames.filter(_ != "end.txt").map(_.toInt):_*)
			println(s"[${getTimeStamp()}] ulChunksSet.size = ${ulChunksSet.size}")
		}
		diffSet = ulChunksSet.diff(doneSet)
		println(s"[${getTimeStamp()}] doneSet.size = ${doneSet.size}, diffSet.size = ${diffSet.size}")
		if (diffSet.isEmpty && !ulStatusDone)
			Thread.sleep(500)
		else
		{
			val diffSetArray = diffSet.toArray
			val timer = new SWTimer
			timer.start
			for (di <- 0 until diffSetArray.size by numThreads)
			{
				val futures = new Array[Future[Unit]](numThreads)
				var nFutures = 0
				for (fi <- 0 until numThreads
					if ((di + fi) < diffSetArray.size))
				{
					val chunkNum = diffSetArray(di + fi)
					futures(fi) = Future
					{
						val info = hdfsManager.readWholeFile(config.getOutputFolder + "sam/" + chunkNum + "/info").split('\n')
						val baContent = hdfsManager.readBytes(config.getOutputFolder + "sam/" + chunkNum + "/content.sam.gz")
						val ba = new GzipDecompressor(baContent).decompressToBytes
						
						doneSet.synchronized
						{
							for (i <- info)
							{
								val ia = i.split('\t')
								val fid = ia(0)
								val si = ia(1).toInt
								val len = ia(2).toInt
								
								if (!osMap.contains(fid))
								{
									osMap.put(fid, {
										if (config.getCombinedFileIsLocal) 
											new FileOutputStream(new File(config.getCombinedFilesFolder + fid + ".sam"))
										else 
											hdfsManager.openStream(config.getCombinedFilesFolder + fid + ".sam")
									})
								}
								
								osMap(fid).write(ba, si, len)
							}	
							hdfsManager.writeWholeFile(config.getCombinedFilesFolder + "status/" + chunkNum, "")
							doneSet += chunkNum
						}
					}
					nFutures += 1
				} // End of for loop with numThreads number of iterations
				for(i <- 0 until nFutures)
					Await.result(futures(i), Duration.Inf)
			} // End of outer for loop
			timer.stop
			println(s"--> [${getTimeStamp()}] diffSetArray.size = ${diffSetArray.size}, time = ${timer.getSecsF} secs")
		} // End of the else that processes chunks using futures.
	} // End of outer while loop
	
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
