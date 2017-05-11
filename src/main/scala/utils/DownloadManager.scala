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
package utils

import tudelft.utils._
import java.io._
import sys.process._

object DownloadManager
{
	private def gunZipDownloadedFile(x: String, filePath: String, config: Configuration) : Long =
	{
		val fileName = FilesManager.getFileNameFromPath(filePath)
		val hdfsManager = new HDFSManager
		val fileSize = hdfsManager.getFileSize(filePath)
		
		LogWriter.dbgLog(x, 0, "gunzip\tfilePath = " + filePath + ", fileSize = " + fileSize, config)
		try{("gunzip " + config.getSfFolder + fileName + ".gz").!}
		catch{case e: Exception => LogWriter.dbgLog(x, 0, "gunzip\tEither already unzipped or some other thread is unzipping it!", config)}
		val f = new File(config.getTmpFolder + fileName)
		@volatile var flen = f.length
		
		var iter = 0
		while(flen != fileSize)
		{
			if ((iter % 10) == 0)
				LogWriter.dbgLog(x, 0, "gunzip\t(flen != fileSize) -> flen = " + flen + ", fileSize = " + fileSize, config)
			iter += 1
			Thread.sleep(1000)
			flen = f.length
		}
		LogWriter.dbgLog(x, 0, "gunzip\t(flen == fileSize) -> flen = " + flen + ", fileSize = " + fileSize, config)
		
		return flen
	}

	private def fileToDownloadAlreadyExists(hdfsPath: String, config: Configuration) : Boolean =
	{
		val fileName = FilesManager.getFileNameFromPath(hdfsPath)
		val hdfsManager = new HDFSManager
		val fileSize = hdfsManager.getFileSize(hdfsPath)
		val f = new File(config.getSfFolder + fileName)
		
		return f.exists && (f.length == fileSize)
	}

	def downloadBWAFiles(x: String, config: Configuration) : Int = 
	{
		val refFolder = FilesManager.getDirFromPath(config.getRefPath)
		val refFileName = FilesManager.getFileNameFromPath(config.getRefPath)
		val hdfsManager = new HDFSManager
		var r = 0
		
		if (!(new File(config.getSfFolder).exists))
			new File(config.getSfFolder()).mkdirs()
		
		// If there is a zipped version of this file, use that to reduce downloaded data
		if (hdfsManager.exists(config.getRefPath + ".gz"))
		{
			if (!fileToDownloadAlreadyExists(config.getRefPath, config))
			{
				LogWriter.dbgLog(x, 0, "download\tDownloading zipped ref file", config)
			
				r = hdfsManager.downloadIfRequired(refFileName + ".gz", refFolder, config.getSfFolder)
				if (r != 0)
				{
					LogWriter.dbgLog(x, 0, "download\tError reading " + refFileName + ".gz from the HDFS folder " + refFolder + ". Error code = " + r, config)
					return 1
				}
				gunZipDownloadedFile(x, config.getRefPath, config)
			}
		}
		else
		{
			LogWriter.dbgLog(x, 0, "download\tDownloading unzipped ref file", config)
			if (hdfsManager.downloadIfRequired(refFileName, refFolder, config.getSfFolder) != 0)
			{
				LogWriter.dbgLog(x, 0, "download\tError reading " + refFileName + " from the HDFS folder " + refFolder, config)
				return 1
			}
		}
		LogWriter.dbgLog(x, 0, "download\tDownloading dict, amb and ann files", config)
		if (hdfsManager.downloadIfRequired(refFileName.replace(".fasta", ".dict"), refFolder, config.getSfFolder) != 0)
		{
			LogWriter.dbgLog(x, 0, "download\tError reading " + refFileName.replace(".fasta", ".dict") + " from the HDFS folder " + refFolder, config)
			return 1
		}
		if (hdfsManager.downloadIfRequired(refFileName + ".amb", refFolder, config.getSfFolder) != 0)
		{
			LogWriter.dbgLog(x, 0, "download\tError reading " + refFileName + ".amb from the HDFS folder " + refFolder, config)
			return 1
		}
		if (hdfsManager.downloadIfRequired(refFileName + ".ann", refFolder, config.getSfFolder) != 0)
		{
			LogWriter.dbgLog(x, 0, "download\tError reading " + refFileName + ".ann from the HDFS folder " + refFolder, config)
			return 1
		}
		// If there is a zipped version of this file, use that to reduce downloaded data
		if (hdfsManager.exists(config.getRefPath + ".bwt.gz"))
		{
			if (!fileToDownloadAlreadyExists(config.getRefPath + ".bwt", config))
			{
				LogWriter.dbgLog(x, 0, "download\tDownloading zipped ref bwt file", config)
			
				r = hdfsManager.downloadIfRequired(refFileName + ".bwt.gz", refFolder, config.getSfFolder);
				if (r != 0)
				{
					LogWriter.dbgLog(x, 0, "download\tError reading " + refFileName + ".gz from the HDFS folder " + refFolder + ". Error code = " + r, config)
					return 1
				}
				gunZipDownloadedFile(x, config.getRefPath + ".bwt", config)
			}
		}
		else
		{
			LogWriter.dbgLog(x, 0, "download\tDownloading unzipped ref bwt file", config)
			if (hdfsManager.downloadIfRequired(refFileName + ".bwt", refFolder, config.getSfFolder) != 0)
			{
				LogWriter.dbgLog(x, 0, "download\tError reading " + refFileName + ".bwt from the HDFS folder " + refFolder, config)
				return 1
			}
		}
		LogWriter.dbgLog(x, 0, "download\tDownloading fai, pac and sa files", config)
		if (hdfsManager.downloadIfRequired(refFileName + ".fai", refFolder, config.getSfFolder) != 0)
		{
			LogWriter.dbgLog(x, 0, "download\tError reading " + refFileName + ".fai from the HDFS folder " + refFolder, config)
			return 1
		}
		if (hdfsManager.downloadIfRequired(refFileName + ".pac", refFolder, config.getSfFolder) != 0)
		{
			LogWriter.dbgLog(x, 0, "download\tError reading " + refFileName + ".pac from the HDFS folder " + refFolder, config)
			return 1
		}
		if (hdfsManager.downloadIfRequired(refFileName + ".sa", refFolder, config.getSfFolder) != 0)
		{
			LogWriter.dbgLog(x, 0, "download\tError reading " + refFileName + ".sa from the HDFS folder " + refFolder, config)
			return 1
		}
		
		return 0
	}

	def downloadDictFile(config: Configuration)
	{
		val refFolder = FilesManager.getDirFromPath(config.getRefPath())
		val refFileName = FilesManager.getFileNameFromPath(config.getRefPath())
		val hdfsManager = new HDFSManager
		
		hdfsManager.downloadIfRequired(refFileName.replace(".fasta", ".dict"), refFolder, config.getSfFolder)
	}

	def downloadBinProgram(fileName: String, config: Configuration)
	{	
		val hdfsManager = new HDFSManager
		
		hdfsManager.downloadIfRequired(fileName, config.getToolsFolder(), config.getTmpFolder)
	}
}
