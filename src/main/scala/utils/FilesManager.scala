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
import java.nio.file.{Paths, Files}
import sys.process._
import org.apache.commons.lang3.exception.ExceptionUtils

object FilesManager
{
	def getFileNameFromPath(path: String) : String =
	{
		return path.substring(path.lastIndexOf('/') + 1)
	}

	def getDirFromPath(path: String) : String =
	{
		return path.substring(0, path.lastIndexOf('/') + 1)
	}

	def getRefFilePath(config: Configuration) : String = 
	{
		return config.getSfFolder() + getFileNameFromPath(config.getRefPath())
	}

	def getToolsDirPath(config: Configuration) : String = 
	{
		return config.getSfFolder()
	}

	def getBinToolsDirPath(config: Configuration) : String = 
	{
		return config.getTmpFolder()
	}

	def readWholeFile(fname: String, config: Configuration) : String =
	{
		val hdfsManager = new HDFSManager
		
		return hdfsManager.readWholeFile(fname)
	}
	
	def readWholeLocalFile(fname: String) : String =
	{
		return new String(Files.readAllBytes(Paths.get(fname)))
	}

	def readPartialFile(fname: String, bytes: Int, config: Configuration) : String =
	{
		val hdfsManager = new HDFSManager
		
		return hdfsManager.readPartialFile(fname, bytes)
	}

	def writeWholeFile(fname: String, s: String, config: Configuration)
	{
		val hdfsManager = new HDFSManager
		
		hdfsManager.writeWholeFile(fname, s)
	}
	
	def getInputFileNames(dir: String, config: Configuration) : Array[String] = 
	{
		val hdfsManager = new HDFSManager
		
		if (!hdfsManager.exists(dir))
			return new Array[String](0)
		
		val a: Array[String] = hdfsManager.getFileList(dir)

		return a
	}
	
	def uploadFileToOutput(filePath: String, outputPath: String, delSrc: Boolean, config: Configuration)
	{
		try 
		{
			val fileName = getFileNameFromPath(filePath)
			new File(config.getTmpFolder + "." + fileName + ".crc").delete()
			// Now upload
			val hconfig = new org.apache.hadoop.conf.Configuration()
			val fs = org.apache.hadoop.fs.FileSystem.get(hconfig)
			fs.copyFromLocalFile(delSrc, true, new org.apache.hadoop.fs.Path(config.getTmpFolder + fileName), 
				new org.apache.hadoop.fs.Path(config.getOutputFolder + outputPath + "/" + fileName))		
		}
		catch 
		{
			case e: Exception => LogWriter.errLog(outputPath, 0, 
				"\tException in uploadFileToOutput: " + ExceptionUtils.getStackTrace(e) , config) 
		}
	}
}
