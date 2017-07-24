/*
 * Copyright (C) 2017 Hamid Mushtaq, TU Delft
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
package hmushtaq.streambwa.utils;

import java.io.File
import java.io.InputStream
import java.io.FileInputStream
import java.util._
import htsjdk.samtools.util.BufferedLineReader
import htsjdk.samtools._

/**
 *
 * @author Hamid Mushtaq
 */
class SamRecsReader(is: InputStream, config: Configuration)
{
	private val samRecs = scala.collection.mutable.ArrayBuffer.empty[SAMRecord]
	private val mConfig = config
	private var mReads = 0
	private val validationStringency: ValidationStringency = ValidationStringency.LENIENT;
	private val mReader = new BufferedLineReader(is);
	private val samRecordFactory = new DefaultSAMRecordFactory();
	private var mCurrentLine: String = null
	
	def getSamRecs() : Array[SAMRecord] = 
	{
		return samRecs.toArray
	}
	
	def writeSAMRecord(sam: SAMRecord) : Integer = 
	{
		var count = 0
		val read1Ref = sam.getReferenceIndex()
		
		if (!sam.getReadUnmappedFlag() && (read1Ref >= 0))
		{
			samRecs.append(sam)
			count = count + 1;
		}
		
		return count
	}
			
	def advanceLine() : String = 
	{
		mCurrentLine = mReader.readLine()
		return mCurrentLine;
	}
	
	def parseSam() : Integer =  
	{
		var mParentReader: SAMFileReader = null
		val headerCodec = new SAMTextHeaderCodec();
		headerCodec.setValidationStringency(validationStringency)
		val mFileHeader = headerCodec.decode(mReader, null)
		val parser = new SAMLineParser(samRecordFactory, validationStringency, mFileHeader, null, null)
		// now process each read...
		var count = 0
		var badLines = 0
		
		mCurrentLine = mReader.readLine()
		
		if (mCurrentLine == null)
			println("Hamid >> mCurrentLine is null!")
		
		while (mCurrentLine != null) 
		{
			try
			{
				val samrecord = parser.parseLine(mCurrentLine, mReader.getLineNumber)
				count += writeSAMRecord(samrecord)
			}
			catch
			{
				case e: Exception => badLines += 1
			}
			//advance line even if bad line
			advanceLine();
		}
        
		mReads = count
		//println("SAMstream counts " + count + " records");
	
		return badLines
	}
	
	def getNumOfReads() : Integer =
	{
		return mReads
	}
	
	def close() =
	{
		mReader.close()
		is.close()
	}
}
