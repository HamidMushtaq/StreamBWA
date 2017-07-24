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

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import scala.Tuple2;
import java.util.ArrayList;
import java.io.*;

/**
 *
 * @author Hamid Mushtaq
 */
public class SamRecsIterator 
{
	protected SAMRecord sam = null;
	protected int reads = 0;
	protected SAMFileHeader header;
	protected SAMRecord[] samRecords;
	protected int index;
	protected int endIndex;

	public SamRecsIterator(SAMRecord[] samRecords, SAMFileHeader header, int size)
	{
		this.samRecords = samRecords;
		this.header = header;
		index = 0;
		endIndex = size;
		getFirstRecord();
	}
    
	public boolean hasNext()
	{
		return (index < endIndex)? true : false;
	}
	
	public SAMRecord getNext()
	{
		return samRecords[index++];
	}
	
	private void getFirstRecord() 
	{
		sam = null;
		if(hasNext()) 
		{
			sam = getNext();
			sam.setHeader(header);
			reads++;
		}
	}

	public SAMRecord next() 
	{
		SAMRecord tmp = sam;
		if (hasNext()) 
		{
			sam = getNext();
			sam.setHeader(header);
			reads++;
		} 
		else 
			sam = null;
		return tmp;
	}
	
	public int getCount() 
	{
		return reads;
	}
}
