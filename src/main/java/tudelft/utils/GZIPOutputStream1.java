/****************************************/
//	Class Name:	GZIPOutputStream1	
//	Author:		Hamid Mushtaq  		
//	Company:	TU Delft	 	
/****************************************/
package tudelft.utils;

import java.util.zip.GZIPOutputStream;
import java.io.*;

public class GZIPOutputStream1 extends GZIPOutputStream 
{
	private ByteArrayOutputStream outputStream;
	private int compressLevel;
	private static final int DEFAULT_COMPRESS_LEVEL = 1;
	
	public GZIPOutputStream1(ByteArrayOutputStream out, int compressLevel) throws IOException 
	{
        super(out, true);
		def.setLevel(compressLevel);
		outputStream = out;
		this.compressLevel = compressLevel;
    } 
	
	public GZIPOutputStream1(ByteArrayOutputStream out) throws IOException 
	{
        super(out, true);
		def.setLevel(DEFAULT_COMPRESS_LEVEL);
		outputStream = out;
		this.compressLevel = DEFAULT_COMPRESS_LEVEL;
    }
	
	public int getCompressLevel()
	{
		return compressLevel;
	}
	
	public int getSize()
	{
		return outputStream.size();
	}
	
	public ByteArrayOutputStream getOutputStream()
	{
		return outputStream;
	}
	
	public byte[] getByteArray()
	{
		return outputStream.toByteArray();
	}
}
