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
package tudelft.utils;

import htsjdk.samtools.*;
import java.io.File;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import java.io.Serializable;
import java.lang.System;
import java.util.*;

public class Configuration implements Serializable
{
	private String refPath;
	private String inputFolder;
	private String outputFolder;
	private String toolsFolder;
	private String extraBWAParams;
	private String tmpFolder;
	private String sfFolder;
	private String numInstances;
	private String numTasks;
	private String groupSize;
	private Long startTime;
	private String execMemGB;
	private String singleEnded;
	private String interleaved;
	private String driverMemGB;
	private String streaming;
	private String downloadRef;
	private String combinedFilePath;
	private boolean makeCombinedFile;
	private boolean combinedFileIsLocal;
	private String writeHeaderSep;
	
	public void initialize(String configFile)
	{	
		try
		{
			File file = new File(configFile);
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			Document document = documentBuilder.parse(file);
			
			refPath = document.getElementsByTagName("refPath").item(0).getTextContent();
			inputFolder = correctFolderName(document.getElementsByTagName("inputFolder").item(0).getTextContent());
			outputFolder = correctFolderName(document.getElementsByTagName("outputFolder").item(0).getTextContent());
			toolsFolder = correctFolderName(document.getElementsByTagName("toolsFolder").item(0).getTextContent());
			extraBWAParams = document.getElementsByTagName("extraBWAParams").item(0).getTextContent();
			tmpFolder = correctFolderName(document.getElementsByTagName("tmpFolder").item(0).getTextContent());
			sfFolder = correctFolderName(document.getElementsByTagName("sfFolder").item(0).getTextContent());
			numInstances = document.getElementsByTagName("numInstances").item(0).getTextContent();
			numTasks = document.getElementsByTagName("numTasks").item(0).getTextContent();
			groupSize = document.getElementsByTagName("groupSize").item(0).getTextContent();
			execMemGB = document.getElementsByTagName("execMemGB").item(0).getTextContent();
			singleEnded = document.getElementsByTagName("singleEnded").item(0).getTextContent();
			interleaved = document.getElementsByTagName("interleaved").item(0).getTextContent();
			driverMemGB = document.getElementsByTagName("driverMemGB").item(0).getTextContent();
			streaming = document.getElementsByTagName("streaming").item(0).getTextContent();
			downloadRef = document.getElementsByTagName("downloadRef").item(0).getTextContent();
			combinedFilePath = document.getElementsByTagName("combinedFilePath").item(0).getTextContent();
			combinedFileIsLocal = combinedFilePath.startsWith("local:");
			if (combinedFileIsLocal)
				combinedFilePath = combinedFilePath.substring(6);
			writeHeaderSep = document.getElementsByTagName("writeHeaderSeparately").item(0).getTextContent();	
			makeCombinedFile = !combinedFilePath.trim().equals("");
		
			startTime = System.currentTimeMillis();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private String correctFolderName(String s)
	{
		String r = s.trim();
		
		if (r.equals(""))
			return r;
		
		if (r.charAt(r.length() - 1) != '/')
			return r + '/';
		else
			return r;
	}
	
	private String getFileNameFromPath(String path)
	{
		return path.substring(path.lastIndexOf('/') + 1);
	}
	
	public String getRefPath()
	{
		return refPath;
	}
	
	public String getInputFolder()
	{
		return inputFolder;
	}
	
	public String getOutputFolder()
	{
		return outputFolder;
	}
	
	public String getToolsFolder()
	{
		return toolsFolder;
	}

	public String getExtraBWAParams()
	{
		return extraBWAParams;
	}
	
	public String getTmpFolder()
	{
		return tmpFolder;
	}
	
	public String getSfFolder()
	{
		return sfFolder;
	}
	
	public String getNumInstances()
	{
		return numInstances;
	}
	
	public String getNumTasks()
	{
		return numTasks;
	}
	
	public String getGroupSize()
	{
		return groupSize;
	}
	
	public void setNumInstances(String numInstances)
	{
		this.numInstances = numInstances;
	}
	
	public Long getStartTime()
	{
		return startTime;
	}
	
	public String getDriverMemGB()
	{
		return driverMemGB + "g";
	}
	
	public String getExecMemGB()
	{
		return execMemGB + "g";
	}
	
	public String getSingleEnded()
	{
		return singleEnded;
	}
	
	public String getInterleaved()
	{
		return interleaved;
	}
	
	public String getStreaming()
	{
		return streaming;
	}
	
	public String getDownloadRef()
	{
		return downloadRef;
	}
	
	public String getCombinedFilePath()
	{
		return combinedFilePath;
	}
	
	public boolean getMakeCombinedFile()
	{
		return makeCombinedFile;
	}
	
	public boolean getCombinedFileIsLocal()
	{
		return combinedFileIsLocal;
	}
	
	public String getWriteHeaderSep()
	{
		return writeHeaderSep;
	}
	
	public void print()
	{
		System.out.println("***** Configuration *****");
		System.out.println("refPath:\t" + refPath);
		System.out.println("inputFolder:\t" + inputFolder);
		System.out.println("outputFolder:\t" + outputFolder);
		System.out.println("tmpFolder:\t" + tmpFolder);
		System.out.println("numInstances:\t" + numInstances);
		System.out.println("numTasks:\t" + numTasks);
		System.out.println("groupSize:\t" + groupSize);
		System.out.println("execMemGB:\t" + execMemGB);
		System.out.println("driverMemGB:\t" + driverMemGB);
		System.out.println("streaming:\t" + streaming);
		System.out.println("downloadRef:\t" + downloadRef);
		System.out.println("*************************");
	}
}