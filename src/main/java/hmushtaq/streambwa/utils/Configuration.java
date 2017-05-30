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

import htsjdk.samtools.*;
import java.io.File;
import java.nio.file.*;
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
	private String numExecutors;
	private String numTasks;
	private String groupSize;
	private Long startTime;
	private String execMemGB;
	private String singleEnded;
	private String interleaved;
	private String driverMemGB;
	private String streaming;
	private String downloadRef;
	private String combinedFilesFolder;
	private String combinerThreads;
	private HashSet<String> ignoreListSet;
	private boolean combinedFileIsLocal;
	private boolean makeCombinedFile;
	private boolean inClientMode;
	private String writeHeaderSep;
	private Long chrRegionLength;
	
	public void initialize(String configFilePath, String deployMode)
	{	
		try
		{
			inClientMode = deployMode.equals("client");
			String configFile = configFilePath;
			if (!inClientMode)
				configFile = getFileNameFromPath(configFile);
			
			File file = new File(configFile);
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			Document document = documentBuilder.parse(file);
			
			refPath = document.getElementsByTagName("refPath").item(0).getTextContent().trim();
			inputFolder = correctFolderName(document.getElementsByTagName("inputFolder").item(0).getTextContent().trim());
			outputFolder = correctFolderName(document.getElementsByTagName("outputFolder").item(0).getTextContent().trim());
			toolsFolder = correctFolderName(document.getElementsByTagName("toolsFolder").item(0).getTextContent().trim());
			extraBWAParams = document.getElementsByTagName("extraBWAParams").item(0).getTextContent().trim();
			tmpFolder = correctFolderName(document.getElementsByTagName("tmpFolder").item(0).getTextContent().trim());
			sfFolder = correctFolderName(document.getElementsByTagName("sfFolder").item(0).getTextContent().trim());
			numExecutors = document.getElementsByTagName("numExecutors").item(0).getTextContent().trim();
			numTasks = document.getElementsByTagName("numTasks").item(0).getTextContent().trim();
			groupSize = document.getElementsByTagName("groupSize").item(0).getTextContent().trim();
			execMemGB = document.getElementsByTagName("execMemGB").item(0).getTextContent().trim();
			singleEnded = document.getElementsByTagName("singleEnded").item(0).getTextContent().trim();
			interleaved = document.getElementsByTagName("interleaved").item(0).getTextContent().trim();
			driverMemGB = document.getElementsByTagName("driverMemGB").item(0).getTextContent().trim();
			streaming = document.getElementsByTagName("streaming").item(0).getTextContent().trim();
			downloadRef = document.getElementsByTagName("downloadRef").item(0).getTextContent().trim();
			combinedFilesFolder = correctFolderName(document.getElementsByTagName("combinedFilesFolder").item(0).getTextContent().trim());
			combinerThreads = document.getElementsByTagName("combinerThreads").item(0).getTextContent().trim();
			String ignoreList = document.getElementsByTagName("ignoreList").item(0).getTextContent().trim();
			if (!inClientMode)
				ignoreList = getFileNameFromPath(ignoreList);
			ignoreListSet = new HashSet<String>();
			if (!ignoreList.equals(""))
			{
				List<String> lines = Files.readAllLines(Paths.get(ignoreList), java.nio.charset.StandardCharsets.UTF_8);
				for (String s: lines)
				{
					String x = s.trim();
					if (!x.equals(""))
						ignoreListSet.add(x);
				}
			}
			combinedFileIsLocal = combinedFilesFolder.startsWith("local:");
			if (combinedFileIsLocal)
				combinedFilesFolder = combinedFilesFolder.substring(6);
			writeHeaderSep = document.getElementsByTagName("writeHeaderSeparately").item(0).getTextContent().trim();	
			makeCombinedFile = !combinedFilesFolder.equals("");
			String chrRegionLengthStr = document.getElementsByTagName("chrRegionLength").item(0).getTextContent().trim();
			float chrRegionLengthF = Float.valueOf(chrRegionLengthStr);
			chrRegionLength = (long)chrRegionLengthF;
		
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
		if (path.contains("/"))
			return path.substring(path.lastIndexOf('/') + 1);
		else
			return path;
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
	
	public String getNumExecutors()
	{
		return numExecutors;
	}
	
	public String getNumTasks()
	{
		return numTasks;
	}
	
	public String getGroupSize()
	{
		return groupSize;
	}
	
	public void setNumExecutors(String numExecutors)
	{
		this.numExecutors = numExecutors;
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
	
	public String getCombinedFilesFolder()
	{
		return combinedFilesFolder;
	}
	
	public String getCombinerThreads()
	{
		return combinerThreads;
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
	
	public long getChrRegionLength()
	{
		return chrRegionLength;
	}
	
	public boolean isInIgnoreList(String s)
	{
		return ignoreListSet.contains(s);
	}
	
	public boolean ignoreListIsEmpty()
	{
		return ignoreListSet.isEmpty();
	}
		
	public void print()
	{
		System.out.println("***** Configuration *****");
		System.out.println("inClientMode:\t|" + inClientMode + "|");
		System.out.println("refPath:\t|" + refPath  + "|");
		System.out.println("inputFolder:\t|" + inputFolder + "|");
		System.out.println("outputFolder:\t|" + outputFolder + "|");
		System.out.println("tmpFolder:\t|" + tmpFolder + "|");
		System.out.println("sfFolder:\t|" + sfFolder + "|");
		System.out.println("numExecutors:\t|" + numExecutors + "|");
		System.out.println("numTasks:\t|" + numTasks + "|");
		System.out.println("groupSize:\t|" + groupSize + "|");
		System.out.println("execMemGB:\t|" + execMemGB + "|");
		System.out.println("driverMemGB:\t|" + driverMemGB + "|");
		System.out.println("singleEnded:\t|" + singleEnded + "|");
		System.out.println("interleaved:\t|" + interleaved + "|");
		System.out.println("streaming:\t|" + streaming + "|");
		System.out.println("downloadRef:\t|" + downloadRef + "|");
		System.out.println("combinedFilesFolder:\t|" + combinedFilesFolder + "|");
		System.out.println("combinerThreads:\t|" + combinerThreads + "|");
		System.out.println("combinedFileIsLocal:\t|" + combinedFileIsLocal + "|");
		System.out.println("writeHeaderSep:\t|" + writeHeaderSep + "|");
		System.out.println("makeCombinedFile:\t|" + makeCombinedFile + "|");
		System.out.println("chrRegionLength:\t|" + chrRegionLength + "|");
		System.out.println("ignoreListIsEmpty:\t|" + ignoreListIsEmpty() + "|");
		System.out.println("ignoreList:");
		for (String s: ignoreListSet)
			System.out.println("<" + s + ">");
		System.out.println("*************************");
	}
}