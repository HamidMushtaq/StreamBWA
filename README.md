# README #

A paper describing the implementation of this program was accepted for the BIBE 2017 conference. So, if you use StreamBWA for your work, please cite the following paper.
>H. Mushtaq, N. Ahmed, Z. Al-Ars, Streaming Distributed DNA Sequence Alignment Using Apache Spark, 17th annual IEEE International Conference on BioInformatics and BioEngineering (BIBE 2017), 23-25 October 2017, Washington DC, USA.

This README contains the following sections.
1. **System requirements and Installation**
2. **Compiling**
3. **Files required**
4. **Configuration files**
	* **Configuration file for StreamBWA**
	* **Configuration file for the chunker utility**
5. **Running SparkGA**

## System requirements and Installation
For StreamBWA, you must have the following software packages installed in your system/cluster. Note that SparkGA runs on a yarn cluster, so before running, the cluster should have been properly setup. Moreover, your system/cluster must be Linux-based.

* Java 1.7 or greater
* Hadoop 2.4 or greater
* Apache Spark 2.0.0 or greater

Note that your Spark version should match the hadoop version. For example, if you have hadoop version 2.5.1, you must use spark-2.x.x-bin-hadoop2.4. Moreover you must have an evironment variable ```$SPARK_HOME``` that points to the Spark directory. You must also add the following to ```$SPARK_HOME/conf/spark-env.sh```.

> HADOOP_CONF_DIR=$HADOOP_INSTALL/etc/hadoop

This of course assumes that you have an environment variable $HADOOP_INSTALL that points to the hadoop's home directory.

Note that if you are running on the Microsoft Azure Cloud, everything is already setup properly if you create a cluster using HDInsight.

## Compiling
For compiling, go to the program folder and type `sbt package` (Use sbt version 0.13.x, as we haven't tested with sbt version 1.x). The code for the chunker utility can be found at https://github.com/HamidMushtaq/FastqChunker, but a compiled jar file is also placed in the chunker folder here.

## Files required
All the reference files that you require for running BWA are required here as well, but you need to copy them to each data node of the cluster. Make sure though, that these files are copied to a folder with common name on all the nodes. You specify the path of that node in the config file (explained later). For example, you can make a folder /tmp/spark on each node, and copy the reference files there.

The *.dict file must be copied to the folder from where you are running the program. Moreover, you need to also create a folder, where you place the BWA executable file. This folder name is also mentioned in the config file.

## Configuration files

### Configuration file for StreamBWA
Several example configuration files are given in the `config `folder. Here is what each tag means.

1. **mode** - The mode of running. Its value could be `yarn-client` or `yarn-cluster`.
2. **refPath** - The path of the FASTA file. All the accompanying files, such as \*.fasta.bwt, \*.dict and \*.fasta.pac etc. should be in the same folder as the FASTA file. The names of those files would be infered from the FASTA file. If, you have already downloaded these files to each node, then only giving the name of the fasta file would suffice. Otherwise, you could set **downloadRef** value to true, to tell StreamBWA to download the files for you. However, this downloading functionality has not been fully tested yet, so its recommended to download the files yourself.
3. **inputFolder** - The folder that contains the input chunks. In other words, this is the folder where the chunker utility puts the chunks.
4. **outputFolder** - The output folder. SAM file corresponding to a chunk is saved in \<outputFolder\>/sam/\<chunkNum\> in gzipped form, by the name of content.sam.gz. The folder where the combined SAM file(s) is produced is given by the tag `combinedFilesFolder`.
5. **tmpFolder** - This folder is the one used for storing temporary files. This folder would be present on each data node.
6. **sfFolder** - This is the folder, where all the reference and index files are stored. This folder would be present on each data node. 
7. **toolsFolder** - In this folder, you put the BWA program. This folder would be in a local directory. The run script, would send these executables to each executor, using `--files` when executing `spark-submit`.
8. **extraBWAParams** - The extra parameters to BWA, such as number of threads and read group string. See example config files to know more.
9. **singleEnded** - Is the input FASTQ file single ended or pair ended.
10. **interleaved** - Are the input FASTQ chunks made by the chunker utility, interleaved or not.
11. **execMemGB** - The executor memory in GB.
12. **numExecutors** - The number of of executors. 
13. **numTasks** - The number of tasks per each executor. 
14. **groupSize** - Put this value to a little more than the number of chunks you expect from the chunker utility. The program would work even with a lesser value, but would be a little slower.
15. **streaming** - Set this to **true** when running alongside the chunker, and **false** if the chunks already exist.
16. **downloadRef** - If you want the program to itself download the reference files from HDFS. This is still experimental though, so its recommended to keep this **false** and download the files yourself.
17. **ignoreList** - Comma seperated names of chromosomes that have to be ignored.
18. **combinedFilesFolder** - Folder where the combined output SAM file(s) would be produced.
19. **combinerThreads** - Number of threads used by the combiner.
20. **writeHeaderSeparately** - Do you want the header included in the SAM file(s) or have it in a separate file.
21. **chrRegionLength** - Leave this to 0, if you want one combined SAM output file. 1 if you want a SAM file for each chromosome. Otherwise, give the region length, for SAM files of specific region size.

### Configuration file for the chunker utility
The chunker program is run on the master node, which means it is not distributed like StreamBWA. It can take both compressed (gzipped) or uncompressed FASTQ files as input. By default, it uploads the chunks it makes to an HDFS directory specified as `outputFolder` in the configuration file. However, if you prefix the value of `outputFolder` with `local:`, it would put the chunks in a local folder. For example, if you specify `local:/home/hamidmushtaq/data/chunks`, it would put the chunks in the folder `/home/hamidmushtaq/data/chunks`. Moreover, you don't have to create the output folder yourself, as the chunker program would create one itself. Example configuration files for the chunker are provided in the config/chunker folder.

1. **fastq1Path** - The path or URL of the first FASTQ file for a pair-ended input or the only FASTQ file for a single-ended input. Note that the chunker utility would automatically infer if the input file is compressed or not by seeing the `.gz` extension.
2. **fastq2Path** - The path or URL of the second FASTQ file for a pair-ended input. For a single-ended input, leave this field empty (Like this -> `<fastq2Path></fastq2Path>`). You can't ommit it though.
3. **outputFolder** - The output folder path where the chunks would be placed. Prefix with `local:` if you want this folder to be in the local directory.
4. **compLevel** - The chunks made are compressed by the level specified in this field. Value can be from 1 to 9.
5. **chunkSizeMB** - The size of the chunks in MB.
6. **blockSizeMB** - The input FASTQ files are read block by block. In this field, you specify how big such block should be in MBs. The bigger the block size, the more memory would be consumed. However, bigger block size can also mean better performance.
7. **driverMemGB** - The memory used by the chunker utility in GBs.
8. **numThreads** - The number of threads performing chunking. Try to use as many threads as possible as allowed by your system.
9. **interleave** - `true` or `false` depending on whether you want to interleave the contents of the two FASTQ files for a paired-ended input into single chunks.

## Running StreamBWA
Run it using the `runWithChunker.py` script. That script would run the chunker utility in parallel with the main program. Before running, make sure that the chunker utility's jar file (chunker_2.11-1.0.jar) is present. Moreover, the lib folder containing the htsjdk-1.143.jar should also be present. Furthermore, the reference *.dict file must also be there. Then run the program as follows.

`python runWithChunker.py <path to the streambwa jar file> <path to your config file> <path to your chunker config file>`
___

For any further help or suggestions, contact me at hmushtaq1980@yahoo.com.
