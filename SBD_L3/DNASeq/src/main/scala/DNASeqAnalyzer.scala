	/* 
	* Copyright (c) 2015-2016 TU Delft, The Netherlands.
	* All rights reserved.
	* 
	* You can redistribute this file and/or modify it under the
	* terms of the GNU Lesser General Public License as published by the
	* Free Software Foundation, either version 3 of the License, or
	* (at your option) any later version.
	*
	* This file is distributed in the hope that it will be useful,
	* but WITHOUT ANY WARRANTY; without even the implied warranty of
	* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	* GNU Lesser General Public License for more details.
	*
	* You should have received a copy of the GNU Lesser General Public License
	* along with this program.  If not, see <http://www.gnu.org/licenses/>.
	* 
	* Authors: Hamid Mushtaq
	*
	*/
	import org.apache.spark.SparkContext
	import org.apache.spark.SparkContext._
	import org.apache.spark.SparkConf
	import org.apache.log4j.Logger
	import org.apache.log4j.Level
	import org.apache.spark.broadcast.Broadcast

	import sys.process._
	import org.apache.spark.scheduler._

	import java.io._
	import java.nio.file.{Paths, Files}
	import java.net._
	import java.util.Calendar

	import scala.sys.process.Process
	import scala.io.Source
	import scala.collection.JavaConversions._
	import scala.util.Sorting._

	import tudelft.utils.ChromosomeRange
	import tudelft.utils.DictParser
	import tudelft.utils.Configuration
	import tudelft.utils.SAMRecordIterator

	import java.text.DateFormat
	import java.text.SimpleDateFormat
	import java.util.Calendar

	import org.apache.commons.lang3.exception.ExceptionUtils
	import org.apache.spark.storage.StorageLevel._
	import org.apache.spark.HashPartitioner

	import collection.mutable.HashMap
	import collection.mutable.ArrayBuffer

	import htsjdk.samtools._

	object DNASeqAnalyzer {
		final val MemString 	= "-Xmx2048m"
		final val RefFileName	= "ucsc.hg19.fasta"
		final val SnpFileName 	= "dbsnp_138.hg19.vcf"
		final val ExomeFileName = "gcat_set_025.bed"
		final val stderrFolder 	= "stderr"
		final val stdoutFolder 	= "stdout"
		val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("sparkListener.txt"), "UTF-8"))
		//////////////////////////////////////////////////////////////////////////////
		// the input for this function ia changed to sam file instead of chunk file after modification		
		def bwaRun(x: String): Array[(Int, SAMRecord)] =
		{
			val bwaKeyValues = new BWAKeyValues(x)
			bwaKeyValues.parseSam()
			val kvPairs: Array[(Int, SAMRecord)] = bwaKeyValues.getKeyValuePairs()

			return kvPairs
		}

		def writeToBAM(fileName: String, samRecordsSorted: Array[SAMRecord], bcconfig: Broadcast[Configuration]): ChromosomeRange =
		{
			val config = bcconfig.value
			val header = new SAMFileHeader()
			header.setSequenceDictionary(config.getDict())
			val outHeader = header.clone()
			outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);
			val factory = new SAMFileWriterFactory();
			val writer = factory.makeBAMWriter(outHeader, true, new File(fileName));

			val r = new ChromosomeRange()
			val input = new SAMRecordIterator(samRecordsSorted, header, r)
			while (input.hasNext()) {
				val sam = input.next()
				writer.addAlignment(sam);
			}
			writer.close();

			return r
		}

		def compareSAMRecords(a: SAMRecord, b: SAMRecord) : Int = 
		{
			if(a.getReferenceIndex == b.getReferenceIndex)
				return a.getAlignmentStart - b.getAlignmentStart
			else
				return a.getReferenceIndex - b.getReferenceIndex
		}

		// this function creat folders of log info for executing command
		def runCommand(cmd: Seq[String], command_name:String, region_num:Int){
			val stdoutStream = new ByteArrayOutputStream
			val stderrStream = new ByteArrayOutputStream
			val stdoutWriter = new PrintWriter(stdoutStream)
			val stderrWriter = new PrintWriter(stderrStream)
			val exitValue = cmd.!(ProcessLogger(stdoutWriter.println, stderrWriter.println))
			stdoutWriter.close()
			stderrWriter.close()
			new File(stderrFolder + "/" + command_name).mkdirs
			new File(stdoutFolder + "/" + command_name).mkdirs
			val writer_stderr = new BufferedWriter(new FileWriter(stderrFolder + "/" + command_name + "/" + region_num + ".txt"))
			val writer_stdout = new BufferedWriter(new FileWriter(stdoutFolder + "/" + command_name + "/" + region_num + ".txt" ))
			// Note: the output generated from jar is interpreted as std err
			writer_stderr.write(stdoutWriter.toString)
			writer_stderr.close()
			writer_stdout.write(stderrStream.toString)
			writer_stdout.close()
		}

		def variantCall(chrRegion: Int, samRecords: Array[SAMRecord], bcconfig: Broadcast[Configuration]): Array[(Int, (Int, String))] =
		{
			val config = bcconfig.value
			val tmpFolder = config.getTmpFolder
			val toolsFolder = config.getToolsFolder
			val refFolder = config.getRefFolder
			val numOfThreads = config.getNumThreads

			// Following is shown how each tool is called. Replace the X in regionX with the chromosome region number (chrRegion). 
			// 	You would have to create the command strings (for running jar files) and then execute them using the Scala's process package. More 
			// 	help about Scala's process package can be found at http://www.scala-lang.org/api/current/index.html#scala.sys.process.package.
			//	Note that MemString here is -Xmx14336m, and already defined as a constant variable above, and so are reference files' names.

			// Create stream and output directories if they don't already exist
			new File(tmpFolder).mkdirs

			val p1 = tmpFolder + s"/region$chrRegion-p1.bam"
			val p2 = tmpFolder + s"/region$chrRegion-p2.bam"
			val p3 = tmpFolder + s"/region$chrRegion-p3.bam"
			val p3_metrics = tmpFolder + s"/region$chrRegion-p3-metrics.txt"
			val regionFile = tmpFolder + s"/region$chrRegion.bam"

			println("Running toBAM")
			// SAM records should be sorted by this point
			val chrRange = writeToBAM(p1, samRecords, bcconfig)

			println(chrRange)

			println("Running cmd...")
			// Picard preprocessing
			//	java MemString -jar toolsFolder/CleanSam.jar INPUT=tmpFolder/regionX-p1.bam OUTPUT=tmpFolder/regionX-p2.bam
			var command = Seq("java", MemString, "-jar", toolsFolder + "CleanSam.jar", "INPUT=" + p1, "OUTPUT=" + p2)
			println(command)
			runCommand(command, "clean_sam", chrRegion)
			//	java MemString -jar toolsFolder/MarkDuplicates.jar INPUT=tmpFolder/regionX-p2.bam OUTPUT=tmpFolder/regionX-p3.bam 
			//		METRICS_FILE=tmpFolder/regionX-p3-metrics.txt
			command = Seq("java", MemString, "-jar", toolsFolder + "MarkDuplicates.jar", "INPUT=" + p2, "OUTPUT=" + p3, "METRICS_FILE=" + p3_metrics)
			println(command)
			runCommand(command, "mark_dup", chrRegion)
			//	java MemString -jar toolsFolder/AddOrReplaceReadGroups.jar INPUT=tmpFolder/regionX-p3.bam OUTPUT=tmpFolder/regionX.bam 
			//		RGID=GROUP1 RGLB=LIB1 RGPL=ILLUMINA RGPU=UNIT1 RGSM=SAMPLE1
			command = Seq("java", MemString, "-jar", toolsFolder + "AddOrReplaceReadGroups.jar", "INPUT=" + p3, "OUTPUT=" + regionFile, "RGID=GROUP1", "RGLB=LIB1", "RGPL=ILLUMINA", "RGPU=UNIT1", "RGSM=SAMPLE1")
			println(command)
			runCommand(command, "add_replace_groups", chrRegion)
			// 	java MemString -jar toolsFolder/BuildBamIndex.jar INPUT=tmpFolder/regionX.bam
			command = Seq("java", MemString, "-jar", toolsFolder + "BuildBamIndex.jar", "INPUT=" + regionFile)
			println(command)
			runCommand(command, "build_bam_indx", chrRegion)
			//	delete tmpFolder/regionX-p1.bam, tmpFolder/regionX-p2.bam, tmpFolder/regionX-p3.bam and tmpFolder/regionX-p3-metrics.txt
			Seq("rm", p1, p2, p3, p3_metrics).lines

			// Make region file 
			val tmpBedFile = tmpFolder + s"tmp$chrRegion.bed"
			val bedFile = tmpFolder + s"bed$chrRegion.bed"
			//	val tmpBed = new File(tmpFolder/tmpX.bed)
			val tmpBed = new File(tmpBedFile)
			//	chrRange.writeToBedRegionFile(tmpBed.getAbsolutePath())
			chrRange.writeToBedRegionFile(tmpBed.getAbsolutePath())
			//	toolsFolder/bedtools intersect -a refFolder/ExomeFileName -b tmpFolder/tmpX.bed -header > tmpFolder/bedX.bed
			(Seq(toolsFolder + "bedtools", "intersect", "-a", refFolder + ExomeFileName, "-b", tmpBedFile, "-header") #> new File(bedFile)).lines
			//	delete tmpFolder/tmpX.bed
			Seq("rm", tmpBedFile).lines

			// Indel Realignment 
			val intervalFile = tmpFolder + s"region$chrRegion.intervals"
			val region2File = tmpFolder + s"region$chrRegion-2.bam"
			val baiFile = tmpFolder + s"region$chrRegion.bai"
			//	java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T RealignerTargetCreator -nt numOfThreads -R refFolder/RefFileName 
			//		-I tmpFolder/regionX.bam -o tmpFolder/regionX.intervals -L tmpFolder/bedX.bed
			command = Seq("java", MemString, "-jar", toolsFolder + "GenomeAnalysisTK.jar", "-T", "RealignerTargetCreator", "-nt", numOfThreads, "-R", refFolder + RefFileName, "-I", regionFile, "-o", intervalFile, "-L", bedFile)
			println(command)
			runCommand(command, "realigner_trgt_creator", chrRegion)
			//	java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T IndelRealigner -R refFolder/RefFileName -I tmpFolder/regionX.bam 
			//		-targetIntervals tmpFolder/regionX.intervals -o tmpFolder/regionX-2.bam -L tmpFolder/bedX.bed
			command = Seq("java", MemString, "-jar", toolsFolder + "GenomeAnalysisTK.jar", "-T", "IndelRealigner", "-R", refFolder + RefFileName, "-I", regionFile, "-targetIntervals", intervalFile, "-o", region2File, "-L", bedFile)
			println(command)
			runCommand(command, "indel_realigner", chrRegion)
			//	delete tmpFolder/regionX.bam, tmpFolder/regionX.bai, tmpFolder/regionX.intervals
			Seq("rm", intervalFile, baiFile).lines //, intervalFile).lines
			//

			// Base quality recalibration 
			val regionTableFile = tmpFolder + s"region$chrRegion.table"
			val region3File = tmpFolder + s"region$chrRegion-3.bam"
			val bai2File = tmpFolder + s"region$chrRegion-2.bai"
			//	java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T BaseRecalibrator -nct numOfThreads -R refFolder/RefFileName -I 
			//		tmpFolder/regionX-2.bam -o tmpFolder/regionX.table -L tmpFolder/bedX.bed --disable_auto_index_creation_and_locking_when_reading_rods 
			//		-knownSites refFolder/SnpFileName
			command = Seq("java", MemString, "-jar", toolsFolder + "GenomeAnalysisTK.jar", "-T", "BaseRecalibrator", "-nct", numOfThreads, "-R", refFolder + RefFileName, "-I", region2File, "-o", regionTableFile, "-L", bedFile, "--disable_auto_index_creation_and_locking_when_reading_rods", "-knownSites", refFolder + SnpFileName)
			println(command)
			runCommand(command, "base_recalib", chrRegion)
			//
			//	java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T PrintReads -R refFolder/RefFileName -I 
			//		tmpFolder/regionX-2.bam -o tmpFolder/regionX-3.bam -BSQR tmpFolder/regionX.table -L tmpFolder/bedX.bed 
			command = Seq("java", MemString, "-jar", toolsFolder + "GenomeAnalysisTK.jar", "-T", "PrintReads", "-R", refFolder + RefFileName, "-I", region2File, "-o", region3File, "-BQSR", regionTableFile, "-L", bedFile)
			println(command)
			runCommand(command, "prnt_reads", chrRegion)
			// delete tmpFolder/regionX-2.bam, tmpFolder/regionX-2.bai, tmpFolder/regionX.table
			Seq("rm", region2File, bai2File, regionTableFile).lines

			// Haplotype -> Uses the region bed file
			val vcfFile = tmpFolder + s"region$chrRegion.vcf"
			val bai3File = tmpFolder + s"region$chrRegion-3.bai"
			// java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T HaplotypeCaller -nct numOfThreads -R refFolder/RefFileName -I 
			//		tmpFolder/regionX-3.bam -o tmpFolder/regionX.vcf  -stand_call_conf 30.0 -stand_emit_conf 30.0 -L tmpFolder/bedX.bed 
			//		--no_cmdline_in_header --disable_auto_index_creation_and_locking_when_reading_rods
			command = Seq("java", MemString, "-jar", toolsFolder + "GenomeAnalysisTK.jar", "-T", "HaplotypeCaller", "-nct", numOfThreads, "-R", refFolder + RefFileName, "-I", region3File, "-o", vcfFile, "-stand_call_conf", "30.0", "-stand_emit_conf", "30.0", "-L", bedFile, "--no_cmdline_in_header", "--disable_auto_index_creation_and_locking_when_reading_rods")
			println(command)
			runCommand(command, "haplo_caller", chrRegion)
			// delete tmpFolder/regionX-3.bam, tmpFolder/regionX-3.bai, tmpFolder/bedX.bed

			command = Seq("rm", region3File, bai3File, bedFile)
			println(command)
			runCommand(command, "rm_regionFile", chrRegion)

			var results = ArrayBuffer[(Int, (Int, String))]()
			val resultFile = Source.fromFile(vcfFile)
			for (line <- resultFile.getLines()) {
				if (!line.startsWith("#")) {
					val tabs = line.split("\t")
					var chrom = 0
					if (tabs(0) == "chrX") {
						chrom = 23
					} else {
						chrom = (tabs(0).filter(_.isDigit)).toInt
					}
					val pos = tabs(1).toInt
					results += ((chrom, (pos, line)))
				}
			}
			println("steady")
			results.toArray
		}

		// this function creat the load map for later application
		def loadBalancer(weights: Array[((Int, Int), (Long, Long, Long))], numTasks: Int): ArrayBuffer[ArrayBuffer[(Int, (Int, Int, Int))]] = 
		{
			var results = ArrayBuffer.fill(numTasks)(ArrayBuffer[(Int, (Int, Int, Int))]())
			var sizes = (1 to numTasks).toArray.map(i => (i,0))
			var chro_num = 100
			for (i <- 0 until weights.length) {
				if (weights(i)._1._1 != chro_num){
					chro_num = weights(i)._1._1
					// according to the current workload of each worker-region, sort the arraybuffer.
					sizes = sizes.sortWith(_._2 < _._2)
					// geting all the worker-regions that will be assigned for a specified chromosome
					var chro_seq = ArrayBuffer.fill(weights(i)._2._1.toInt)(0)
					for(k <- 0 until weights(i)._2._1.toInt){
						chro_seq(k) = sizes(k)._1
					}
					// make sure the worker-region is in the ascending order
					// for example chr15 has 5 block(20 chr-region per block), assigned with worker region (4,6,15,16,32), because this 5 worker-region are the most relaxed ones currently
					chro_seq = chro_seq.sortWith(_ < _)
					for(k <- 0 until weights(i)._2._1.toInt){
						results(chro_seq(k)-1).append((weights(i)._1._1, (k, weights(i)._2._1.toInt, weights(i)._2._3.toInt)))
					}
				}
				// updating the workload for the chosen worker-regions
				// because the size arraybuffer has been sorted, the first n regions are the chosen worker-regions
				sizes(weights(i)._1._2-1) = (sizes(weights(i)._1._2-1)._1, sizes(weights(i)._1._2-1)._2 + weights(i)._2._2.toInt)
			}
			results
		}

		def getTimeStamp() : String =
		{
			return "[" + new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime()) + "] "
		}

		// Given a number, the function returns a list of it. Exmaple: 3 -> {1,2,3}
		def createList(value:Long) :List[Int] = 
		{
			var value_list = List[Int]()
			for(i <- 1 to value.toInt) {
				value_list = i :: value_list
			}
			return value_list.reverse
		}

		def main(args: Array[String]) 
		{
			val config = new Configuration()
			config.initialize()

			val numInstances = Integer.parseInt(config.getNumInstances)
			val inputFolder = config.getInputFolder
			val outputFolder = config.getOutputFolder
			val numRegions = 64

			var mode = "local"

			val conf = new SparkConf().setAppName("DNASeqAnalyzer")
			// For local mode, include the following two lines
			if (mode == "local") {
				conf.setMaster("local[" + config.getNumInstances() + "]")
				conf.set("spark.cores.max", config.getNumInstances())
			}
			if (mode == "cluster") {
				// For cluster mode, include the following commented line
				conf.set("spark.shuffle.blockTransferService", "nio")
			}
			//conf.set("spark.rdd.compress", "true")

			new File(outputFolder).mkdirs
			new File(outputFolder + "output.vcf")
			new File(stderrFolder).mkdirs
			new File(stdoutFolder).mkdirs
			val sc = new SparkContext(conf)
			val bcconfig = sc.broadcast(config)

			// Comment these two lines if you want to see more verbose messages from Spark
			Logger.getLogger("org").setLevel(Level.OFF);
			Logger.getLogger("akka").setLevel(Level.OFF);

			sc.addSparkListener(new SparkListener() 
			{
				override def onApplicationStart(applicationStart: SparkListenerApplicationStart) 
				{
					bw.write(getTimeStamp() + " Spark ApplicationStart: " + applicationStart.appName + "\n");
					bw.flush
				}

				override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) 
				{
					bw.write(getTimeStamp() + " Spark ApplicationEnd: " + applicationEnd.time + "\n");
					bw.flush
				}

				override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) 
				{
					val map = stageCompleted.stageInfo.rddInfos
					map.foreach(row => {
						if (row.isCached)
						{
							bw.write(getTimeStamp() + row.name + ": memsize = " + (row.memSize / 1000000) + "MB, rdd diskSize " + 
								row.diskSize + ", numPartitions = " + row.numPartitions + "-" + row.numCachedPartitions + "\n");
						}
						else if (row.name.contains("rdd_"))
						{
							bw.write(getTimeStamp() + row.name + " processed!\n");
						}
						bw.flush
					})
				}
			});

			var t0 = System.currentTimeMillis

			val files = sc.parallelize(new File(inputFolder).listFiles, numInstances)
			files.cache
			println("inputFolder = " + inputFolder + ", list of files = ")
			files.collect.foreach(x => println(x))

			var bwaResults = files.flatMap(files => bwaRun(files.getPath))
			.combineByKey(
				(sam: SAMRecord) => Array(sam),
				(acc: Array[SAMRecord], value: SAMRecord) => (acc :+ value),
				(acc1: Array[SAMRecord], acc2: Array[SAMRecord]) => (acc1 ++ acc2)
			).persist(MEMORY_ONLY_SER)//cache
			bwaResults.setName("rdd_bwaResults")

			// =============================================================================================================================================================
			// most of the modifications of the code are below here.
			// =============================================================================================================================================================

			// sort the Array[SAMRecord] such that each chromosome has an array of sorted SAMRecord
			bwaResults = bwaResults.map{ case (key, values) => (key, values.sortWith{case(first, second) => compareSAMRecords(first, second) < 0}) }
			// obtain the number of SAMRecord for each chromosome
			// (index, distance)
			// chromosome 2 has 105031 SAMRecords thus (2, 105031)
			var loadPerChromosome = bwaResults.map { case (key, values) => (key, values.length) }

			// Reading a dictionary
			val dict_input = sc.textFile(config.getRefFolder+"/ucsc.hg19.dict")
			// (chromosom_name, length, ...,...,)
			val lines = dict_input.map(line => line.split("\t")).filter(col => !(col(1).contains("_")))
			// (chromosom, length)
			val chromosom_length_rdd = lines.map(ts => if(ts.size > 3) (ts(1).replace("SN:",""), ts(2).replace("LN:",""))).filter(_. != ())
			// (index, length)
			val chromosom_length_rdd_index = chromosom_length_rdd.zipWithIndex.map{case((chr,length),index) => (index.toInt, length)}
			// (index, entries_num)
			// eg: chr1 has 250 entries: (1, 250)
			val chromosom_length_rdd_entries = chromosom_length_rdd_index.map(i => (i._1, (i._2).toString.toLong/1000000 + 1))

			// join the distance and entries number to calculate for each entry how many reads need to be done for each chromosome region
			// (index, (distance, entries_total_num)) eg: (1, (113517, 250))
			val chromosom_distance = loadPerChromosome.join(chromosom_length_rdd_entries)
			// (index, (distance/entry, block_num, entries))
			// (1, (452, 98))
			val index_length_region = chromosom_distance.map(i => (i._1, (i._2._1/i._2._2, (i._2._2+19)/20, createList(i._2._2)))).flatMap{case (index, (dis_ave, block, list)) => list.map(k => (index, (dis_ave, block, k)))}
			// for the later loadbalancing, we need to spilt the chromosome into blocks(each 20 chromosome regions is a block)
			// ((index, entries) , (block_num, distance/entry))
			// ((1, 198),(10, 452)) stands for:   the 198th region in chr1 contains 452 reads and in the 10th blocks of chr1
			val index_entry = index_length_region.map(x => ((x._1, x._2._3), (x._2._2, x._2._1)))

			// reading the variant density file
			val vardensity_input = sc.textFile("vardensity.txt")
			// ((index, entries) , variant_number)
			val vardensity_index = vardensity_input.map(x => x.split(",")).map(col => ((col(1).toInt, col(2).toInt), col(3).replace(")","").toLong))
			// combine the number of SAMRecords and the number variants
			// ((index, block_entry) , (total_block_num, distance/entry, variant_number, distance/entry))
			// ((1, 10),(13, 452, 19253))
			val index_distance_var = index_entry.join(vardensity_index).map(x => ((x._1._1, (x._1._2+19)/20),(x._2._1._1, x._2._1._2, x._2._2, x._2._1._2)))
			// calculate the workload, the formula for the workload is :
			// workload == #reads + #variants/100, #reads is the number of SAMRecords/reads, #variants is the number of well-known variants
			// ((index, block_entry) , (block_num, workload, distance/entry))
			val index_block = index_distance_var.reduceByKey((p1, p2) => (p1._1, p1._2 + p2._2, p1._3 + p2._3, p1._4)).map(x => ((x._1._1,x._1._2),(x._2._1,x._2._2 + x._2._3/100,x._2._4))).sortByKey()

			// assign the work load into 64 worker-region according to the workload.
			// the line return a loadMap telling us, for a instance, block 2 of chr3 is assigned to worker-region 34.
			// There are 64 worker-regions in total.
			val loadMap = loadBalancer(index_block.collect(), numRegions)

			// prepare the balanced SAMRecords Array
			var loadBalanced_arr = ArrayBuffer[(Int, Array[SAMRecord])]()
			// get the array type of bwaResults for later use
			val bwa_array = bwaResults.collect().sortWith(_._1 < _._1)
			
			// this part is the actual assigning part for load balancing according to the loadMap
			for(i <- 0 until numRegions){
				val load_region = loadMap(i)
				for(k <- 0 until load_region.length){
					// (Int, (Int, Int, Int))
					val load_block = load_region(k)
					val sam_list = bwa_array(load_block._1)._2
					if (k == 0) {
						if (load_block._2._1 < load_block._2._3-1) {
							// appending the corresponding part of the SAMRecords Array into the worker-regions
							loadBalanced_arr.append((i+1, sam_list.slice(load_block._2._1*load_block._2._3*20, (load_block._2._1+1)*load_block._2._3*20)))
						} else {
							loadBalanced_arr.append((i+1, sam_list.slice(load_block._2._1*load_block._2._3*20, sam_list.length)))
						}
					}
					else {
						if (load_block._2._1 < load_block._2._3-1) {
							// keep appending the SAMRecords Array for each worker-region
							loadBalanced_arr(i) = (i+1, loadBalanced_arr(i)._2 ++ sam_list.slice(load_block._2._1*load_block._2._3*20, (load_block._2._1+1)*load_block._2._3*20))
						} else {
							loadBalanced_arr(i) = (i+1, loadBalanced_arr(i)._2 ++ sam_list.slice(load_block._2._1*load_block._2._3*20, sam_list.length))
						}
					}
				} 
			}
			// get the RDD type of balanced load
			val loadBalancedRDD = sc.parallelize(loadBalanced_arr)

			// =============================================================================================================================================================
			// the rest part of code is the same as before.
			// =============================================================================================================================================================

			val variantCallData = loadBalancedRDD
			.flatMap { case (key: Int, sams: Array[SAMRecord]) => variantCall(key, sams, bcconfig) }
			variantCallData.setName("rdd_variantCallData")

			val results = variantCallData.combineByKey(
				(value: (Int, String)) => Array(value),
				(acc: Array[(Int, String)], value: (Int, String)) => (acc :+ value),
				(acc1: Array[(Int, String)], acc2: Array[(Int, String)]) => (acc1 ++ acc2)
			).cache
			//results.setName("rdd_results")

			val fl = new PrintWriter(new File(outputFolder + "output.vcf"))
			for (i <- 1 to 24) {
				println("Writing chrom: " + i.toString)
				val fileDump = results.filter { case (chrom, value) => chrom == i }
				.flatMap { case (chrom: Int, value: Array[(Int, String)]) => value }
				.sortByKey(true)
				.map { case (position: Int, line: String) => line }
				.collect
				for (line <- fileDump.toIterator) {
					fl.println(line)
				}
			}
			fl.close()

			sc.stop()
			bw.close()

			val et = (System.currentTimeMillis - t0) / 1000 
			println(getTimeStamp() + "Execution time: %d mins %d secs".format(et/60, et%60))
		}
		//////////////////////////////////////////////////////////////////////////////
	} // End of Class definition
