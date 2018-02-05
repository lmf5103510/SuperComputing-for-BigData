/* VarDensity.scala */
/* Author: Hamid Mushtaq */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageLevel._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Calendar
import java.text._
import java.io._

object VarDensity 
{
	final val compressRDDs = true
	val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("sparkLog.txt"), "UTF-8"))

	def getTimeStamp() : String =
	{
		return new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime())
	}

	/**
	Given a number, the function returns a list of it. Exmaple: 3 -> {1,2,3}
	**/
	def createList(value:Long) :List[Int] = 
	{
		var value_list = List[Int]()
		for(i <- 1 to value.toInt) {
			value_list = i :: value_list
		}
		return value_list.reverse
	}

	def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
		val p = new java.io.PrintWriter(f)
		try { op(p) } finally { p.close() }
	}

	def main(args: Array[String]) 
	{
		val tasks = args(0)
		val dbsnpFile = args(1)
		val dictFile = args(2)
		
		println(s"Tasks = $tasks\ndbsnpFile = $dbsnpFile\ndictFile = $dictFile\n")

		val conf = new SparkConf().setAppName("Variant Density Calculator App")
		conf.setMaster("local[" + tasks + "]")
		conf.set("spark.cores.max", tasks)
		if (compressRDDs)
			conf.set("spark.rdd.compress", "true")
		val sc = new SparkContext(conf)

		// add spark listener for our code.
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
						bw.write(getTimeStamp() + row.name + " processed!\n");
					bw.flush
				})
			}
		});
		
		val t0 = System.currentTimeMillis
		// Add your main code here

		// Reading a dictionary
		val dict_input = sc.textFile(dictFile)
		// (chromosom_name, length, ...,...,)
		val lines = dict_input.map(line => line.split("\t")).filter(col => !(col(1).contains("_")))
		// (chromosom, length)
		val chromosom_length_rdd = lines.map(ts => if(ts.size > 3) (ts(1).replace("SN:",""), ts(2).replace("LN:",""))).filter(_. != ())
		// (chromosom, (index, length))
		val chromosom_length_rdd_index = chromosom_length_rdd.zipWithIndex.map{case((chr,length),index) => (chr,(index,length))}
		// (chromosom, (index, entries))
		val chromosom_length_rdd_entries = chromosom_length_rdd_index.map(i => (i._1, (i._2._1, (i._2._2).toString.toLong/1000000 + 1)))
		// (chromosom, (index, region))
		var creating_table = chromosom_length_rdd_entries.map(i => (i._1, (i._2._1, createList(i._2._2)))).flatMap{case (x, (index, list)) => list.map(k => (x, (index, k)))}
		// ((chromosom,region), index)
		// eg: ((chr1, 5), 1)
		// eg: ((chrX, 2), 23)
		var chromosom_region = creating_table.map(i=> ((i._1.toString, i._2._2.toLong), i._2._1.toString.toInt))

		// Reading the variant database
		val dbsnp_input = sc.textFile(dbsnpFile)
		// // // // filter lines with hashtags
		val dbsnp_filtered = dbsnp_input.filter(_.head != '#')
		val dbsnp_lines = dbsnp_filtered.map(line => line.split("\t"))

		// get the position of the well-known variant in the chromosome
		// ((chromosom, region),1)  prepared for later reducebykey function
		// eg: ((chrX, 2), 1)
		val chromosom_name_pos = dbsnp_lines.map(i => ((i(0), (i(1).toLong/1000000) + 1),1))//.filter(pos => pos._2.toInt > 3000000)
		// obtain the occurance of the well-known variants in each region
		// ((chromosom,region),occurrence)
		// eg: ((chr1, 5), 10534)
		val variant_occ = chromosom_name_pos.reduceByKey(_ + _)
		// obtain the result by join chromosom_region and variant_occ
		// (chromosom,index, region, occurrence)
		val joined_ = chromosom_region.join(variant_occ).map(i => (i._1._1, i._2._1, i._1._2, i._2._2))

		// Sort by the index of the chromosome
		val sorted_list = joined_.sortBy(_._2).collect()

		// Saving
		val writter = new BufferedWriter(new FileWriter("vardensity.txt"))
		for(i <- sorted_list) {
			writter.write(i + "\n")
		}
		writter.close()
		
		val et = (System.currentTimeMillis - t0) / 1000
		println("{Time taken = %d mins %d secs}".format(et / 60, et % 60))

		sc.stop()
		bw.close()
	} 
}
