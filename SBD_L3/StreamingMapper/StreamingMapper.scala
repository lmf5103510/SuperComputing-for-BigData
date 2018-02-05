	import org.apache.log4j.{Level, Logger}
	import org.apache.spark._
	import org.apache.spark.SparkContext._
	import org.apache.spark.streaming._
	import org.apache.spark.streaming.StreamingContext._
	import java.io._
	import java.io.FileWriter
	import java.io.BufferedWriter
	import scala.collection.mutable.ArrayBuffer
	import java.util.Locale
	import java.text._
	import scala.util.Random
	import scala.concurrent.{Await, Future}
	import scala.concurrent.duration._
	import scala.concurrent.ExecutionContext.Implicits.global
	import java.net._
	import java.util.Calendar
	import javax.xml.parsers.DocumentBuilderFactory
	import javax.xml.parsers.DocumentBuilder
	import org.w3c.dom.Document
	import sys.process._
	import scala.sys.process.Process
	import scala.util.control.Breaks._

	object StreamingMapper
	{
		var refPath = ""
		var bwaPath = ""
		var numThreads = 0
		var streamDir = ""
		var sam_num = 0
		var outputDir = ""

		def getTimeStamp() : String =
		{
			return new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime())
		}

		def getTagValue(document: Document, tag: String) : String =
		{
			document.getElementsByTagName(tag).item(0).getTextContent
		}

		// This function read the debugString and get the finename that is writen to the stream folder
		// Then execute the command to get sam output file according to each chunk file.
		def getfilename(debugString : String) = 
		{
			val line = debugString.split("streamDir/")
			val line_size = line.size
			val i = 1
			if (line_size > 1){
				for( i <- 1 until line_size){				
					val filename = line(i).slice(0, 10)
					// execute the command to obtain the sam file.
					val dirContents = (bwaPath  + " mem " + refPath + " -p" + " -t " + numThreads.toString + " ./" +streamDir + "/" + filename).!!
					var sam_writer = new BufferedWriter(new FileWriter(outputDir + "/output" + filename.slice(6, 7) + ".sam"))
					sam_writer.write(dirContents)
					sam_writer.close()
					// raise the flag when reach the final sam file.
					if (filename.slice(6, 7) == "8"){
						sam_num = 1
					}
				}
			}
		}

		def main(args: Array[String])
		{
			Logger.getLogger("org").setLevel(Level.OFF)
			Logger.getLogger("akka").setLevel(Level.OFF)
			Logger.getRootLogger.setLevel(Level.OFF)

			val sparkConf = new SparkConf().setAppName("WordCount")

			// Read the parameters from the config file //////////////////////////
			val file = new File("config.xml")
			val documentBuilderFactory = DocumentBuilderFactory.newInstance
			val documentBuilder = documentBuilderFactory.newDocumentBuilder
			val document = documentBuilder.parse(file)

			refPath = getTagValue(document, "refPath")
			bwaPath = getTagValue(document, "bwaPath")
			val numTasks = getTagValue(document, "numTasks")
			numThreads = getTagValue(document, "numThreads").toInt
			val intervalSecs = getTagValue(document, "intervalSecs").toInt
			streamDir = getTagValue(document, "streamDir")
			val inputDir = getTagValue(document, "inputDir")
			outputDir = getTagValue(document, "outputDir")

			println(s"refPath = $refPath\nbwaPath = $bwaPath\nnumTasks = $numTasks\nnumThreads = $numThreads\nintervalSecs = $intervalSecs")
			println(s"streamDir = $streamDir\ninputDir = $inputDir\noutputDir = $outputDir")

			// Create stream and output directories if they don't already exist
			new File(streamDir).mkdirs
			new File(outputDir).mkdirs
			//////////////////////////////////////////////////////////////////////

			sparkConf.setMaster("local[" + numTasks + "]")
			sparkConf.set("spark.cores.max", numTasks)
			val ssc = new StreamingContext(sparkConf, Seconds(intervalSecs))

			// Add your code here.
			// Use the function textFileStream of StreamingContext to read data as the files are added to the streamDir directory.

			// creat the thread to monitor the stream folder, once there is a new file function getfilename() will be executed then.
			val stream_monitor = new Thread(new Runnable {
				def run() {
					val moni = ssc.textFileStream(streamDir)
					moni.foreachRDD(data => getfilename(data.toDebugString))
					ssc.start()
				}
			})
			// actually start the stream-directory monitor thread.
			stream_monitor.start
			// This sleep timeout make sure that the file writing to stream folder happens after monitor thread starts
			Thread.sleep(1000)

			// Code below writes chunk file to streamDir
			// read the input fastq file
			val bufferedReader_fis1 = new BufferedReader(new FileReader(inputDir+"/fastq1.fq"))
			val bufferedReader_fis2 = new BufferedReader(new FileReader(inputDir+"/fastq2.fq"))

			// each four lines in the fastq file is actually one read, creat buffer to store the reads 
			val four_lines_fis1 = new ArrayBuffer[String]()
			val four_lines_fis2 = new ArrayBuffer[String]()

			var line_fis1: String = null
			var line_fis2: String = null
			var counter:Int = 0;
			var chunk_num:Int = 1;

			// BufferWritter of the output chunk file
			var bufferedWritter = new BufferedWriter(new FileWriter(streamDir + "/chunk_" + chunk_num + ".fq"))
			
			line_fis1 = bufferedReader_fis1.readLine
			line_fis2 = bufferedReader_fis2.readLine

			while (line_fis1 != null && line_fis2 != null) {

				four_lines_fis1 += line_fis1
				four_lines_fis2 += line_fis2
				counter+=1;

				if(counter % 4 == 0) {
					// Writting a read into a chunk file
					bufferedWritter.write(four_lines_fis1(0)) 
					bufferedWritter.newLine()
					bufferedWritter.write(four_lines_fis1(1)) 
					bufferedWritter.newLine()
					bufferedWritter.write(four_lines_fis1(2)) 
					bufferedWritter.newLine()
					bufferedWritter.write(four_lines_fis1(3)) 
					bufferedWritter.newLine()
					bufferedWritter.write(four_lines_fis2(0)) 
					bufferedWritter.newLine()
					bufferedWritter.write(four_lines_fis2(1)) 
					bufferedWritter.newLine()
					bufferedWritter.write(four_lines_fis2(2)) 
					bufferedWritter.newLine()
					bufferedWritter.write(four_lines_fis2(3)) 
					bufferedWritter.newLine()

					four_lines_fis1.clear()
					four_lines_fis2.clear()

					// this number obtained by the total length of the fastq file / 8, because we want to creat 8 chunks
					if (counter == 281496){
						chunk_num += 1;
						counter = 0;

						bufferedWritter.close()
						// creat a new chunk
						bufferedWritter = new BufferedWriter(new FileWriter(streamDir + "/chunk_" + chunk_num + ".fq")) 
					}
					if (counter == 281472 && chunk_num == 8){
						bufferedWritter.close()
					}

				}
				line_fis1 = bufferedReader_fis1.readLine
				line_fis2 = bufferedReader_fis2.readLine
			}

			bufferedReader_fis1.close()
			bufferedReader_fis2.close()

			// check the flag, if the flag is raised then jump outside the while loop, stop the listener and monitor thread 
			while (sam_num == 0){
				print("")
			}
			ssc.stop(stopSparkContext = false)
			stream_monitor.stop
		}
	}
