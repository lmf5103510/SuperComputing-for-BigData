import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import java.io._
import java.util.Locale
import java.text._
import java.net._
import java.util.Calendar
import org.apache.tika.language.LanguageIdentifier
import java.util.regex.Matcher
import java.util.regex.Pattern
import javax.xml.parsers.DocumentBuilderFactory
import scala.collection.mutable.ArrayBuffer
import javax.xml.parsers.DocumentBuilder
import org.w3c.dom.Document

object Twitterstats
{ 
	def getTimeStamp() : String =
	{
		return new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime())
	}
	
	def getLang(s: String) : String =
	{
		val inputStr = s.replaceFirst("RT", "").replaceAll("@\\p{L}+", "").replaceAll("https?://\\S+\\s?", "")
		var langCode = new LanguageIdentifier(inputStr).getLanguage
		
		// Detect if japanese
		var pat = Pattern.compile("\\p{InHiragana}") 
		var m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ja"
		// Detect if korean
		pat = Pattern.compile("\\p{IsHangul}");
		m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ko"
		
		return langCode
	}

	// This function reture the boolean whether a specifc hashtag occurs more than 3 times
	def isDuplicated(key : String , ls : List[String]) : Boolean = 
	{
		var counter = 0
		if(key == "None")
			return false

		for(i<-0 until ls.size) 
		{
			var ls_key = ls(i)
			if(ls_key == key)
				counter = counter + 1
		}

		if(counter > 3)
			return true
		else
			return false
	}

	var firstTime = true
	var t0: Long = 0
	val pw = new java.io.PrintWriter(new File("twitterLog.txt"))
	// This function generate the final output txt file.
	def write2Log(tweets: Array[(String, ((Long, String, Int, String, Int), Int))])
	{
		if (firstTime)
		{
			pw.write("Record#. <Number of HashTags for this HashTag> #<HashTag>:<user_name>:<tweetCount> < tweetText>\n")
			pw.write("==========================================================================\n")
			pw.write(getTimeStamp + "\n")
			pw.write("==========================================================================\n")
			t0 = System.currentTimeMillis
			firstTime = false
		}
		else
		{
			val seconds = (System.currentTimeMillis - t0) / 1000
			
			if (seconds < 120){
				println("\nElapsed time = " + seconds + " seconds. Logging will be started after 120 seconds.")
				return
			}
			
			if(seconds > 120 && seconds < 122) {
				println("Logging the output to the log file\n\n\n")
				var old_key  = List[String]()
				var num = 0

				for(i <-0 until tweets.size)
				{
					var key = tweets(i)._1
					if(!key.contains("#")) {
						key = "None"
					}
					old_key = key :: old_key
					var username = tweets(i)._2._1._2
					var tweetCount = tweets(i)._2._1._3
					var text = tweets(i)._2._1._4
					var hashtag_nums = tweets(i)._2._2

					// Print the first 10 tweets
				    if(!isDuplicated(key, old_key) && num < 10) {
				    	num += 1
				    	println(num + ". "+ hashtag_nums + " " + key + ":" + username + ":"+tweetCount + " " + text + "\n")
				    }

		        	// We enclose seconds with brackets because otherwise languages like persian would write seconds in reverse!
		        	pw.write(i + ". "+ hashtag_nums + " " + key + ":" + username + ":"+tweetCount + " " + text + "\n")
		        	pw.write("----------------------------------------------------------------\n")
		        }
			    pw.close()
			}
		}
	}

	//This function return the list of hashtags in the String input
	def getHashTags(str: String):List[String] ={
		 val pattern = "(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)".r
		 var hashtags = List[String]()
         for(m <- pattern.findAllIn(str)) { 
            hashtags = m.replaceAll("\\s", "") :: hashtags
          }
          return hashtags
	}

	def main(args: Array[String])
	{
		val file = new File("cred.xml")
		val documentBuilderFactory = DocumentBuilderFactory.newInstance
		val documentBuilder = documentBuilderFactory.newDocumentBuilder
		val document = documentBuilder.parse(file);
			
		// Configure Twitter credentials
		val consumerKey = document.getElementsByTagName("consumerKey").item(0).getTextContent 				
		val consumerSecret = document.getElementsByTagName("consumerSecret").item(0).getTextContent 		
		val accessToken = document.getElementsByTagName("accessToken").item(0).getTextContent 				
		val accessTokenSecret = document.getElementsByTagName("accessTokenSecret").item(0).getTextContent	
		
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)
		Logger.getRootLogger.setLevel(Level.OFF)

		// Set the system properties so that Twitter4j library used by twitter stream
		// can use them to generate OAuth credentials
		System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
		System.setProperty("twitter4j.oauth.accessToken", accessToken)
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

		val sparkConf = new SparkConf().setAppName("TwitterPopularTags")


		val ssc = new StreamingContext(sparkConf, Seconds(2))
		val tweets = TwitterUtils.createStream(ssc, None)
		
		// filter the non-english tweets
		val english_tweets = tweets.filter(status => getLang(status.getText)=="en"||getLang(status.getText)=="no")
		// (tweets'id, (username, current retweet number, current retweet number, tweets' content))
		val mapped_tweets = english_tweets.map(status => (if(status.isRetweet()) status.getRetweetedStatus().getId() else status.getId(),
															(if(status.isRetweet()) status.getRetweetedStatus().getUser.getScreenName else status.getUser.getScreenName, 
															if(status.isRetweet()) status.getRetweetedStatus().getRetweetCount else status.getRetweetCount,
															if(status.isRetweet()) status.getRetweetedStatus().getRetweetCount else status.getRetweetCount,
															if(status.isRetweet()) status.getRetweetedStatus().getText() else status.getText)))
		
		// (tweets'id, (username, max_counter_tweet, min_counter_tweet, tweets' content))
		// Later we use the difference of the max and min to measure the retweet number during the window
		val transformed_ = mapped_tweets.transform(rdd => rdd.sortByKey()).reduceByKeyAndWindow((a:(String, Int, Int, String), b:(String, Int, Int, String)) => 
			(a._1, math.max(a._2, b._2), math.min(a._2, b._2), a._4), Seconds(120), Seconds(2))

		transformed_.foreachRDD(rdd => rdd.cache())

		// Measuring the tweetcount
		// (tweets'id, tweetcount)
		val ds = transformed_.map(x => (x._1, (x._2._2 - x._2._3 + 1))).transform(rdd => rdd.reduceByKey(_ + _))
		
		//  (tweets'id, ((username, max_counter_tweet, min_counter_tweet, tweets' content), tweetount))
		var joined_d = transformed_.join(ds)

		// (tweets'id, (username, tweetcount, tweets' content, list of hashtags in the content))
		var hashtags_id_key = joined_d.map(x => (x._1, (x._2._1._1, x._2._2, x._2._1._4, getHashTags(x._2._1._4))))

		// (key, (tweets'id, username, tweetcount, tweets' content, 1/0))
		// if the key is hashtag then x._2._5 = 1 
		// the key keeps as id and x._2._5 == 0 if the list of the hashtags obtained from last step is empty
		var hashtags = joined_d.map(x => ((x._1, x._2._1._1, x._2._2, x._2._1._4), getHashTags(x._2._1._4))).transform(rdd => rdd.flatMap{case (x, list) => if(list.length > 0) list.map(k => (k, (x._1, x._2, x._3, x._4, 1))) else List((x._1.toString, (x._1, x._2, x._3, x._4, 0))) })

		// (id/hashtag, 1/0)
		var hashtags_pair = hashtags.map(x => (x._1, x._2._5))
		
		// (id/hashtag, number of hashtags)
		// measuring the number of hashtags
		var hashtags_counts = hashtags_pair.reduceByKey((x,y) => x + y)

		// (key, ((tweets'id, username, tweetcount, tweets' content, 1/0), number of hashtags))
		var final_output = hashtags.join(hashtags_counts)

		// filter out the hashtag with number of 1 and no retweets during the window then sort by the number of hashtags
		var filtering_tweetWithOnce = final_output.filter(x => !(x._2._1._3 == 1 && x._2._2 == 1)).transform(rdd => rdd.sortBy(x => x._2._2, false))

		// write the data into output txt file
		filtering_tweetWithOnce.foreachRDD(rdd => write2Log(rdd.collect))

		ssc.start()
		ssc.awaitTermination()
	}
}
