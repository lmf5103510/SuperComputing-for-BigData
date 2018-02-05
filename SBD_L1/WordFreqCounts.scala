import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import scala.util.matching.Regex
import java.util.Locale
import org.apache.commons.lang3.StringUtils

// This class holds two words, and two boolean checks for each word to check if there is a special character at the
// begining and the end of the word. It's a valid bigram if both words are non empty
case class Bigram(firstWord: String,f_Beg_anySpecialChar:Boolean,f_End_anySpecialChar:Boolean, secondWord: String, nd_Beg_anySpecialChar:Boolean, nd_End_anySpecialChar:Boolean) {
  def isValidBigram: Boolean = !firstWord.isEmpty && !secondWord.isEmpty
}

object Bigram {
  // For each line, the input is split into words, and returns a list of bigram
  def apply(input: String): List[Bigram] = {
    // For each line, a special identifier is added at the beginning and end of line
    // In order to not lose counts when performing the sliding(2)
    val splitWords = ("itsstartoftheline!"+:(input.split(" "):+ "itsendoftheline")).sliding(2).toList
    // This regular expression is used to replace all non alphabetic at the beginning and end of word
    val reg = "^[^a-zA-Z]*|[^a-zA-Z]*$"
    // This regular exp is used to check if there is a special character at the beginning
    val reg_beg = "^[^a-zA-Z\\s].*"
    // This regular exp is used to check if there is a special character at the end
    val reg_end = ".*[^a-zA-Z\\s]$"

    // Replacing all non_alphabetic which starts at the beg or end of the word with empty in order to get the word it self
    // Each two words is creating a bigram class. If there is one word, and the other word would be empty
    splitWords.map(words =>
      if(words.size > 1) new Bigram(words(0).trim.toLowerCase.replaceAll(reg,""),words(0).trim.toLowerCase.matches(reg_beg),words(0).trim.toLowerCase.matches(reg_end),words(1).trim.toLowerCase.replaceAll(reg,""),words(1).trim.toLowerCase.matches(reg_beg),words(1).trim.toLowerCase.matches(reg_end)) else if(words.size == 1) new Bigram(words(0).trim.toLowerCase.replaceAll(reg,""),words(0).trim.toLowerCase.matches(reg_beg),words(0).trim.toLowerCase.matches(reg_end),"",false,false) else new Bigram("",false,false,"",false,false))
  }
}

object WordFreqCounts
{

  def main(args: Array[String])
  {
    // Get input file's name from this command line argument
    val inputFile = args(0)
    val outputFile = "output.txt"
    val conf = new SparkConf().setAppName("WordFreqCounts")
    val sc = new SparkContext(conf)

    println("Input file: " + inputFile)

    // Uncomment these two lines if you want to see a less verbose messages from Spark
    //Logger.getLogger("org").setLevel(Level.OFF);
    //Logger.getLogger("akka").setLevel(Level.OFF);

    val t0 = System.currentTimeMillis

    val input =  sc.textFile(inputFile)

    // prepare pairs from the original data
    // for example:   i like play computer games.   =>   ( (i, like), (like, play), (play, computer), (computer, games) )
    // (firstword, secondword)
    val words = input.flatMap(Bigram.apply).filter(_.isValidBigram)

    // obtain the word frequency
    // (firstword, occurrence)
    val counts = words.filter(word => word != "itsstartoftheline!").map(word => (word.firstWord, 1)).reduceByKey{case (x, y) => x + y}

    // sort the counts result by occurrence alphabetically
    val sorted_list = counts.sortByKey().sortBy(_._2,false)

    // prepare counting precedence words frequency
    // (secondword, firstword)
    val filtered = words.filter(word => word.secondWord != "itsendoftheline").map(bg =>
      if(bg.f_End_anySpecialChar == false && bg.nd_Beg_anySpecialChar == false)
        (bg.secondWord, bg.firstWord)
      else
        (bg.secondWord, ""))

    // group val filtered by key
    // (secondword, listof preceded word)
    val grouping = filtered.groupByKey

    // count the frequency for precedence
    // (secondword, listof preceded word and its occurrence)
    val counting_pres  = grouping.map(x => (x._1, x._2.toList.groupBy(identity).mapValues(_.size).toSeq.sortWith(_._1 < _._1).sortWith(_._2 > _._2)))

    // filter the emply firstword which is the case when special character is the precedence
    // (secondword, listof preceded word and its occurrence)
    var empty_filterd_pres = counting_pres.map(x => (x._1, x._2.filter(e => e._1 != "")))

    // prepare the final format output
    // (word, list => string)
    val converted_toString = empty_filterd_pres.map(x => (x._1,x._2.mkString))

    // (word, occurrence.tostring)
    val converted_counts = sorted_list.map(x => (x._1, x._2.toString))

    // join the word frequency and preceded word frequency
    // (word, (occurrence, precedencelist.tostring))
    val joined_d = converted_counts.join(converted_toString).sortByKey().sortBy(_._2._1.toInt,false)

    // format the final output
    val format_data = joined_d.map(x => x._1 +":" + x._2._1 + "\n\t" + x._2._2.replace(")(","\n\t").replace("(","").replace(")","").replace(",",":"))

    // generate the output file
    format_data.saveAsTextFile(outputFile)

    val et = (System.currentTimeMillis - t0) / 1000
    System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
  }
}
