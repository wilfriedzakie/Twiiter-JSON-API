// Databricks notebook source
import sys.process._
"wget -P /tmp https://www.datacrucis.com/media/datasets/stratahadoop-BCN-2014.json" !!


// COMMAND ----------

val localpath="file:/tmp/stratahadoop-BCN-2014.json"
dbutils.fs.mkdirs("dbfs:/datasets/")
dbutils.fs.cp(localpath, "dbfs:/datasets/")
display(dbutils.fs.ls("dbfs:/datasets/stratahadoop-BCN-2014.json"))
val df = sqlContext.read.json("dbfs:/datasets/stratahadoop-BCN-2014.json")

// COMMAND ----------

/*
*To find the hashtags in tweets we select the tweet text, we split it and we get the word starting with the hash symbol '#' representing hashtags
*We print the first 10 tweets
**/


val rdd= df.select("text").rdd.map(row => row.getString(0))
val words = rdd.flatMap(_.split(" "))
val hashtag=words.filter(_.startsWith("#"))
print("10 hashtags displayed below")
hashtag.take(10).foreach(println)

// COMMAND ----------

print("There are: "+hashtag.count+" tags\n")
print("\nBelow are some tags and their occurances\n")
val hashcount=hashtag.map(word => (word,1)).reduceByKey((a,b) => a+b)
hashcount.take(10).foreach(println)

// COMMAND ----------

/*
*Most frequent tags are obtained by applying a mapreduce function on the 'hastags' RDD generated above.
*A key value pairs is generated from the mapper where the key is the hashtags and the value is 1
*The pairs are reduced by their keys to get the number of occurences
*the result is inverted and sorted by values then re-inverted to get the hashtag occurence in descending order.
*The 10 most frequent tags are displayed on the screen
*/

print("The most frequent tags are\n")
val hashcount=hashtag.map(word => (word,1)).reduceByKey((a,b) => a+b).map(item => item.swap).sortByKey(false).map(item => item.swap)
hashcount.take(10).foreach(println)

// COMMAND ----------

/**
*For this question, we select from the dataframe the user's name and we apply a mapper to generate a key value pair for each user name. 
*The key is the name and the value is one. 
*Further a reducer reduces the pairs by key.
*To sort the users according to their occurence, we swap the pair and we sort by the value(in integer type) in descending order.
*The result is swap back to get the user's names following by their number of tweets.  
*/



val rdd_user= df.select("user.name").rdd.map(row => row.getString(0)).map(word => (word,1)).reduceByKey((a,b) => a+b).map(item => item.swap).sortByKey(false).map(item => item.swap)
print("The 10 users with more tweets:\n\n")
rdd_user.take(10).foreach(println)

// COMMAND ----------

/* This function convert string tweet's date into date data type. It uses Java libraries such as Date, SimpleDateFormat and Calendar. 
* Firstly, we set dateformat in a string. The format should match with the tweet's date format.
*Secondly, we create a SimpleDateFormat variable with argument the format string. That variable will be in charge of parsing the date string into date data type.
*Finally, we retun the parsed date
*/

import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar

def tweetsDate (tweet_date:String) : Date = {
  var  TWITTER:String = "EEE MMM dd HH:mm:ss ZZZZZ yyyy"
  val sf = new SimpleDateFormat(TWITTER)
  sf.setLenient(true)
  var date:Date =sf.parse(tweet_date)
   return date
}


// COMMAND ----------

/*In this section we are going display the trending topics over a period.
*The tweet's dates are accessible in the "created_at" column in string data type. In order to compare it to a date, the function tweetsDate(explained above) is implemented.
*We first select for each line tweet's text and the creation date from the dataframe and we compare the tweet's creation to the starting as well as the ending date of our period. If the tweet was published in that period, the text is written in a RDD variable otherwise None is written.

*The RDD generated in java.seriazable, is converted to string, splitted and the hashatgs to extract the hashtags.
*Finally the trending topics are determined using mapreduce. The reduction is done by key  
*/

//Define a date format and the period from which we would like to get the trends. Date and time can be set



val sf1 = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
var start_date:Date=sf1.parse("17/11/2014 23:00:00")
var end_date:Date=sf1.parse("20/11/2014 23:40:00")

val tweets_period=df.select("text","created_at").rdd.map(row=>if(tweetsDate(row.getString(1)).compareTo(start_date)> 0 && tweetsDate(row.getString(1)).compareTo(end_date)< 0) row.getString(0) else None)

val tweets_string=tweets_period.map(row => row.toString())
val word=tweets_string.flatMap(_.split(" "))
val hashtag_list=word.filter(_.startsWith("#"))
val trend=hashtag_list.map(word => (word,1)).reduceByKey((a,b) => a+b).map(item => item.swap).sortByKey(false).map(item => item.swap)

trend.take(50).foreach(println)


// COMMAND ----------


