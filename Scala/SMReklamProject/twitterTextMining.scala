//sbt içi


name := "twitterDeneme"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.2.0"

libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"


//son 

import org.apache.spark._
import com.datastax.spark.connector._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import TutorialHelper._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf


object Tutorial {
  def main(args: Array[String]) {

    // Checkpoint directory
    val checkpointDir = TutorialHelper.getCheckpointDirectory()

    // Configure Twitter credentials
    val apiKey = "**"
    val apiSecret = "**"
    val accessToken = "**"
    val accessTokenSecret = "**"
    TutorialHelper.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)

    //Cassandra

    val myConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1").setMaster("local[4]").setAppName("Tutorial")
    val sc = new SparkContext("local", "test", myConf)

    case class FoodToUserIndex(food: String, user: String)

    val user_table = sc.cassandraTable("tutorial", "user")

    val food_index = user_table.map(r => new FoodToUserIndex(r.getString("favorite_food"), r.getString("name")))
    food_index.saveToCassandra("tutorial", "food_to_user_index")




    //Cassandra


    // Your code goes here
   val conf =new SparkConf(true).setMaster("local[4]").setAppName("TwitterNLP")
    val ssc = new StreamingContext(conf, Seconds(1))


    val tweets = TwitterUtils.createStream(ssc, None, Array("mutluyum", "happy"))


    val statuses = tweets.map(status => status.getText())

    val statusesA = tweetsA.map(statusA => statusA.getText())



    statuses.print()
    statusesA.print()

    ssc.checkpoint(checkpointDir)


    ssc.start()
    ssc.awaitTermination()

  }
}


