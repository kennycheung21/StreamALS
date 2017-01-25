package com.Kenny.streaming.spark

import java.io._

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.{SparkConf, SparkException}
import scala.concurrent.duration._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.OutputMode.Append
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._
import scala.collection.Map
import org.apache.log4j.{Level, LogManager, Logger}


/**
  * Created by kzhang on 1/13/17.
  */

object streamingKMeans {

  private val logger = LogManager.getLogger(getClass)

  val DEFAULT_PROPERTIES_FILE = "conf/consumeKMeans-defaults.conf"

  case class Rating(uId: Int, mId: Int, rating: Float)

  def main(args: Array[String]) {

    var propertiesFile: String = null

    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: consumeKMeans.jar <batch> [properties-file]
           |  <batch> the interval of each batch for spark streaming [required]
           |  [properties-file] the config properties file for streamingKMeans [optional]
         """.stripMargin)
      System.exit(1)
    }

    val batch = args(0)

    if (args.length < 2) {
      println(s"No properties-file configured. It will use the default properties file ${DEFAULT_PROPERTIES_FILE} .")
    }else{
      propertiesFile = args(1)
      println(s"Use provided ${propertiesFile} as properties config file.")
    }

    //StreamingExamples.setStreamingLogLevels()

    val wholeConfigs = loadPropertiesFile(propertiesFile)
    val kafkaParams = prepareKafkaConfigs(wholeConfigs)
    val kMeansConfigs = prepareKMeansConfigs(wholeConfigs)

    if (wholeConfigs.getOrDefault("verbose", "false") == "true" )
      {
        println(s"Logger Class: ${this.toString}. Logger lever: ${logger.getEffectiveLevel}")
        println(s"wholeConfigs: ${wholeConfigs.toString()}")
        println(s"kafkaParams: ${kafkaParams.toString()}")
        println(s"kMeansConfigs: ${kMeansConfigs.toString()}")
      }

    val spark = SparkSession.builder().appName("KennyStreamingKMeans").enableHiveSupport().getOrCreate()

    import spark.implicits._

    val topics =kafkaParams.getOrDefault("kafka.topics", "test").toString

    /*val ssc = new StreamingContext(sparkConf, Seconds(batch.toLong))
    ssc.checkpoint("/tmp/checkpoint")*/

    val topicsSet = topics.split(",").toSet.mkString(",")



    val stringStream = spark.readStream
      .format("kafka")
      .options(kafkaParams).option("subscribe", topicsSet)
      .load()
      .selectExpr("CAST(value AS STRING)").toDF()

    /*
    root
    |-- value: string (nullable = true)
    */
    stringStream.printSchema()

    val ratingStream = stringStream
      .withColumn("UID", uID($"value"))
      .withColumn("MID", mID($"value"))
      .withColumn("Rating", rating($"value"))

    ratingStream.printSchema()
    /*
        root
        |-- value: string (nullable = true)
        |-- UID: string (nullable = true)
        |-- MID: string (nullable = true)
        |-- Rating: string (nullable = true)
        */

    val out = ratingStream.writeStream
      .trigger(ProcessingTime(5.seconds))
      .outputMode("Append")
      .format("console")
      .start()

    out.awaitTermination()

    //ToDo: Manually comit kafka consumer offset after processing

    /*messages.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // some time later, after outputs have completed
      messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }*/


    //lines.print(10)
    // 173871,265,2.0

    /*val trainingData = lines.map(lineToVector)

    trainingData.print(10)
    val sc = ssc.sparkContext.
    ssc.start()
    ssc.awaitTermination()*/
  }

  /* /user/spark/ALS-KMeans/genome-scores.csv

  movieId	  tagId	            relevance
        1,	    1.0,	    02400000000000002
  */

 /* def loadMovies(sc: SQLContext, path : String)  = {
    val rdd = sqlContext.read.format("com.databricks.spark.csv")

  }*/

  def uID  = udf((v: String) => {
    val rs = v.split(",")(0)
    rs
  })

  def mID = udf((v: String) => {
    val rs = v.split(",")(1)
    rs
  })

  def rating = udf((v: String) => {
    val rs = v.split(",")(2)
    rs
  })

  def lineToVector(line: String) : Vector = {
    println("Line: "+line)
    // 173871,265,2.0
    val Array(uId, mId, rating) = line.split(",")
    /*val rating: Float = buf.remove(2).toFloat
    val uId = buf.remove(0).toInt*/

    println(f"UID = ${uId}, rating = ${rating}, MoviesID = ${mId}")
    val stringBuf = "[" + mId + "]"
    Vectors.parse(stringBuf)
  }
  @throws[IOException]
  def loadPropertiesFile (propertiesFile: String) : Map[String, String] = {
    val props = new Properties
    var map :Map[String, String] = Map()
    var propsFile: File = null
    if (propertiesFile != null) {
      propsFile = new File(propertiesFile)
      if (!propsFile.isFile)
        {
          throw new FileNotFoundException(s"Invalid properties file ${propertiesFile}.")
        }
    }
    else propsFile = new File(DEFAULT_PROPERTIES_FILE)

    if (propsFile.isFile) {
      var fd:FileInputStream = null
      try {
        fd = new FileInputStream(propsFile)
        props.load(new InputStreamReader(fd, "UTF-8"))
        map = props.stringPropertyNames()
          .map(k => (k, props(k).trim))
          .toMap
      } finally {
        if (fd != null) {
          try {
            fd.close()
          }catch {
            case e: IOException => {
              throw new SparkException(s"Failed when loading Spark properties from ${propsFile.getName}", e)
            }
          }
        }
      }
    } else {
      throw new FileNotFoundException(s"Default properties file ${DEFAULT_PROPERTIES_FILE} not found.")
    }
    map
  }

  def prepareKafkaConfigs (wholeConfig: Map[String, String], prefix: String = "kafkaStream."): Map[String, String] = {
    var kafkaParam = new ConcurrentHashMap[String, String]
    val DEFAULT_BOOTSTRAP_SERVER = "localhost:6667"
    val DEFAULT_TOPICS = "test"

    val forbiddenConfigs = Array("group.id", "auto.offset.reset", "key.deserializer", "value.deserializer", "enable.auto.commit", "interceptor.classes")

    wholeConfig.foreach{ case (k, v) =>
      if (k.startsWith(prefix)) kafkaParam.put(k.replace(prefix, "kafka."), v)
    }

    logger.debug(s"Before injecting the default values, kafkaParams: ${kafkaParam.toString}")

    if (!kafkaParam.containsKey("kafka.bootstrap.servers")){
      logger.info(s"Property kafka.bootstrap.servers is not found, setting it to default value ${DEFAULT_BOOTSTRAP_SERVER}.")
      kafkaParam.put("kafka.bootstrap.servers", DEFAULT_BOOTSTRAP_SERVER)
    }
    if (!kafkaParam.containsKey("kafka.topics")){
      logger.info(s"Property kafka.topics is not found, setting it to default value ${DEFAULT_TOPICS}.")
      kafkaParam.put("kafka.topics", DEFAULT_TOPICS)
    }
    // The following Kafka params cannot be set and the Kafka source will throw an exception in spark2

    for (conf <- forbiddenConfigs) {
      if (kafkaParam.containsKey("kafka."+ conf)){
        logger.warn(s"Property kafka.${conf} is found and forbidden, removing it.")
        kafkaParam.remove("kafka."+ conf)
      }
    }

    kafkaParam.toMap[String,String]
  }

  def prepareKMeansConfigs (wholeConfig: Map[String, String], prefix: String = "kMeans."): Map[String, String] = {

    var kMeansParam = new ConcurrentHashMap[String, String]
    val DEFAULT_K: Int = 5
/*    val DEFAULT_RUNS = 10
    val DEFAULT_EPSILON = 1.0e-6*/

    wholeConfig.foreach{ case (k, v) =>
      if (k.startsWith(prefix)) kMeansParam.put(k.substring(prefix.length), v)
    }

    if (!kMeansParam.containsKey("K")){
      logger.info(s"Property K is not found, setting it to default value ${DEFAULT_K}.")
      kMeansParam.put("K", DEFAULT_K.toString)
    }
    kMeansParam.toMap[String, String]
  }
}
