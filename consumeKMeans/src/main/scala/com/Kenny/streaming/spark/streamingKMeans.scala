package com.Kenny.streaming.spark

import java.io._

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, SparkException}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
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
        println(s"Logger Class: ${logger.getClass.getName}. Logger lever: ${logger.getEffectiveLevel}")
        println(s"wholeConfigs: ${wholeConfigs.toString()}")
        println(s"kafkaParams: ${kafkaParams.toString()}")
        println(s"kMeansConfigs: ${kMeansConfigs.toString()}")
      }


    val sparkConf = new SparkConf().setAppName("KennyStreamingKMeans")
    val topics =kafkaParams.getOrDefault("topics", "test").toString

    val ssc = new StreamingContext(sparkConf, Seconds(batch.toLong))
    ssc.checkpoint("/tmp/checkpoint")

    val topicsSet = topics.split(",").toSet

    val messages = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String,String](topicsSet, kafkaParams))


    //Manually comit kafka consumer offset after processing
    messages.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // some time later, after outputs have completed
      messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    val keys = messages.map(_.key())
    val lines = messages.map(_.value())
    messages.map(_.value()).print(10)
    // 173871,265,2.0

    ssc.start()
    ssc.awaitTermination()
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

  def prepareKafkaConfigs (wholeConfig: Map[String, String], prefix: String = "kafkaStream."): Map[String, Object] = {
    var kafkaParam = new ConcurrentHashMap[String, Object]
    val DEFAULT_BOOTSTRAP_SERVER = "localhost:6667"
    val DEFAULT_TOPICS = "test"
    val DEFAULT_GROUP_ID = "KMean_Consumer"
    val DEFAULT_KEY_DESERIALIZER = classOf[IntegerDeserializer]
    val DEFAULT_VALUE_DESERIALIZER = classOf[StringDeserializer]

    wholeConfig.foreach{ case (k, v) =>
      if (k.startsWith(prefix)) kafkaParam.put(k.substring(prefix.length), v)
    }

    logger.debug(s"Before injecting the default values, kafkaParams: ${kafkaParam.toString}")

    if (!kafkaParam.containsKey("bootstrap.servers")){
      logger.info(s"Property bootstrap.servers is not found, setting it to default value ${DEFAULT_BOOTSTRAP_SERVER}.")
      kafkaParam.put("bootstrap.servers", DEFAULT_BOOTSTRAP_SERVER)
    }
    if (!kafkaParam.containsKey("topics")){
      logger.info(s"Property topics is not found, setting it to default value ${DEFAULT_TOPICS}.")
      kafkaParam.put("topics", DEFAULT_TOPICS)
    }
    if (!kafkaParam.containsKey("group.id")){
      logger.info(s"Property group.id is not found, setting it to default value ${DEFAULT_GROUP_ID}.")
      kafkaParam.put("group.id", DEFAULT_GROUP_ID)
    }
    if (!kafkaParam.containsKey("key.deserializer")){
      logger.info(s"Property key.deserializer is not found, setting it to default value ${DEFAULT_KEY_DESERIALIZER.toString}.")
      kafkaParam.put("key.deserializer", DEFAULT_KEY_DESERIALIZER)
    }
    if (!kafkaParam.containsKey("value.deserializer")){
      logger.info(s"Property value.deserializer is not found, setting it to default value ${DEFAULT_VALUE_DESERIALIZER.toString}.")
      kafkaParam.put("value.deserializer", DEFAULT_VALUE_DESERIALIZER)
    }
    kafkaParam.toMap[String,Object]
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
