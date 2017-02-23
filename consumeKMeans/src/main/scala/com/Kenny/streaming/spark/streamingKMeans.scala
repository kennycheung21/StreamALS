package com.Kenny.streaming.spark

import java.io._
import java.util.Calendar
import java.util.{Map => JMap, HashMap => JHashMap}

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.consumer._
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}


/**
  * Created by kzhang on 1/13/17.
  */

object streamingKMeans {

  private val logger = LogManager.getLogger(getClass)

  val DEFAULT_PROPERTIES_FILE = "conf/consumeKMeans-defaults.conf"

  val schema = StructType(Array(StructField("movieId", DataTypes.IntegerType), StructField("tagId", DataTypes.IntegerType), StructField("relevance", DataTypes.FloatType)))

  final case class score(movieId: Int, tagId: Int, relevance: Float)


  def main(args: Array[String]) {

    var propertiesFile: String = null
    val committed = new JHashMap[TopicPartition, OffsetAndMetadata]()

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

    val wholeConfigs = loadPropertiesFile(propertiesFile)
    val kafkaParams = prepareKafkaConfigs(wholeConfigs, batch.toInt)
    val kMeansConfigs = prepareKMeansConfigs(wholeConfigs)

    if (wholeConfigs.getOrDefault("verbose", "false") == "true" )
      {
        println(s"Logger Class: ${logger.getClass.getName}. Logger lever: ${logger.getEffectiveLevel}")
        println(s"wholeConfigs: ${wholeConfigs.toString()}")
        println(s"kafkaParams: ${kafkaParams.toString()}")
        println(s"kMeansConfigs: ${kMeansConfigs.toString()}")
      }

    val topics =kafkaParams.getOrDefault("topics", "test").toString

    val spark = SparkSession.builder().appName("KennyStreamingKMeans").enableHiveSupport().getOrCreate()

    import spark.implicits._

    val genomeScoreDS = loadGenomeScore(spark, "/user/spark/ALS-KMeans/genome-scores.csv", schema)

    //  val genomeScoreCount = genomeScoreDS.count()

    //println(s"Total line of score: ${genomeScoreCount}")

    def consolidateScore(k: Int, ite: Iterator[score]) = {
      val buf = ArrayBuffer.fill(1128){0F}
      ite.foreach( (s: score) => {buf(s.tagId-1) = s.relevance})
      (k, buf.toArray)
    }

    //[int, score]
    val genomeScoreMap = genomeScoreDS.groupByKey(_.movieId).mapGroups(consolidateScore)

    val genomeScoreMapRdd = genomeScoreMap.rdd.cache()

    val cb = new myCallback()
    /*Todo: val genomeScoreVar = spark.sparkContext.broadcast(genomeScoreMap.rdd)
     val LPVarStream = lines.transform(rdd => rdd.map()    )
    */
    /*genomeScoreMap.show(2)
    genomeScoreMap.printSchema()
    root
     |-- _1: integer (nullable = true)
     |-- _2: array (nullable = true)
     |    |-- element: float (containsNull = false)
     */

    //genomeScoreDF.printSchema()

    /*df.printSchema()
    root
     |-- movieId: integer (nullable = true)
     |-- tagId: integer (nullable = true)
     |-- relevance: float (nullable = true)

    /user/spark/ALS-KMeans/genome-scores.csv
      movieId	  tagId	            relevance
            1,	    1.0,	    02400000000000002
    */
    val ssc = new StreamingContext(spark.sparkContext, Seconds(batch.toLong))
    ssc.checkpoint("/tmp/checkpoint")

    val topicsSet = topics.split(",").toSet
    val messages = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String,String](topicsSet, kafkaParams))

    /*val lines = messages.map(_.value()).map( (v: String) => {
      val Array(uID, mID, rating) = v.split(",")
      (mID.toInt, (uID.toInt, rating.toFloat))
    })*/

    val LPStream = messages.transform{rdd =>
      var offsetRanges = Array[OffsetRange]()
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      logger.info("OffsetRanges of RDD "+rdd.id + " : " + offsetRanges.mkString(";"))
      val rddRS = rdd.map(_.value()).map( (v: String) => {
        val Array(uID, mID, rating) = v.split(",")
        (mID.toInt, (uID.toInt, rating.toFloat))
      })
      val LPRdd = genomeScoreMapRdd.join(rddRS).map(r => {
        val mID: Int = r._1
        val score: Array[Float] = r._2._1
        val uID: Int = r._2._2._1
        val rate: Float = r._2._2._2
        score.foreach(_*rate)
        //val relScore = score.ensuring()
        val relScore:String = "["+score.mkString(",")+"]"
        val v = Vectors.parse(relScore)
        val LP = LabeledPoint(uID, v)
        LP
      })

      messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges, cb)
      //offsetRanges
      LPRdd
    }

    //(3000,([F@2a39753a,(191070,4.0)))
/*    val LPStream = lines.transform(rdd => {

    })*/



    //LPStream.foreachRDD(rdd => rdd.take(5).foreach( (l:LabeledPoint) => println(l.features.size)))
      //size = 1128

    //LPStream.print(2)
    /*
    (192889.0,[0.0165,0.01875,0.04425,0.069,0...])
     */

    val model = new StreamingKMeans()
      .setK(kMeansConfigs.getOrDefault("K", "5").toInt)
      .setDecayFactor(kMeansConfigs.getOrDefault("DecayFactor", "1.0").toFloat)
      .setRandomCenters(kMeansConfigs.getOrDefault("RandomCenters.dim", "1128").toInt, kMeansConfigs.getOrDefault("RandomCenters.weight", "0").toFloat)

    model.trainOn(LPStream.map(_.features))

    val predictData = LPStream.transform(rdd => {
      rdd.sample(false, 0.01)
    })
    val predictions = model.predictOn(predictData.map(_.features))

    predictions.foreachRDD { rdd =>
      val modelString = model.latestModel().clusterCenters
        .map(c => c.toString.slice(1, c.toString.length-1)).mkString("\n")
      val predictString = rdd.map(p => p.toString).collect().mkString(",")
      val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
      println(dateString + "-model: ")
      println(modelString)
      println(dateString + "-prediction: ")
      println(predictString)
    }

    ssc.start()
/*    val genomeScoreCount = genomeScoreMap.count()

    logger.info("Total Genome Score Count: " + genomeScoreCount)*/
    ssc.awaitTermination()

  }
  // Total 1128 tags,

  private class myCallback extends OffsetCommitCallback () with java.io.Serializable {
    def onComplete(m: JMap[TopicPartition, OffsetAndMetadata], e: Exception) {
      if (null != e) {
        logger.error("Commit failed", e)
      } else {
        logger.info("Commit sucessfully for: "+ m.toString)
      }
    }
  }

  def loadGenomeScore (spark: SparkSession, path: String, sT: StructType) = {
    import spark.implicits._
    val ds = spark.read.schema(sT).option("header", "true").csv(path).as[score]
    ds
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

  def prepareKafkaConfigs (wholeConfig: Map[String, String], batch: Int = 3, prefix: String = "kafkaStream."): Map[String, Object] = {
    var kafkaParam = new ConcurrentHashMap[String, Object]
    val DEFAULT_BOOTSTRAP_SERVER = "localhost:6667"
    val DEFAULT_TOPICS = "test"
    val DEFAULT_GROUP_ID = "KMean_Consumer"
    val DEFAULT_KEY_DESERIALIZER = classOf[IntegerDeserializer]
    val DEFAULT_VALUE_DESERIALIZER = classOf[StringDeserializer]
    val MIN_HEARTBEAT_MS = (batch + 5) * 1000 // add the some cushions
    val MIN_SESSION_TIMEOUT_MS = MIN_HEARTBEAT_MS * 3

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


    if (!kafkaParam.containsKey("heartbeat.interval.ms")) {
      logger.info(s"Property heartbeat.interval.ms is not found, setting it to batch value ${MIN_HEARTBEAT_MS}.")
      kafkaParam.put("heartbeat.interval.ms", MIN_HEARTBEAT_MS.toString)
    }else {
      if (kafkaParam.getOrDefault("heartbeat.interval.ms", "3000").asInstanceOf[Int] < MIN_HEARTBEAT_MS){
        logger.info(s"Property heartbeat.interval.ms is less than the batch interval, setting it to batch value ${MIN_HEARTBEAT_MS}")
        kafkaParam.update("heartbeat.interval.ms", MIN_HEARTBEAT_MS.toString)
      }
    }

    val hearbeat = kafkaParam.getOrDefault("heartbeat.interval.ms", MIN_HEARTBEAT_MS.toString).toString.toInt
    var sessionTimeout = kafkaParam.getOrDefault("session.timeout.ms", "10000").toString.toInt

    if (sessionTimeout < hearbeat * 3){
      logger.info(s"Property session.timeout.ms is less than the 3 times of batch interval, setting it to batch value ${hearbeat*3}")
      kafkaParam.update("session.timeout.ms", (hearbeat*3).toString)
      sessionTimeout = hearbeat*3
    }

    val requestTimeout = kafkaParam.getOrDefault("request.timeout.ms", "40000").toString.toInt

    if (requestTimeout <= sessionTimeout){
      logger.info(s"request.timeout.ms is less than session.timeout.ms, setting it to ${sessionTimeout+10000}")
      kafkaParam.update("request.timeout.ms", (sessionTimeout+10000).toString)
    }

    kafkaParam.toMap[String,Object]
  }

  def prepareKMeansConfigs (wholeConfig: Map[String, String], prefix: String = "kMeans."): Map[String, String] = {

    var kMeansParam = new ConcurrentHashMap[String, String]
    val DEFAULT_K: Int = 5
    val DEFAULT_DecayFactor = 1.0
    val DEFAULT_numDimensions = 1128
    val DEFAULT_weight = 0.0
/*    val DEFAULT_RUNS = 10
    val DEFAULT_EPSILON = 1.0e-6*/

    wholeConfig.foreach{ case (k, v) =>
      if (k.startsWith(prefix)) kMeansParam.put(k.substring(prefix.length), v)
    }

    if (!kMeansParam.containsKey("K")){
      logger.info(s"Property K is not found, setting it to default value ${DEFAULT_K}.")
      kMeansParam.put("K", DEFAULT_K.toString)
    }
    if (!kMeansParam.containsKey("DecayFactor")){
      logger.info(s"Property DecayFactor is not found, setting it to default value ${DEFAULT_DecayFactor}.")
      kMeansParam.put("DecayFactor", DEFAULT_DecayFactor.toString)
    }
    if (!kMeansParam.containsKey("RandomCenters.dim")){
      logger.info(s"Property RandomCenters.dim is not found, setting it to default value ${DEFAULT_numDimensions}.")
      kMeansParam.put("RandomCenters.dim", DEFAULT_numDimensions.toString)
    }
    if (!kMeansParam.containsKey("RandomCenters.weight")){
      logger.info(s"Property RandomCenters.weight is not found, setting it to default value ${DEFAULT_weight}.")
      kMeansParam.put("RandomCenters.weight", DEFAULT_weight.toString)
    }
    kMeansParam.toMap[String, String]
  }
}
