package com.Kenny.streaming.kafka

import java.util.Properties

import org.apache.log4j.Logger
import joptsimple.OptionParser
import kafka.message.{DefaultCompressionCodec, NoCompressionCodec}
import kafka.serializer.DefaultEncoder
import kafka.utils.{CommandLineUtils, ToolsUtils}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata
//import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source
import scala.util.Random

/**
  * Created by kzhang on 12/19/16.
  */
object ALSProducer {

  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]) {

    try {
      val config = new ProducerConfig(args)

      //ToDo: this can be re-write
      val reader = Class.forName(config.readerClass).newInstance().asInstanceOf[LineMessageReader]
      reader.init(getReaderProps(config))

      val producerProps = getNewProducerProps(config)
      val producer = new KafkaProducer[Int, String](producerProps)
      //val t = new NewShinyProducer()
      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run() {
          producer.close()
        }
      })

      var recordWave: Iterator[ProducerRecord[Int, String]] = null
      val sync = producerProps.getProperty("producer.type", "async").equals("sync")
      val topic = config.topic


      //Send the Rating.
      /*
      userId	movieId	rating	timestamp
        1	    122	    2	      945544824
        Total: 24404096 records

        2200,3527,4.0,1422126710
       */
      recordWave = reader.nextWave()
      var wave = 0
      var total = reader.getSize()
      while (true){
        var p = 0
        var progress: Float = p.toFloat/total.toFloat
        var threshold: Double = 0

        while (recordWave.hasNext) {
          try {
            var record = recordWave.next()
            val key = record.key()
            var value = record.value()

            //Drop the timestamp from the record
            var valueNew = value.split(",").dropRight(1)

            var vString = valueNew.mkString(",")

            //Setting the timestamp field in ProducerRecord using the current timestamp
            //ProducerRecord(topic=ALStest, partition=0, key=0, value=2,441,2.0, timestamp=1484613127)

            /*userId	movieId	rating
              1	    122	    2
              */

            val recordNew = new ProducerRecord[Int, String](record.topic, key, (System.currentTimeMillis/1000).toLong, key, vString)

            var response = if (sync) producer.send(recordNew).get() else producer.send(recordNew,new ErrorLoggingCallback(topic, key, value))
            logger.debug("Successfully sent records: " + recordNew.toString + " with response: "+ response + " Sync = " + sync.toString)
            Thread.sleep(config.sendTimeout.toLong)

          } catch {
            case e: joptsimple.OptionException =>
              logger.error("jobtsimple: " + e.getMessage)
            case e: Exception =>
              logger.error("Exception: " + e.getMessage)
            case what: Throwable => logger.error("Unkown: " + what.toString)
          }
          p += 1
          progress = p.toFloat/total.toFloat
          if (progress >= threshold)
            {
              var percentage = progress*100
              logger.info(f"Progress: $percentage%3.2f%%")
              threshold += 0.001
            }
        }
        if (progress != 1)
        {
          logger.error(f"Failed to send all the message, $p%d out of $total%d message are sent. Progress: $progress%3.9f")
        }
        else {
          logger.info("Progress: 100%")
          logger.info("I'm gonna sleep after sending the wave #" + wave + " !")
        }

        //Thread.sleep(producerProps.getProperty("sendTimeout", "2000").toLong)
        Thread.sleep(config.sendTimeout.toLong)
        recordWave = reader.nextWave()
        wave += 1
      }
    } catch {
      case e: joptsimple.OptionException =>
        logger.fatal(e.getMessage)
        System.exit(1)
      case e: Exception =>
        logger.fatal(e.printStackTrace)
        System.exit(1)
    }
    System.exit(0)
  }

  def getReaderProps(config: ProducerConfig): Properties = {
    val props = new Properties
    props.put("topic",config.topic)
    props.put("inputFile", config.inputFile)
    props.putAll(config.cmdLineProps)
    props
  }

  private def producerProps(config: ProducerConfig): Properties = {
    val props =
      if (config.options.has(config.producerConfigOpt))
        Utils.loadProps(config.options.valueOf(config.producerConfigOpt))
      else new Properties
    props.putAll(config.extraProducerProps)
    props
  }

  def getNewProducerProps(config: ProducerConfig): Properties = {

    val props = producerProps(config)

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokerList)
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.compressionCodec)
    props.put(ProducerConfig.SEND_BUFFER_CONFIG, config.socketBuffer.toString)
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, config.retryBackoffMs.toString)
    props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, config.metadataExpiryMs.toString)
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, config.maxBlockMs.toString)
    props.put(ProducerConfig.ACKS_CONFIG, config.requestRequiredAcks.toString)
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, config.requestTimeoutMs.toString)
    props.put(ProducerConfig.RETRIES_CONFIG, config.messageSendMaxRetries.toString)
    props.put(ProducerConfig.LINGER_MS_CONFIG, config.sendTimeout.toString)
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.maxMemoryBytes.toString)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.maxPartitionMemoryBytes.toString)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "kenny-ALSProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put("security.protocol", config.securityProtocol.toString)

    props
  }

  class ProducerConfig(args: Array[String]) {
    val parser = new OptionParser
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic id to produce messages to.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val brokerListOpt = parser.accepts("broker-list", "REQUIRED: The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
      .withRequiredArg
      .describedAs("broker-list")
      .ofType(classOf[String])
    val syncOpt = parser.accepts("sync", "If set message send requests to the brokers are synchronously, one at a time as they arrive.")
    val compressionCodecOpt = parser.accepts("compression-codec", "The compression codec: either 'none', 'gzip', 'snappy', or 'lz4'." +
      "If specified without value, then it defaults to 'gzip'")
      .withOptionalArg()
      .describedAs("compression-codec")
      .ofType(classOf[String])
    val batchSizeOpt = parser.accepts("batch-size", "Number of messages to send in a single batch if they are not being sent synchronously.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(200)
    val messageSendMaxRetriesOpt = parser.accepts("message-send-max-retries", "Brokers can fail receiving the message for multiple reasons, and being unavailable transiently is just one of them. This property specifies the number of retires before the producer give up and drop this message.")
      .withRequiredArg
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(3)
    val retryBackoffMsOpt = parser.accepts("retry-backoff-ms", "Before each retry, the producer refreshes the metadata of relevant topics. Since leader election takes a bit of time, this property specifies the amount of time that the producer waits before refreshing the metadata.")
      .withRequiredArg
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(100)
    val sendTimeoutOpt = parser.accepts("timeout", "If set and the producer is running in asynchronous mode, this gives the maximum amount of time" +
      " a message will queue awaiting sufficient batch size. The value is given in ms.")
      .withRequiredArg
      .describedAs("timeout_ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1000)
    val timeIntervalOpt = parser.accepts("interval", "Time interval to send message to the topic in ms. Default is 5000 ms.")
      .withRequiredArg
      .describedAs("interval")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(5000)
    val queueSizeOpt = parser.accepts("queue-size", "If set and the producer is running in asynchronous mode, this gives the maximum amount of " +
      " messages will queue awaiting sufficient batch size.")
      .withRequiredArg
      .describedAs("queue_size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(10000)
    val queueEnqueueTimeoutMsOpt = parser.accepts("queue-enqueuetimeout-ms", "Timeout for event enqueue")
      .withRequiredArg
      .describedAs("queue enqueuetimeout ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(Int.MaxValue)
    val requestRequiredAcksOpt = parser.accepts("request-required-acks", "The required acks of the producer requests")
      .withRequiredArg
      .describedAs("request required acks")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(0)
    val requestTimeoutMsOpt = parser.accepts("request-timeout-ms", "The ack timeout of the producer requests. Value must be non-negative and non-zero")
      .withRequiredArg
      .describedAs("request timeout ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1500)
    val metadataExpiryMsOpt = parser.accepts("metadata-expiry-ms",
      "The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any leadership changes.")
      .withRequiredArg
      .describedAs("metadata expiration interval")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(5*60*1000L)
    val maxBlockMsOpt = parser.accepts("max-block-ms",
      "The max time that the producer will block for during a send request")
      .withRequiredArg
      .describedAs("max block on send")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(60*1000L)
    val maxMemoryBytesOpt = parser.accepts("max-memory-bytes",
      "The total memory used by the producer to buffer records waiting to be sent to the server.")
      .withRequiredArg
      .describedAs("total memory in bytes")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(32 * 1024 * 1024L)
    val maxPartitionMemoryBytesOpt = parser.accepts("max-partition-memory-bytes",
      "The buffer size allocated for a partition. When records are received which are smaller than this size the producer " +
        "will attempt to optimistically group them together until this size is reached.")
      .withRequiredArg
      .describedAs("memory in bytes per partition")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(16 * 1024L)
    val valueEncoderOpt = parser.accepts("value-serializer", "The class name of the message encoder implementation to use for serializing values.")
      .withRequiredArg
      .describedAs("encoder_class")
      .ofType(classOf[java.lang.String])
      .defaultsTo(classOf[DefaultEncoder].getName)
    val keyEncoderOpt = parser.accepts("key-serializer", "The class name of the message encoder implementation to use for serializing keys.")
      .withRequiredArg
      .describedAs("encoder_class")
      .ofType(classOf[java.lang.String])
      .defaultsTo(classOf[DefaultEncoder].getName)
    val messageReaderOpt = parser.accepts("line-reader", "The class name of the class to use for reading lines from standard in. " +
      "By default each line is read as a separate message.")
      .withRequiredArg
      .describedAs("reader_class")
      .ofType(classOf[java.lang.String])
      .defaultsTo(classOf[LineMessageReader].getName)
    val socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1024*100)
    val propertyOpt = parser.accepts("property", "A mechanism to pass user-defined properties in the form key=value to the message reader. " +
      "This allows custom configuration for a user-defined message reader.")
      .withRequiredArg
      .describedAs("prop")
      .ofType(classOf[String])
    val producerPropertyOpt = parser.accepts("producer-property", "A mechanism to pass user-defined properties in the form key=value to the producer. ")
      .withRequiredArg
      .describedAs("producer_prop")
      .ofType(classOf[String])
    val producerConfigOpt = parser.accepts("producer.config", s"Producer config properties file. Note that $producerPropertyOpt takes precedence over this config.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])
    val securityProtocolOpt = parser.accepts("security-protocol", "The security protocol to use to connect to broker.")
      .withRequiredArg
      .describedAs("security-protocol")
      .ofType(classOf[String])
      .defaultsTo("PLAINTEXT")

    val inputFileOpt = parser.accepts("input-file", "Input file to generate producer records.")
      .withRequiredArg()
      .describedAs("input")
      .ofType(classOf[String])

    val options = parser.parse(args : _*)
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Read data from standard input and publish it to Kafka.")
    CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, brokerListOpt)

    import scala.collection.JavaConversions._
    val inputFile = options.valueOf(inputFileOpt)
    val topic = options.valueOf(topicOpt)
    val brokerList = options.valueOf(brokerListOpt)
    ToolsUtils.validatePortOrDie(parser,brokerList)
    val sync = options.has(syncOpt)
    val compressionCodecOptionValue = options.valueOf(compressionCodecOpt)
    val compressionCodec = if (options.has(compressionCodecOpt))
      if (compressionCodecOptionValue == null || compressionCodecOptionValue.isEmpty)
        DefaultCompressionCodec.name
      else compressionCodecOptionValue
    else NoCompressionCodec.name
    val batchSize = options.valueOf(batchSizeOpt)
    val sendTimeout = options.valueOf(sendTimeoutOpt)
    val timeInterval = options.valueOf(timeIntervalOpt)
    val queueSize = options.valueOf(queueSizeOpt)
    val queueEnqueueTimeoutMs = options.valueOf(queueEnqueueTimeoutMsOpt)
    val requestRequiredAcks = options.valueOf(requestRequiredAcksOpt)
    val requestTimeoutMs = options.valueOf(requestTimeoutMsOpt)
    val messageSendMaxRetries = options.valueOf(messageSendMaxRetriesOpt)
    val retryBackoffMs = options.valueOf(retryBackoffMsOpt)
    val keyEncoderClass = options.valueOf(keyEncoderOpt)
    val valueEncoderClass = options.valueOf(valueEncoderOpt)
    val readerClass = options.valueOf(messageReaderOpt)
    val socketBuffer = options.valueOf(socketBufferSizeOpt)
    val cmdLineProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(propertyOpt))
    val extraProducerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(producerPropertyOpt))
    /* new producer related configs */
    val maxMemoryBytes = options.valueOf(maxMemoryBytesOpt)
    val maxPartitionMemoryBytes = options.valueOf(maxPartitionMemoryBytesOpt)
    val metadataExpiryMs = options.valueOf(metadataExpiryMsOpt)
    val maxBlockMs = options.valueOf(maxBlockMsOpt)
    val securityProtocol = options.valueOf(securityProtocolOpt).toString
  }


  class LineMessageReader {
    var topic: String = null
    //var reader: BufferedReader = null
    var parseKey = false
    var keySeparator = "\t"
    var ignoreError = false
    var lineNumber = Random
    var input: String = null
    var timeStamp: Long = System.currentTimeMillis / 1000
    var message = "Message_" + lineNumber.toString + ": " + timeStamp.toString
    var batchSize = 1

    def init(props: Properties) {
      topic = props.getProperty("topic")
      if(props.containsKey("parse.key"))
        parseKey = props.getProperty("parse.key").trim.toLowerCase.equals("true")
      if(props.containsKey("key.separator"))
        keySeparator = props.getProperty("key.separator")
      if(props.containsKey("ignore.error"))
        ignoreError = props.getProperty("ignore.error").trim.toLowerCase.equals("true")
      input = props.getProperty("inputFile")
      logger.info("Initialized MessageReader with input = " + input)
      //reader = new BufferedReader(new InputStreamReader(inputStream))
    }

    def nextWave(): Iterator[ProducerRecord[Int, String]] = {
       /*
        userId	movieId	rating	timestamp
        1	      122	    2	      945544824
        Total: 24404096 records
       */
      logger.info("Reading rating from file: " + input)
      Source.fromFile(input).getLines().map((value :String) => {
        val userId = value.split(",")(0).toInt
        val key:Int = userId % 2
        new ProducerRecord [Int, String] (topic, key, value)
      })
    }

    def getSize(): Int = {
      logger.debug("Calculating the size of file: " + input)
      Source.fromFile(input).getLines().size
    }

    def close() {}
  }

  class ErrorLoggingCallback(var topic: String, var key: Int, val value: String) extends Callback {

    def onCompletion(metadata: RecordMetadata, e: Exception) {
      if (e != null) {
        logger.error("Error when sending message to topic {} with key: {}, value: {} with error:", topic, key, value, e)
      }
    }
  }
}
