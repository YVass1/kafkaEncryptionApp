package com.github.yoginivassaramo.kafka.encryptionapp

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties
import scala.io.Source
import scala.util.Random

object ProducerRawMessages extends App {

  //logging
  var logger: Logger = LoggerFactory.getLogger(ProducerRawMessages.getClass.getName())
  //values
  val bootstrapServers: String = "127.0.0.1:9092"

  //create Producer properties
  //property object
  val properties: Properties = new Properties()
  properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  //tells producer what data is sent and so how to covert to
  //bytes
  properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName())
  properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName())

  //create producer
  //key = string value = string
  val producer: KafkaProducer[String, String]
  = new KafkaProducer[String, String](properties)
  for (i <- 0 to 4) {
    //create producer record

    val topic: String = "raw_messages"
    val value: String = generateRandomWord.toLowerCase
    val key: String = "Key_id " + Integer.toString(i) //CREATE HASH MAP?

    val record: ProducerRecord[String, String]
    = new ProducerRecord[String, String](topic, key, value)

    logger.info("Key: " + key) //log the key

    //send data - asynchronous
    producer.send(record, new Callback {
      override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
        //execute everytime message sends successfully or exception if fails
        if (e == null) {
          //record successfully sent
          logger.info("Received new metadata: \n" +
            "Topic:" + recordMetadata.topic() + "\n" +
            "Partition:" + recordMetadata.partition() + "\n" +
            "Offset:" + recordMetadata.offset() + "\n" +
            "Timestamp:" + recordMetadata.timestamp())
        } else {
          logger.error("Error while producing", e)
        }
      }
    }).get() //.get() blocks the .send() to make data synchronous - dont do in production!
  }
  //flush data  = send data
  producer.flush()
  //flush and close
  producer.close()

  def generateRandomWord(): String ={
    val fileString = Source.fromResource("WordList.csv").mkString
    val wordsList = fileString.split(Array('\n', ',')).map(_.trim.stripPrefix("\"").stripSuffix("\""))
    val random: Random = new Random()
    val randomNumber = random.nextInt(wordsList.length - 1)
    val randomWord = wordsList(randomNumber)
    randomWord
  }

}
