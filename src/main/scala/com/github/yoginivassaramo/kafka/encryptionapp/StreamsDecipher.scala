package com.github.yoginivassaramo.kafka.encryptionapp

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}

import java.util.Properties
import scala.util.control.Breaks

object StreamsDecipher extends App {
  //create properties
  val properties: Properties = new Properties()
  properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.01:9092")
  properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-decipher")
  properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde].getName())
  properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde].getName())

  //create a topology
  val streamsBuilder: StreamsBuilder = new StreamsBuilder()

  //input topic
  val inputTopic: KStream[String, String ] = streamsBuilder.stream("encrypted_messages")
  val decryptedStream: KStream[String, String] = inputTopic.mapValues(v => doDecryption(v))

  decryptedStream.to("decrypted_messages");

  //build the topology
  val kafkaStreams: KafkaStreams = new KafkaStreams(
    streamsBuilder.build(),
    properties
  );

  //start our stream application
  kafkaStreams.start();

  var selectedKey: String = "cab"
  var sortedKey: Array[Char] = selectedKey.toCharArray()
  var sortedKeyPos: Array[Int] = new Array[Int](selectedKey.length)

  def doProcessOnKey(selectedKey: String, sortedKey: Array[Char], sortedKeyPos: Array[Int]): Unit = {

    var temp: Char = 0
    var min: Int = 0
    var i: Int = 0
    var j: Int = 0
    var originalKey: Array[Char] = selectedKey.toCharArray()

    for (i <- 0 to (selectedKey.length - 1)) {
      var min = i
      for (j <- i to (selectedKey.length - 1))
        if (sortedKey(min) > sortedKey(j)) min = j
      if (min != i) {
        temp = sortedKey(i)
        sortedKey(i) = sortedKey(min)
        sortedKey(min) = temp
      }
    }
    val x: Int = 0
    val y: Int = 0

    // Fill the position of array according to alphabetical order
    for (x <- 0 to (selectedKey.length - 1)) {
      for (y <- 0 to (selectedKey.length - 1)) {
        if (originalKey(x) == sortedKey(y)) sortedKeyPos(x) = y
      }
    }

  }

  // to decrypt the targeted string
def doDecryption(s: String): String = {
  var i: Int = 0
  var j: Int = 0

  val encryptedwordarray: Array[Char] = s.toCharArray()
  doProcessOnKey(selectedKey, sortedKey, sortedKeyPos)

  // Now generating plain message
  val totalrows: Int = s.length() / selectedKey.length()
  val encryptedwordmatrix = Array.ofDim[Char](totalrows, (selectedKey.length))
  var tempcounter: Int = -1
  var colcounter2: Int = -1
  val loop = new Breaks

  for (i <- 0 to (selectedKey.length - 1)) {
    colcounter2 = -1
    loop.breakable {for (k <- 0 to (selectedKey.length - 1)) {
      colcounter2 += 1
      if (i == sortedKeyPos(k)) loop.break
    }}

    for (j <- 0 to (totalrows - 1)) {
      tempcounter += 1
      encryptedwordmatrix(j)(colcounter2) = encryptedwordarray(tempcounter)

    }
  }

  // storing matrix character in to a single string
  val decryptedwordarray = new Array[Char](totalrows * selectedKey.length)
  var k: Int = -1

  for (i <- 0 to (totalrows - 1)) {
    for (j <- 0 to (selectedKey.length - 1)) {

      if (encryptedwordmatrix(i)(j) != '*') {
        k += 1
        decryptedwordarray(k) = encryptedwordmatrix(i)(j)
      }
    }
  }
  decryptedwordarray.mkString("")
  }
}
