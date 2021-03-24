package com.github.yoginivassaramo.kafka.encryptionapp

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}

import java.util.Properties
import scala.util.control.Breaks
import scala.util.control.Breaks.break

object StreamsCipher extends App {

  var selectedKey: String = "cab"
  var sortedKey: Array[Char] = selectedKey.toCharArray()
  var sortedKeyPos: Array[Int] = new Array[Int](selectedKey.length)

  //create properties
  val properties: Properties = new Properties()
  properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.01:9092")
  properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-cipher")
  properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde].getName)
  properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde].getName)

  //create a topology
  val streamsBuilder: StreamsBuilder = new StreamsBuilder()

  //input topic
  val inputTopic: KStream[String, String] = streamsBuilder.stream("raw_messages")

  val encryptedStream: KStream[String, String] = inputTopic.mapValues(value => doEncryption(value))

  encryptedStream.to("encrypted_messages")

  //build the topology
  val kafkaStreams: KafkaStreams = new KafkaStreams(
    streamsBuilder.build(),
    properties
  )

  //start our stream application
  kafkaStreams.start()

  def doProcessOnKey(selectedKey: String, sortedKey: Array[Char], sortedKeyPos: Array[Int]): Unit = {

    var temp: Char = 0
    var min: Int = 0
    val i: Int = 0
    val j: Int = 0
    var originalKey: Array[Char] = selectedKey.toCharArray()

    for (i <- 0 to (selectedKey.length - 1)) {
      var min = i
      for (j <- i to (selectedKey.length - 1))
        if (sortedKey(min) > sortedKey(j)) min = j
      if (min != i) {
        var temp = sortedKey(i)
        sortedKey(i) = sortedKey(min)
        sortedKey(min) = temp;
      }
    }

    val x: Int = 0
    val y: Int = 0

    // Fill the position of array according to alphabetical order
    for ( x <- 0 to (selectedKey.length - 1)){
      for (y <-  0 to (selectedKey.length - 1)){
        if (originalKey(x) == sortedKey(y)) sortedKeyPos(x) = y
      }
    }

  }

  // encrypt the targeted string
  def doEncryption(plainText: String): String = {
    doProcessOnKey(selectedKey, sortedKey, sortedKeyPos)
    // Generate encrypted message by doing encryption using Transposition
    // Cipher

    //creating matrix of correct size due to key length and word give length
    var fullrows: Int = (plainText.length / selectedKey.length)
    val extrabit: Int = plainText.length % selectedKey.length
    val extrarow: Int = if (extrabit == 0) 0 else 1
    var coltemp = -1
    val lengthofencryptedword: Int = (fullrows + extrarow) * selectedKey.length
    val pmatrix = Array.ofDim[String]((fullrows + extrarow), selectedKey.length)
    val encry: Array[String] = new Array[String](lengthofencryptedword)
    var rows: Int = 0
    val totalrows: Int = fullrows + extrarow

    //filling in matrix
    for (m <- 0 to (lengthofencryptedword - 1)) {
      coltemp += 1
      if (m < plainText.length) {
        if (coltemp == selectedKey.length) {
          rows += 1
          coltemp = 0
        }
        pmatrix(rows)(coltemp) = plainText.charAt(m).toString
      } else pmatrix(rows)(coltemp) = "*"
    }

    var len = -1
    val k: Int = 0
    var colcounter: Int = -1
    val loop = new Breaks

    for (p <- 0 to (selectedKey.length - 1)) {
      colcounter = -1

      loop.breakable {for (k <- 0 to (selectedKey.length - 1)) {
        colcounter += 1
        if (p == sortedKeyPos(k)) loop.break
      }}

      for (q <- 0 to (totalrows - 1)) {
        len += 1
        encry(len) = pmatrix(q)(colcounter)
      }
    }
    val p1 = encry.mkString("")
    p1
  }
}
