package com.github.yoginivassaramo.kafka.encryptionapp;

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import java.util.{Arrays, Properties}
import scala.jdk.CollectionConverters.IterableHasAsScala;

object ConsumerGroupRawMessages extends App {

    var logger: Logger = LoggerFactory.getLogger(ConsumerGroupRawMessages.getClass.getName())

    var bootstrapServers: String  = "127.0.01:9092"
    var groupId: String = "raw-messages-application"
    var topic: String = "raw_messages"

    val properties: Properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName())
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName())
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    //create consumer
    val consumer: KafkaConsumer[String, String]  =
            new KafkaConsumer[String, String](properties)

    //subscribe consumer to our topic(s)
    consumer.subscribe(Arrays.asList(topic))

    //subscribe to single topic
    //consumer.subscribe(Collections.singleton(topic));

    //poll for new data
    while(true){ //bad practice to break out of while loop
        val records: ConsumerRecords[String, String]  =
                consumer.poll(Duration.ofMillis(100))
        records.asScala.foreach(record => {
            logger.info("Key: " + record.key() + "\n" + "Value: " + record.value())
            logger.info("Partition: " + record.partition() + " Offset: " + record.offset())
        })
    }

}
