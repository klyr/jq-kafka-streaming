package org.barbot.jqstream

import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode


import net.thisptr.jackson.jq.JsonQuery
import net.thisptr.jackson.jq.exception.JsonQueryException

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, KeyValue}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream._

import scala.collection.JavaConverters._

//sbt "run --bootstrap-servers localhost:9092 --application-id 'jq-transformation' inJson outJson "'{"this":.data,"@context":"http://schema.org/lights"}' inJson_2 outJson_2 "'{"this":.value.data,"@context":"http://schema.org/pollution"}'" '{"deviceid": .value.deviceid}'

object JQTransformationStream {
  val usage = """
    Usage: jq-stream --bootstrap-servers <server1 [serverN]...> [--application-id <appID>] [--rest-port <port>] <input-topic> <output-topic> <jq-filter-body> <jq-filter-key> [<input-topic> <output-topic> <jq-filter-body> <jq-filter-key>]...
  """

  def streamJQTransform(streamBuilder: KStreamBuilder, mapper: ObjectMapper,
    inputTopic: String, outputTopic: String, filterBody: String, filterKey: String): Unit = {

    val compiledFilterBody: JsonQuery = JsonQuery.compile(filterBody)
    val compiledFilterKey: JsonQuery = JsonQuery.compile(filterKey)

    val rawStream: KStream[Array[Byte], Array[Byte]] = streamBuilder.stream(Serdes.ByteArray, Serdes.ByteArray, inputTopic)

    val jqTranformStream: KStream[JsonNode, JsonNode] = rawStream.flatMap(new KeyValueMapper[Array[Byte], Array[Byte], java.lang.Iterable[KeyValue[JsonNode, JsonNode]]] {
      override def apply(key: Array[Byte], value: Array[Byte]): java.lang.Iterable[KeyValue[JsonNode, JsonNode]] = {
        try {
          val inValue: JsonNode = mapper.readTree(value)
          val inKey: JsonNode = mapper.readTree(key)

          val top: ObjectNode = mapper.createObjectNode()
          top.put("key", inKey)
          top.put("value", inValue)

          val resultValue: Seq[JsonNode] = compiledFilterBody.apply(top).asScala
          val resultKey: Seq[JsonNode] = compiledFilterKey.apply(top).asScala
          val result = new KeyValue[JsonNode, JsonNode](resultKey(0), resultValue(0)) // Only return first element
          List(result).asJava
        } catch {
          case e: Exception =>
            println(s"Dropping invalid payload ... $e")
            List().asJava
        }
      }
    })

    // This stream takes a JSON data, and serializes it to a string
    val serializeJson: KStream[Array[Byte], Array[Byte]] = jqTranformStream.map(new KeyValueMapper[JsonNode, JsonNode, KeyValue[Array[Byte], Array[Byte]]] {
      override def apply(key: JsonNode, value: JsonNode): KeyValue[Array[Byte], Array[Byte]] = {
        val k = mapper.writeValueAsBytes(key)
        val v = mapper.writeValueAsBytes(value)
        new KeyValue[Array[Byte], Array[Byte]](k, v)
      }
    })
    serializeJson.to(Serdes.ByteArray, Serdes.ByteArray, outputTopic)

    println(Console.BLUE + s"Reading data from $inputTopic, applying filter '$filterBody' to body, and '$filterKey' to key, writing to $outputTopic" + Console.RESET)
  }

  def main(args: Array[String]): Unit = {

    var bootstrapServers = ""
    var applicationId = "jq-transformation"
    var restPort: Int = 0

    var processings: List[(String, String, String, String)] = List()

    def processArgs(args: List[String]): Boolean = {
      args match {
        case "--bootstrap-servers" :: a :: tail => bootstrapServers = a; processArgs(tail)
        case "--application-id" :: a :: tail => applicationId = a; processArgs(tail)
        case "--rest-port":: a :: tail => restPort = a.toInt; processArgs(tail)
        case inputTopic :: outputTopic :: jqFilterBody :: jqFilterKey :: tail =>
          processings = (inputTopic, outputTopic, jqFilterBody, jqFilterKey) :: processings;
          processArgs(tail)
        case Nil => true
        case _ => println(usage)
          System.exit(-1)
          false
      }
    }

    processArgs(args.toList)

    val settings = new Properties
    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    settings.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8989")
    settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    val MAPPER: ObjectMapper = new ObjectMapper();

    val streamBuilder = new KStreamBuilder
    for ((i, o, fb, fk) <- processings) {
      streamJQTransform(streamBuilder, MAPPER, i, o, fb, fk)
    }

    val streams = new KafkaStreams(streamBuilder, settings)
    streams.cleanUp
    streams.start
    print(streams.toString)

    if (restPort != 0) {
      println("Let's start the http server ...")
      RestProxy.startServer("localhost", restPort)
    }
  }
}
