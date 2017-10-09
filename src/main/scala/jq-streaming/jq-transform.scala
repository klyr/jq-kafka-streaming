package org.barbot.jqstream

import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode

import net.thisptr.jackson.jq.JsonQuery
import net.thisptr.jackson.jq.exception.JsonQueryException

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, KeyValue}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream._

import scala.collection.JavaConverters._


//sbt "run --bootstrap-servers localhost:9092 --input-topic inJson --output-topic outJson --jq-filter "'{"this":.data,"@context":"http://schema.org/lights"}'

class JQTransformation(val filter: String) {

  val compiledFilter: JsonQuery = JsonQuery.compile(filter)

  def apply(source: String): String = {
    return source
  }

  def apply(source: JsonNode): Seq[JsonNode] = {
    val jqTransformed = compiledFilter.apply(source).asScala
    return jqTransformed
  }
}

object JQTransformationStream {
  val usage = """
    Usage: jq-stream --bootstrap-servers <server1, server2> --input-topic <topic> --output-topic <topic> --jq-filter <filter>
  """

  def main(args: Array[String]): Unit = {
    if (args.length != 8) {
      println(usage)
      System.exit(-1)
    }

    var bootstrapServers = ""
    var inputTopic = ""
    var outputTopic = ""
    var jqFilter = ""
    args.sliding(2, 2).toList.collect {
      case Array("--bootstrap-servers", a: String) => bootstrapServers = a
      case Array("--input-topic", a: String) => inputTopic = a
      case Array("--output-topic", a: String) => outputTopic = a
      case Array("--jq-filter", a: String) => jqFilter = a
    }

    val settings = new Properties
    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "jq-transformation")
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    settings.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8989")
    settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    val MAPPER: ObjectMapper = new ObjectMapper();
    val writer = MAPPER.writer()
    val jqTransform = new JQTransformation(jqFilter)

    val streamBuilder = new KStreamBuilder
    val rawStream: KStream[String, Array[Byte]] = streamBuilder.stream(Serdes.String, Serdes.ByteArray, inputTopic)

    val jqTranformStream: KStream[String, JsonNode] = rawStream.flatMapValues(new ValueMapper[Array[Byte], java.lang.Iterable[JsonNode]] {
      override def apply(value: Array[Byte]): java.lang.Iterable[JsonNode] = {
        try {
          val in: JsonNode = MAPPER.readTree(value)
          val result: Seq[JsonNode] = jqTransform.apply(in)
          return List(result(0)).asJava // Only return first element
        } catch {
          case e: Exception =>
            println("Dropping invalid payload ...")
            return List().asJava
        }
      }
    })

    // This stream takes a JSON data, and serializes it to a string
    val serializeJson: KStream[String, Array[Byte]] = jqTranformStream.mapValues(new ValueMapper[JsonNode, Array[Byte]] {
      override def apply(value: JsonNode): Array[Byte] = {
        return MAPPER.writeValueAsBytes(value)
      }
    })
    serializeJson.to(Serdes.String, Serdes.ByteArray, outputTopic)

    val streams = new KafkaStreams(streamBuilder, settings)
    streams.cleanUp
    streams.start
    print(streams.toString)

    println(s"Reading data from $inputTopic, applying filter '$jqFilter', writing to $outputTopic")

    println("Let's start the http server ...")
    RestProxy.startServer("localhost", 8888)
  }
}
