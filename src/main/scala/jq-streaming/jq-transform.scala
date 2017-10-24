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

//sbt "run --bootstrap-servers localhost:9092 inJson outJson "'{"this":.data,"@context":"http://schema.org/lights"}' inJson_2 outJson_2 "'{"this":.data,"@context":"http://schema.org/pollution"}'"

object JQTransformationStream {
  val usage = """
    Usage: jq-stream --bootstrap-servers <server1 [serverN]...> [--rest-port <port>] <input-topic> <output-topic> <jq-filter> [<input-topic> <output-topic> <jq-filter>]...
  """

  def streamJQTransform(streamBuilder: KStreamBuilder, mapper: ObjectMapper,
    inputTopic: String, outputTopic: String, filter: String): Unit = {

    val compiledFilter: JsonQuery = JsonQuery.compile(filter)

    val rawStream: KStream[String, Array[Byte]] = streamBuilder.stream(Serdes.String, Serdes.ByteArray, inputTopic)
    val jqTranformStream: KStream[String, JsonNode] = rawStream.flatMapValues(new ValueMapper[Array[Byte], java.lang.Iterable[JsonNode]] {
      override def apply(value: Array[Byte]): java.lang.Iterable[JsonNode] = {
        try {
          val in: JsonNode = mapper.readTree(value)
          val result: Seq[JsonNode] = compiledFilter.apply(in).asScala
          List(result(0)).asJava // Only return first element
        } catch {
          case e: Exception =>
            println("Dropping invalid payload ...")
            List().asJava
        }
      }
    })

    // This stream takes a JSON data, and serializes it to a string
    val serializeJson: KStream[String, Array[Byte]] = jqTranformStream.mapValues(new ValueMapper[JsonNode, Array[Byte]] {
      override def apply(value: JsonNode): Array[Byte] = {
        mapper.writeValueAsBytes(value)
      }
    })
    serializeJson.to(Serdes.String, Serdes.ByteArray, outputTopic)

    println(Console.BLUE + s"Reading data from $inputTopic, applying filter '$filter', writing to $outputTopic" + Console.RESET)
  }

  def main(args: Array[String]): Unit = {

    var bootstrapServers = ""
    var restPort: Int = 0

    var processings: List[(String, String, String)] = List()

    def processArgs(args: List[String]): Boolean = {
      args match {
        case "--bootstrap-servers" :: a :: tail => bootstrapServers = a; processArgs(tail)
        case "--rest-port":: a :: tail => restPort = a.toInt; processArgs(tail)
        case inputTopic :: outputTopic :: jqFilter :: tail =>
          processings = (inputTopic, outputTopic, jqFilter) :: processings;
          processArgs(tail)
        case Nil => true
        case _ => println(usage)
          System.exit(-1)
          false
      }
    }

    processArgs(args.toList)

    val settings = new Properties
    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "jq-transformation")
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    settings.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8989")
    settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    val MAPPER: ObjectMapper = new ObjectMapper();

    val streamBuilder = new KStreamBuilder
    for ((i, o, f) <- processings) {
      streamJQTransform(streamBuilder, MAPPER, i, o, f)
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
