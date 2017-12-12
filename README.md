JQ KafkaStream
==============

This is a simple Scala, KafkaStreaming application doing JQ transformations on one topic.

The JQ filter is applied on a JSON of the following form:

    {
        "key": The content of the Kafka key,
        "value": The content of the Kafka message
    }

Installation
------------

    $ sbt compile

Usage
-----

1. Start a Kafka broker

2. Start the KafkaStreaming application

        $ sbt "run --bootstrap-servers localhost:9092 inJson outJson "'{"this":.value.data,"@context":"http://schema.org/lights"}' '.key.deviceid'

3. Send a JSON payload to the `inJson` topic

4. Read the transformed payload from the `outJson` topic

Packaging
---------

To create the standalone jar, execute:

    $ sbt assembly

Run it with:

    $ java -jar target/scala-2.12/KafkaStreamingJq-assembly-0.4.0-SNAPSHOT.jar

