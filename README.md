JQ KafkaStream
==============

This is a simple Scala, KafkaStreaming application doing JQ transformations on one topic.

Installation
------------

    $ sbt compile

Usage
-----

1. Start a Kafka broker

2. Start the KafkaStreaming application

        $ sbt "run --bootstrap-servers localhost:9092 inJson outJson "'{"this":.data,"@context":"http://schema.org/lights"}'

3. Send a JSON payload to the `inJson` topic

4. Read the transformed payload from the `outJson` topic

Packaging
---------

To create the standalone jar, execute:

    $ sbt assembly

Run it with:

    $ java -jar target/scala-2.12/KafkaStreamingJq-assembly-0.3.0-SNAPSHOT.jar

