package org.barbot.jqstream

import akka.http.scaladsl.model.{ ContentTypes, HttpEntity }
import akka.http.scaladsl.server.HttpApp
import akka.http.scaladsl.server.Route

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport

import spray.json._
import DefaultJsonProtocol._


object RestProxy extends HttpApp {
  type KafkaTopic = String
  type JQFilter = String

  case class JQTransformation(inputTopic: KafkaTopic, outputTopic: KafkaTopic, jqFilter: JQFilter)

  trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
    implicit val jqTransformationFormat = jsonFormat3(JQTransformation)
  }

  val transformations: Seq[JQTransformation] = List(
    JQTransformation("i1", "o1", "{.f1}"),
    JQTransformation("i2", "o2", "{.f2}"),
    JQTransformation("i3", "o3", "{.f3}")
  )

  override def routes: Route =
      path("info") {
        get {
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
        }
      }

  override  protected def postHttpBindingFailure(cause: Throwable): Unit = {
    println(s"The server could not be started due to $cause")
  }
}
