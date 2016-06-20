package com.askme.ramanujan.handler.kafkatest

import akka.actor.Actor
import com.askme.ramanujan.Configurable
import com.askme.ramanujan.handler.message.{ErrorResponse, RamanujanResult}
import com.askme.ramanujan.handler.search.message.TestParams
import com.askme.ramanujan.server.RootServer.AppContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.apache.spark.rdd.RDD
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.spark._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization


/**
 * Created by adichad on 08/01/15.
 */


class KafkaRequestHandler(val config: Config, appContext: AppContext) extends Actor with Configurable with Logging {
  private val sc = appContext.sparkContext

  implicit class Jsonifier(obj: AnyRef) {
    implicit val formats = org.json4s.DefaultFormats
    def toJValue = parse(Serialization.write(obj))
  }

  override def receive = {
      case kafkaParams: KafkaParams =>
        try {
          context.parent ! RamanujanResult(System.currentTimeMillis()-kafkaParams.startTime, JNothing)
        } catch {
          case e: Throwable =>
            error("exception", e)
            context.parent ! ErrorResponse(e.getMessage, e)
        }


  }


}

