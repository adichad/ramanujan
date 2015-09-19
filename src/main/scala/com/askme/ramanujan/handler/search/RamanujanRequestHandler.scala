package com.askme.ramanujan.handler.search

import java.net.URLEncoder

import akka.actor.Actor
import com.askme.ramanujan.Configurable
import com.askme.ramanujan.handler.message.{RamanujanResult, ErrorResponse}
import com.askme.ramanujan.handler.search.message.{TestParams}
import com.askme.ramanujan.server.RootServer.AppContext
import com.typesafe.config.Config
import org.elasticsearch.spark._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query.FilterBuilders._
import grizzled.slf4j.Logging
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._
import scala.collection.Map


/**
 * Created by adichad on 08/01/15.
 */


class RamanujanRequestHandler(val config: Config, appContext: AppContext) extends Actor with Configurable with Logging {
  private val sc = appContext.sparkContext
  val query = compact(
    JObject(
      List(
        JField("query", parse(termQuery("Product.categorykeywordsexact", "mobile").toString)),
        JField("_source", JArray(List(JString("LocationName"))))
      )
    )
  )

  override def receive = {
      case searchParams: TestParams =>
        try {

          info(query)
          val res = sc.esRDD("askme/place", query).take(10).toList.toString()
          info(res)
          context.parent ! RamanujanResult(System.currentTimeMillis()-searchParams.startTime, res)

        } catch {
          case e: Throwable =>
            context.parent ! ErrorResponse(e.getMessage, e)
        }
  }


}

