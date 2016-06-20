package com.askme.ramanujan.handler.search


import akka.actor.Actor
import com.askme.ramanujan.Configurable
import com.askme.ramanujan.handler.message.{ErrorResponse, RamanujanResult}
import com.askme.ramanujan.handler.search.message.TestParams
import com.askme.ramanujan.server.RootServer.AppContext
import com.typesafe.config.Config
import org.elasticsearch.spark._
import org.elasticsearch.index.query.QueryBuilders._
import grizzled.slf4j.Logging
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

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
        JField("query", parse(termQuery("LocationName", "pizza").toString)),
        JField("_source", JArray(List(JString("LocationName")))),
        JField("size", JInt(10000))
      )
    )
  )

  implicit class Jsonifier(obj: AnyRef) {
    implicit val formats = org.json4s.DefaultFormats
    def toJValue = parse(Serialization.write(obj))
  }

  case class RDDResult(rdd: RDD[_], searchParams: TestParams)

  override def receive = {
      case searchParams: TestParams =>
        try {
          info(query)
          appContext.rddCache("test")(sc.esRDD("askme/place", query).cache()) ({
            rdd: RDD[_]=>self ! RDDResult(rdd, searchParams)
          },{
            e: Throwable => throw e
          })
        } catch {
          case e: Throwable =>
            error("exception", e)
            context.parent ! ErrorResponse(e.getMessage, e)
        }
      case rddResult: RDDResult =>
        val res = rddResult.rdd.take(10).toJValue
        info("Hello!: "+compact(res))
        context.parent ! RamanujanResult(System.currentTimeMillis()-rddResult.searchParams.startTime, res)

  }


}

