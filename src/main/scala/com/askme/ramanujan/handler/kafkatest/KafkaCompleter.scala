package com.askme.ramanujan.handler.kafkatest

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import com.askme.ramanujan.Configurable
import com.askme.ramanujan.handler.message.{ErrorResponse, RestMessage}
import com.askme.ramanujan.server.RootServer.AppContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json4s._
import spray.can.Http
import spray.http._
import spray.http.StatusCodes._
import spray.httpx.Json4sSupport
import spray.routing.RequestContext
import spray.http.MediaTypes._

import scala.concurrent.duration._
import scala.reflect.ClassTag

case object Ok

class KafkaCompleter(val config: Config, appContext: AppContext, requestContext: RequestContext, kafkaParams: KafkaParams) extends Actor with Configurable with Json4sSupport with Logging {
  val json4sFormats = DefaultFormats

  /*
  val target = context.actorOf (Props (classOf[KafkaRequestHandler], config, appContext) )
  context.setReceiveTimeout(Duration(kafkaParams.timeoutms * 5, MILLISECONDS))
  target ! kafkaParams
  */



  val responseStart = HttpResponse(entity = HttpEntity(`text/html`, "starting\n"))
  requestContext.responder ! ChunkedResponseStart(responseStart).withAck(Ok)


  override def receive = {
    case Ok =>
      Thread.sleep(500)
      appContext.messages.foreachRDD { m =>
        requestContext.responder ! MessageChunk(s"$m\n").withAck(Ok)
      }



    case ev: Http.ConnectionClosed =>
      info(s"connection closed ${ev}")
      context.stop(self)

  }

  def complete[T <: AnyRef](status: StatusCode, obj: T) = {
    requestContext.complete(status, obj)
    context.stop(self)
  }


  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        val timeTaken = System.currentTimeMillis - kafkaParams.startTime
        error("[" + timeTaken + "] [" + kafkaParams.req.clip.toString + "]->[" + kafkaParams.req.httpReq.uri + "]", e)
        complete(InternalServerError, e)
        Stop
      }
    }
}


