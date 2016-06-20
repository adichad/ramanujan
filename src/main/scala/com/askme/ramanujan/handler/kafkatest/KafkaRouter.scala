package com.askme.ramanujan.handler.kafkatest


import akka.actor.Props
import com.askme.ramanujan.handler._
import com.askme.ramanujan.handler.search.message._
import spray.http.MediaTypes._
import spray.http.{HttpRequest, RemoteAddress}

/**
  * Created by adichad on 03/04/16.
  */
case object KafkaRouter extends Router {

  override def apply(implicit service: RamanujanHandler) = {
    import service._

    clientIP { (clip: RemoteAddress) =>
      requestInstance { (httpReq: HttpRequest) =>
        jsonpWithParameter("callback") {
          path("stream"/ Segment) { (source)=>
            parameters('timeoutms.as[Int]?60000) { (timeoutms) =>
              respondWithMediaType(`application/json`) { ctx =>
                context.actorOf(Props(classOf[KafkaCompleter], config, serverContext, ctx, KafkaParams(
                  RequestParams(httpReq, clip),
                  source,
                  System.currentTimeMillis,
                  timeoutms
                )))
              }
            }
          }
        }
      }
    }
  }

}
