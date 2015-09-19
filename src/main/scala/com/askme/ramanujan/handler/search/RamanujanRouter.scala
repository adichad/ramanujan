package com.askme.ramanujan.handler.search

import akka.actor.Props
import com.askme.ramanujan.handler._
import com.askme.ramanujan.handler.search.message._
import spray.http.MediaTypes._
import spray.http.{HttpRequest, RemoteAddress}
import com.askme.ramanujan.util.Utils._

/**
 * Created by adichad on 31/03/15.
 */
case object RamanujanRouter extends Router {

  override def apply(implicit service: RamanujanHandler) = {
    import service._
    clientIP { (clip: RemoteAddress) =>
      requestInstance { (httpReq: HttpRequest) =>
        jsonpWithParameter("callback") {
          path("test") {
            parameters('timeoutms.as[Int]?60000) { (timeoutms) =>
              respondWithMediaType(`application/json`) { ctx =>
                  context.actorOf(Props(classOf[RamanujanCompleter], config, serverContext, ctx, TestParams(
                    RequestParams(httpReq, clip),
                    System.currentTimeMillis,timeoutms
                  )))
                }
            }
          }
        }
      }
    }
  }

}
