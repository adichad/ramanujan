package com.askme.ramanujan.handler.search

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, OneForOneStrategy, Props, ReceiveTimeout}
import com.askme.ramanujan.Configurable
import com.askme.ramanujan.handler.message.{ErrorResponse, RestMessage, Timeout}
import com.askme.ramanujan.handler.EmptyResponse
import com.askme.ramanujan.handler.search.message.{TestParams}
import com.askme.ramanujan.server.RootServer.AppContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.json4s._
import spray.http.StatusCode
import spray.http.StatusCodes._
import spray.httpx.Json4sSupport
import spray.routing.RequestContext

import scala.concurrent.duration.{Duration, MILLISECONDS}

class RamanujanCompleter(val config: Config, appContext: AppContext, requestContext: RequestContext, searchParams: TestParams) extends Actor with Configurable with Json4sSupport with Logging {
  val json4sFormats = DefaultFormats

  val target = context.actorOf (Props (classOf[RamanujanRequestHandler], config, appContext) )
  context.setReceiveTimeout(Duration(searchParams.timeoutms * 5, MILLISECONDS))
  target ! searchParams

  override def receive = {
    case _: EmptyResponse => {
      val timeTaken = System.currentTimeMillis - searchParams.startTime
      warn("[" + timeTaken + "] [" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "] [empty search criteria]")
      complete(BadRequest, EmptyResponse("empty search criteria"))
    }
    case err: ErrorResponse => complete(InternalServerError, err.message)
    case res: RestMessage => complete(OK, res)

  }

  def complete[T <: AnyRef](status: StatusCode, obj: T) = {
    requestContext.complete(status, obj)
    context.stop(self)
  }


  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        val timeTaken = System.currentTimeMillis - searchParams.startTime
        error("[" + timeTaken + "] [" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "]", e)
        complete(InternalServerError, e)
        Stop
      }
    }
}


