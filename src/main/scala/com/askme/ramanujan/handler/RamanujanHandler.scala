package com.askme.ramanujan.handler

import java.io.IOException

import akka.actor.SupervisorStrategy.{Restart, Resume}
import akka.actor.{Actor, OneForOneStrategy, Props}
import com.askme.ramanujan.Configurable
import com.askme.ramanujan.handler.kafkatest.KafkaRouter
import com.askme.ramanujan.handler.search.RamanujanRouter
import com.askme.ramanujan.server.RootServer.AppContext
import com.askme.ramanujan.util.CORS
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import spray.routing.Directive.pimpApply
import spray.routing.HttpService

import scala.language.postfixOps
import scala.concurrent.duration._



class RamanujanHandler(val config: Config, val serverContext: AppContext)
  extends HttpService with Actor with Logging with Configurable with CORS {

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: IOException ⇒ Resume
      case _: NullPointerException ⇒ Resume
      case _: Exception ⇒ Restart
    }

  private implicit val service: RamanujanHandler = this
  private val route = {
    cors {
      compressResponseIfRequested() {
        decompressRequest() {
          get {
            RamanujanRouter(this) ~ KafkaRouter(this)
          }
        }
      }
    }
  }

  override final def receive: Receive = {
    runRoute(route)
  }

  override def postStop = {
    info("Kill Message received")
  }

  implicit def actorRefFactory = context
}

