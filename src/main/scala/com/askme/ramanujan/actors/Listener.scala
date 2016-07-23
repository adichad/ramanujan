package com.askme.ramanujan.actors

import akka.actor.Actor
import com.askme.ramanujan.Configurable
import com.typesafe.config.Config
import grizzled.slf4j.Logging

class Listener(val config: Config) extends Actor with Configurable with Logging{
		  def receive = {
		    case message: String =>
		      debug("[DEBUG] [Listener] : "+message)

		  }
}