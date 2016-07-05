package com.askme.ramanujan.actors

import akka.actor.Actor
import com.askme.ramanujan.Configurable
import com.typesafe.config.Config

class Listener(val config: Config) extends Actor with Configurable with Serializable{
		  def receive = {
		    case message: String =>
		      //info("[AKKA] == "+message)
		  }
}