package com.askme.ramanujan.handler.message

import org.json4s.JsonAST.JValue

/**
 * Created by adichad on 17/09/15.
 */
case class RamanujanResult(`server-time-ms`: Long, results: JValue) extends RestMessage
