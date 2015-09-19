package com.askme.ramanujan.handler

import com.askme.ramanujan.handler.message.RestMessage
import org.json4s.JsonAST.JValue
import spray.http.{HttpRequest, RemoteAddress}


case class RequestParams(httpReq: HttpRequest, clip: RemoteAddress) extends RestMessage

case class EmptyResponse(reason: String) extends RestMessage









