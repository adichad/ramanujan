package com.askme.ramanujan.handler.search.message

import com.askme.ramanujan.handler.RequestParams
import com.askme.ramanujan.handler.message.RestMessage

/**
 * Created by adichad on 31/03/15.
 */
case class TestParams(req: RequestParams, startTime: Long, timeoutms: Long) extends RestMessage
