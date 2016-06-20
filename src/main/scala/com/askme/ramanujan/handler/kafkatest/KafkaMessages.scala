package com.askme.ramanujan.handler.kafkatest

import com.askme.ramanujan.handler.RequestParams

/**
  * Created by adichad on 03/04/16.
  */

case class KafkaParams(req: RequestParams, source: String, startTime: Long, timeoutms: Long)
