package com.askme.ramanujan.util

import spray.caching._

import scala.concurrent.{ExecutionContext, Future}
import spray.util._


/**
  * Created by adichad on 02/04/16.
  */
class LazyMap[T](capacity: Int)(implicit ec: ExecutionContext) {

  val m: Cache[T] = LruCache(capacity, capacity)

  def apply(key: String)(fun: => T)(success: (T) => Unit, failure: (Throwable) => Unit): Unit = {
    val future = m(key) {
      fun
    }

    future onSuccess {
      case v=>success(v)
    }
    future onFailure {
      case e=>failure(e)
    }
  }

  def refresh(key: String)(fun: => T)(success: (T) => Unit, failure: (Throwable) => Unit) = {
    val prev = m.remove(key)
    apply(key)(fun)(success, failure)
    prev
  }

  def remove(key: String) = m.remove(key)

}
