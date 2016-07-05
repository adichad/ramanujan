package com.askme.ramanujan.server

import java.io.Closeable

import com.askme.ramanujan.Configurable


trait Server extends Closeable with Configurable {
  def bind: Unit
  def close: Unit
}
