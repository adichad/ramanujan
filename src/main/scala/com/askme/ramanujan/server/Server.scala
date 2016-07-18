package com.askme.ramanujan.server

import java.io.Closeable

import com.askme.ramanujan.Configurable


trait Server extends Closeable with Configurable with Serializable {
  //def bind: Unit
  //def close: Unit
}
