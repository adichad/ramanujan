package com.askme.ramanujan.server

import com.askme.ramanujan.Configurable

//removing Closeable
trait Server extends Configurable with Serializable {
  //def bind: Unit
  //def close: Unit
}
