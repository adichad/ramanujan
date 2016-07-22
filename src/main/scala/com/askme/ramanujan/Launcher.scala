package com.askme.ramanujan

import java.io.{Closeable, File, PrintWriter}
import java.lang.management.ManagementFactory

import com.askme.ramanujan.server.Server
import grizzled.slf4j.Logging
import org.apache.log4j.Logger

object Launcher extends Configurable with Logging {
  
  object Holder extends Serializable {      
     @transient lazy val log = Logger.getLogger(getClass.getName)    
  }
  
  debug("[DEBUG] starting the launcher . . .")
  override protected[this] val config = configure("environment", "application", "environment_defaults", "application_defaults")
  debug("[DEBUG] config initialized . . .")
  def main(args: Array[String]) { // redundant args

    try {
      // hack to make configuration parameters available in logback.xml
      backFillSystemProperties("component.name", "log.path.current", "log.path.archive", "log.level") // from reference.conf + environment_defaults.conf
      info(string("component.name")) // instance.fqn <- cluster.name <- component.name+env.name
      info("Log path: " + string("log.path.current"))
      info("creating it, the Log path: . . .")
      new java.io.File(string("log.path.current")).mkdirs
      debug("[DEBUG] backfilling completed . . .")
      writePID(string("daemon.pidfile"))
      if (boolean("sysout.detach")) System.out.close()
      if (boolean("syserr.detach")) System.err.close()
      debug("[DEBUG] going to initialize the rootserver . . .")
      val servers = map[Server]("server").values.toList // map of objects of [Server], instantiated using "server" -> .values -> .toList
      debug("[DEBUG] rootserver object has been instantiated . . .")
      debug("[DEBUG] closing the previous running instances of the rootserver . . .")
      //closeOnExit(servers) // call the close function - called before the game starts
      debug("[DEBUG] starting the fresh instance of the rootserver . . .")
      //servers.foreach(_.bind) // now start the servers. <rootserver>
    } catch {
      case e: Throwable =>
        error("fatal", e)
        throw e
    }
  }

  private[this] def writePID(destPath: String) = {
    def pid(fallback: String) = {
      val jvmName = ManagementFactory.getRuntimeMXBean.getName
      val index = jvmName indexOf '@'
      if (index > 0) {
        try {
          jvmName.substring(0, index).toLong.toString
        } catch {
          case e: NumberFormatException ⇒ fallback
        }
      } else fallback
    }

    val pidFile = new File(destPath)
    if (pidFile.createNewFile) {
      (new PrintWriter(pidFile) append pid("<Unknown-PID>")).close()
      pidFile.deleteOnExit()
      info("pid file: " + destPath)
      true
    } else {
      error("unable to write pid file, exiting.")
      System exit 1
      false
    }
  }

  private[this] def closeOnExit(closeables: Seq[Closeable]) = {
    Runtime.getRuntime addShutdownHook new Thread {
      override def run() = {
        try {
          info("jvm killed")
          closeables.foreach(_.close)
        } catch {
          case e: Throwable => error("shutdown hook failure", e)
        }
      }
    }
  }
}
