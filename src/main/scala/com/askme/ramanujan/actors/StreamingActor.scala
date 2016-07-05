package com.askme.ramanujan.actors

import akka.actor.Actor
import java.util.concurrent.Executors
import com.askme.ramanujan.util.DataSource
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.typesafe.config.Config

class StreamingActor(val config: Config,val conf: SparkConf,val sqlContext: SQLContext) extends Actor with Serializable{
	// shall take responsibilities for a table request : x | SQL - Sinks
	def receive = {
	case Streaming(config,request) =>
	import scala.concurrent._
	val numberOfCPUs = sys.runtime.availableProcessors()
	val threadPool = Executors.newFixedThreadPool(numberOfCPUs)
	implicit val ec = ExecutionContext.fromExecutorService(threadPool)
	Future {
		pollAndSink(conf,request)
	}(ExecutionContext.Implicits.global) onSuccess {
	case _ => sender ! new Streamingack(config,request)
	}
	sender ! new StreamingExecuting(config,request)
	}

	def pollAndSink(conf: SparkConf,request: DataSource) = {
		/*
		 * 1. for the keys associated with this data source's table WHERE sourcebkmrk > targetbkmrk -> DF <db,table,key,source> => <key,source>
		 * 2. on this data frame . foreach -> publishToKafka + (aggregated) write to hdfs
		 * 3. update Status table (ensuring that both kafka and hdfs succeed! - not either of these | also update might fail because it gets overwritten
		 */
		// need this every now and then
		request.getPKs4UpdationKafka(conf,sqlContext) // or straightaway and get rid of the wrapper func pollAndSink - KAFKA
		request.getPKs4UpdationHDFS(conf,sqlContext) // or straightaway and get rid of the wrapper func pollAndSink - HDFS 
	}
}