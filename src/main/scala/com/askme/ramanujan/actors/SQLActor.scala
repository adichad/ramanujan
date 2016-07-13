package com.askme.ramanujan.actors

import akka.actor.Actor
import java.util.concurrent.Executors
import com.askme.ramanujan.util.DataSource
import org.apache.spark.sql.SQLContext
import com.askme.ramanujan.Configurable
import com.typesafe.config.Config

class SQLActor(val config: Config,val sqlContext: SQLContext) extends Actor with Configurable{
	// shall take responsibilities for a table request : x | SQL - SQL
	def receive = {
	case SQL(config,request) => // receive a dataSource Object
	import scala.concurrent._ // brute force imported
	val numberOfCPUs = sys.runtime.availableProcessors() // brute force imported
	val threadPool = Executors.newFixedThreadPool(numberOfCPUs) // brute force imported
	implicit val ec = ExecutionContext.fromExecutorService(threadPool) // brute force imported
	Future {
		updateStatusTabs(request) // update the Status table
	}(ExecutionContext.Implicits.global) onSuccess { // future onsuccess check
	case _ => sender ! new SQLack(config,request) // ack message on success
	}
	sender ! new SQLExecuting(config,request) // execute the request
	}
	def updateStatusTabs(request: DataSource) = {
		/*
		 * 1. get previous offset - incr. id / bookmark table -> previous timestamp / bookmark
		 * 2. get current offset - timestamp / source table -> current timestamp / bookmark
		 * 3. get rows between - 1. and 2. -> distinct PKs, max(timestamp / bookmark)
		 * 4. upsert the things from 3.
		 * 5. current offset is appended to the bookmark table
		 */
		var prevBookMark = request.getPrevBookMark() // get the previous bookmark - key,max(autoincr.) group by key filter key -> bookmark / ts | db.internal.tables.bookmarks.defs.defaultBookMarkValue for first time
		var currBookMark = request.getCurrBookMark() // get the latest / maximum bookmark / ts | db.internal.tables.bookmarks.defs.defaultBookMarkValue for first time
		var PKsAffectedDF = request.getAffectedPKs(prevBookMark,currBookMark) // distinct PKs and max(timestamp) + WHERE clause | same as PKs, timestamp if not a log table, and PKs / timestamp otherwise | could be empty also
		if(PKsAffectedDF.rdd.isEmpty()){
		  //info("[SQL] no records to upsert in the internal Status Table == for bookmarks : "+prevBookMark+" ==and== "+currBookMark+" for table == "+request.db+"_"+request.table)
		}
		else{
		  PKsAffectedDF.map { x => request.upsert(x(0).toString(),x(1).toString()) } // assuming x(0) / x(1) converts to string with .toString() | insert db / table / primaryKey / sourceId / 0asTargetId
		}
		request.updateBookMark(currBookMark) // update the bookmark table - done !
	}
}