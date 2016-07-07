package com.askme.ramanujan.server

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import com.askme.ramanujan.util.DataSource
import akka.actor.ActorSystem
import akka.actor.Props
import com.askme.ramanujan.actors.Listener
import grizzled.slf4j.Logging
import com.askme.ramanujan.actors.RequestHandler
import com.askme.ramanujan.actors.ApiHandler
import akka.util.Timeout
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import scala.concurrent.duration.Duration
import com.askme.ramanujan.actors.StartPipeline
import com.typesafe.config.Config
import com.askme.ramanujan.Configurable
import java.sql.DriverManager
import spray.json.DefaultJsonProtocol
import spray.json._
import java.util.concurrent.Executors
import com.askme.ramanujan.util.DataSource
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.sql.SaveMode
import java.util.Properties
import org.apache.spark.sql.functions.udf

class RootServer(val config: Config) extends Logging with Configurable with Server with Serializable {
    // normal sparkContext - initialize it from conf.spark
    debug("[DEBUG] initialize the conf for spark and the spark context . . .")
		val conf = sparkConf("spark")
		val sc = SparkContext.getOrCreate(conf) // ideally this is what one must do - getOrCreate
		// one sqlContext - to pass on
		val sqlContext = new SQLContext(sc)
	  //create an actorSystem
		info("creating the actor system == "+string("actorSystem.name"))
	  private implicit val pipelineSystem = ActorSystem(string("actorSystem.name"))
	  // the listener, an option sys log class basically - one listener only
	  info("creating the listener actor == "+string("actorSystem.actors.listener"))
		val listener = pipelineSystem.actorOf(Props(classOf[Listener],config), name = string("actorSystem.actors.listener"))
		// the master class
		info("creating the master actor == "+string("actorSystem.actors.master"))
		val requestHandlerRef = pipelineSystem.actorOf(Props(classOf[RequestHandler],config,conf,listener,sqlContext), name = string("actorSystem.actors.master"))
		// take a table in - completely? 
		/*
		 * COMMENTING THE GENERAL SPARK SQL APPROACH - USE A DB CONN INSTEAD.
		 * debug("[DEBUG]creating requestsRecordsDB")
		var requestsRecordsDB = sqlContext.read.format(string("db.conn.jdbc")).option("url", string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+string("db.internal.url")+"/"+string("db.internal.dbname")).option("driver",string("db.internal.driver"))
		.option("dbtable",string("db.internal.tables.requests.name")).option("user",string("db.internal.user")).option("password",string("db.internal.password"))
		.load()
		*   
		*/
		debug("[DEBUG] the internal db connections evoked . . .")
		val internalHost = string("db.internal.url") // get the host from env confs - tables bookmark & status mostly
		val internalPort = string("db.internal.port") // get the port from env confs - tables bookmark & status mostly
		val internalDB = string("db.internal.dbname") // get the port from env confs - tables bookmark & status mostly
		val internalUser = string("db.internal.user") // get the user from env confs - tables bookmark & status mostly
		val internalPassword = string("db.internal.password") // get the password from env confs - tables bookmark & status mostly
		
		val internalURL: String = (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+internalHost+":"+internalPort+"/"+internalDB) // the internal connection DB <status, bookmark, requests> etc
		
		var connection = DriverManager.getConnection(internalURL, internalUser, internalPassword)
		debug("[DEBUG] the initial internal db connection invoked . . .")
		val statement = connection.createStatement()
		// get the "0" status requests - them all basically
		debug("[DEBUG] [Requests table query] SELECT * FROM "+string("db.internal.tables.requests.name")+" WHERE "+string("db.internal.tables.requests.cols.status")+" in (\""+string("db.internal.tables.requests.defs.defaultStatusVal")+"\")")
		val resultSet = statement.executeQuery("SELECT * FROM "+string("db.internal.tables.requests.name")+" WHERE "+string("db.internal.tables.requests.cols.status")+" in (\""+string("db.internal.tables.requests.defs.defaultStatusVal")+"\")")
		
		while(resultSet.next()){
		  val processDate = resultSet.getString(string("db.internal.tables.requests.cols.processDate"))
		  val request = resultSet.getString(string("db.internal.tables.requests.cols.request"))
		  val status = resultSet.getString(string("db.internal.tables.requests.cols.status"))
		  debug("[DEBUG] the table as a request == "+request)
		  val dataSource = transform(request)
		  import scala.concurrent._ // brute force imported
    	val numberOfCPUs = sys.runtime.availableProcessors() // brute force imported
	    val threadPool = Executors.newFixedThreadPool(numberOfCPUs) // brute force imported
	    debug("[DEBUG] the converted datasource object == "+dataSource.toString())
	    implicit val ec = ExecutionContext.fromExecutorService(threadPool) // brute force imported
	    debug("[DEBUG] doing the SQL-SQL flow for == "+dataSource.db+"_"+dataSource.table)
	    debug("[DEBUG] obtaining the previous, the current and the affected PKs")
	    var prevBookMark = dataSource.getPrevBookMark()
	    debug("[DEBUG] the previous bookmark == "+prevBookMark)
	    var currBookMark = dataSource.getCurrBookMark(sqlContext)
	    debug("[DEBUG] the current bookmark == "+currBookMark)
	    var PKsAffectedDF: DataFrame = dataSource.getAffectedPKs(sqlContext,prevBookMark,currBookMark)
	    debug("[DEBUG] the count of PKs returned == "+PKsAffectedDF.count())
	    if(PKsAffectedDF.rdd.isEmpty()){
		      info("[SQL] no records to upsert in the internal Status Table == for bookmarks : "+prevBookMark+" ==and== "+currBookMark+" for table == "+dataSource.db+"_"+dataSource.table)
		  }
		  else{
		      /*
		       * because this Option 1 is giving a NotSerializationException
		       * 
		      debug("[DEBUG] mapping the rows of affected PKs one by one / batch maybe onto STATUS table . . .")
		      val upserter = new Upserter(internalURL, internalUser, internalPassword)
		      PKsAffectedDF.map { x => upserter.upsert(x(0).toString(),x(1).toString(),dataSource.db,dataSource.table) } // assuming x(0) / x(1) converts to string with .toString() | insert db / table / primaryKey / sourceId / 0asTargetId
		      */
		      // trying the upsert workaround here.
		      val statusRecordsDB = sqlContext.read.format(string("db.conn.jdbc")).option("url", string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+string("db.internal.url")+"/"+string("db.internal.dbname")).option("driver",string("db.internal.driver"))
		      .option("dbtable",string("db.internal.tables.status.name")).option("user",string("db.internal.user")).option("password",string("db.internal.password"))
		      .load()
		      val prop: java.util.Properties = new Properties()
		      prop.setProperty("user",string("db.internal.user"))
		      prop.setProperty("password", string("db.internal.password"))
		      if(statusRecordsDB.rdd.isEmpty()){
		        // straightaway write the PKsAffectedDF dataframe into status table, overriding it.
		        PKsAffectedDF.write.mode(SaveMode.Overwrite).jdbc(internalURL,string("db.internal.tables.status.name"), prop)
		      }
		      else{
		        val affectedPKs = PKsAffectedDF.select(string("db.internal.tables.status.cols.qualifiedName")).rdd.map(r => r(0).asInstanceOf[String]).collect()
		        val sc = SparkContext.getOrCreate(conf)
		        val affectedPKsBrdcst = sc.broadcast(affectedPKs)
		        
		        val func1a: (String => Boolean) = (arg: String) => !affectedPKsBrdcst.value.contains(arg)
		        //val func1b: (String => Boolean) = (arg: String) => affectedPKsBrdcst.value.contains(arg)
		        val sqlfunc1a = udf(func1a)
		        //val sqlfunc1b = udf(func1b)
		        val statusRecordsDB_1a = statusRecordsDB.filter(sqlfunc1a(new org.apache.spark.sql.Column(string("db.internal.tables.status.cols.qualifiedName"))))
		        //val statusRecordsDB_1b = statusRecordsDB.filter(sqlfunc1b(new org.apache.spark.sql.Column(string("db.internal.tables.status.cols.qualifiedName"))))
		        //statusRecordsDB_1a.write.mode(SaveMode.Overwrite).jdbc(internalURL,"chawl_a", prop)
		        //statusRecordsDB_1b.write.mode(SaveMode.Overwrite).jdbc(internalURL,"chawl_b", prop)
		        // upsert work-around
		        //val statusRecordsDB_1 = statusRecordsDB.filter("$\""+string("db.internal.tables.status.cols.qualifiedName")+"\" not in PKsAffectedDF(\""+string("db.internal.tables.status.cols.qualifiedName")+"\")")
		        //val statusRecordsDB_2 = statusRecordsDB_1.unionAll(PKsAffectedDF)
		        //val statusRecordsDB_1 = statusRecordsDB.join(PKsAffectedDF, statusRecordsDB(string("db.internal.tables.status.cols.qualifiedName")) !== PKsAffectedDF(string("db.internal.tables.status.cols.qualifiedName")))
		        val statusRecordsDB_2a = PKsAffectedDF.unionAll(statusRecordsDB_1a)
		        statusRecordsDB_2a.write.mode(SaveMode.Overwrite).jdbc(internalURL,string("db.internal.tables.statustmp.name"), prop)
		        
		        val internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword)
		        val statement = internalConnection.createStatement()
		        val status_to_tmp_query = "ALTER TABLE "+string("db.internal.tables.status.name")+" RENAME swap_status;"
		        val tmp_to_status_query = "ALTER TABLE "+string("db.internal.tables.statustmp.name")+" RENAME "+string("db.internal.tables.status.name")+";"
		        val drop_swap_status_query = "drop table swap_status";
		        statement.executeUpdate(status_to_tmp_query)
		        statement.executeUpdate(tmp_to_status_query)
		        statement.executeUpdate(drop_swap_status_query)
		        //val statusRecordsDB_2b = statusRecordsDB_1b.unionAll(PKsAffectedDF)
		        //statusRecordsDB_2b.write.mode(SaveMode.Overwrite).jdbc(internalURL,"phatuu", prop)
		      }
		  }
		  debug("[DEBUG] updating the current bookmark for this db + table into BOOKMARKS table . . .")
		  dataSource.updateBookMark(currBookMark)
		  
		  dataSource.getPKs4UpdationKafka(conf,sqlContext)
		  dataSource.getPKs4UpdationHDFS(conf,sqlContext)
		  
	    /*
	     * Getting rid of the futures too, for the time being . . .
	     * 
	    Future {
		   /*
		 		* 1. get previous offset - incr. id / bookmark table -> previous timestamp / bookmark
		 		* 2. get current offset - timestamp / source table -> current timestamp / bookmark
		 		* 3. get rows between - 1. and 2. -> distinct PKs, max(timestamp / bookmark)
		 		* 4. upsert the things from 3.
		 		* 5. current offset is appended to the bookmark table
				*/
		    debug("[DEBUG] doing the SQL-SQL flow for == "+dataSource.db+"_"+dataSource.table)
		    debug("[DEBUG] obtaining the previous, the current and the affected PKs")
        var prevBookMark = dataSource.getPrevBookMark() // get the previous bookmark - key,max(autoincr.) group by key filter key -> bookmark / ts | db.internal.tables.bookmarks.defs.defaultBookMarkValue for first time
		    debug("[DEBUG] the previous bookmark == "+prevBookMark)
        var currBookMark = dataSource.getCurrBookMark(sqlContext) // get the latest / maximum bookmark / ts | db.internal.tables.bookmarks.defs.defaultBookMarkValue for first time
		    debug("[DEBUG] the current bookmark == "+currBookMark)
        var PKsAffectedDF = dataSource.getAffectedPKs(sqlContext,prevBookMark,currBookMark) // distinct PKs and max(timestamp) + WHERE clause | same as PKs, timestamp if not a log table, and PKs / timestamp otherwise | could be empty also
		    debug("[DEBUG] the count of PKs returned == "+PKsAffectedDF.count())
        if(PKsAffectedDF.rdd.isEmpty()){
		      info("[SQL] no records to upsert in the internal Status Table == for bookmarks : "+prevBookMark+" ==and== "+currBookMark+" for table == "+dataSource.db+"_"+dataSource.table)
		    }
		    else{
		      debug("[DEBUG] mapping the rows of affected PKs one by one / batch maybe onto STATUS table . . .")
		      PKsAffectedDF.map { x => dataSource.upsert(x(0).toString(),x(1).toString()) } // assuming x(0) / x(1) converts to string with .toString() | insert db / table / primaryKey / sourceId / 0asTargetId
		    }
		    debug("[DEBUG] updating the current bookmark for this db + table into BOOKMARKS table . . .")
		    dataSource.updateBookMark(currBookMark) // update the bookmark table - done !
	      }(ExecutionContext.Implicits.global) onSuccess { // future onSuccess check
	        case _ => info("[SQL] [COMPLETED] =/="+dataSource.toString()) // ack message on success
	      }
	      debug("[DEBUG] [SQL] still executing == "+dataSource.toString())
	      info("[SQL] [EXECUTING] =/="+dataSource.toString())
	      //sender ! new SQLExecuting(config,request)
	      Future {
		      dataSource.getPKs4UpdationKafka(conf,sqlContext) // or straightaway and get rid of the wrapper func pollAndSink - KAFKA
		      dataSource.getPKs4UpdationHDFS(conf,sqlContext) // or straightaway and get rid of the wrapper func pollAndSink - HDFS
	      }(ExecutionContext.Implicits.global) onSuccess {
	        case _ => info("[STREAMING] [COMPLETED] =/="+dataSource.toString()) // ack message on success
	      }
	      debug("[DEBUG] [STREAMING] still executing == "+dataSource.toString())
    	  info("[STREAMING] [EXECUTING] =/="+dataSource.toString())
    	  */
		}
    debug("[DEBUG] closing down the internal connection . . .")
		connection.close()
		//debug("[DEBUG] requestsRecordsDB.count() == "+requestsRecordsDB.count())
		// getting just those rows which were 0 i.e not even SQL persistence  (commenting) - filter
		//requestsRecordsDB = requestsRecordsDB.where(requestsRecordsDB(string("db.internal.tables.requests.cols.status"))==="\""+string("db.internal.tables.requests.defs.defaultStatusVal")+"\"")// make sure to put status as 0 and update them also.
		//debug("[DEBUG] filtered ....")
		//debug("[DEBUG] records after filtering on status == "+requestsRecordsDB.count())
		// for all the records fetched above, parse the requests and send them to actors etc.
		//debug("[DEBUG] Sending the records one by one to the Pipeline . . .")
		//requestsRecordsDB.foreach { 
		  // to avoid the Serialization Exceptions
		  //row  => requestHandlerRef ! new StartPipeline(config,row) 
		//}
		// the init process i.e that reads from the db is complete
		debug("[DEBUG] starting the api service actor . . .")
		private val api = pipelineSystem.actorOf(Props(classOf[ApiHandler],config,conf,sqlContext,listener), name = string("actorSystem.actors.service"))
		private implicit val timeout = Timeout(Duration.apply(int("handler.timeout.scalar"), string("handler.timeout.unit")))//from root.server
    debug("[DEBUG] on for the transport IO(Http) Actor . . .")
		private val transport = IO(Http)
    override def bind {
      debug("[DEBUG] binding the api service actor to 127.0.0.1:9999")
      transport ! Http.Bind(api, interface = string("host"), port = int("port"))
      info("server bound: " + string("host") + ":" + int("port"))
    }
	  
	  override def close() {
	    debug("[DEBUG] time to shut the api service actor and the entire system / rootserver down. . .")
      transport ? Http.Unbind
      pipelineSystem.stop(api)
      pipelineSystem.shutdown()
      info("server shutdown complete: " + string("host") + ":" + int("port"))
    }

  def transform(request: String) = {
		    case class RequestObject(host: String, port: String, user: String, password: String, db: String, table: String, bookmark: String, bookmarkformat: String, primaryKey: String, conntype: String,fullTableSchema: String)

    		object RequestJsonProtocol extends DefaultJsonProtocol {
		  	  implicit val RequestFormat = jsonFormat11(RequestObject)
		    }
    		import RequestJsonProtocol._
		    val jsonValue = request.parseJson
		    debug("[DEBUG] [SQL] [transform json value] == "+jsonValue)

    		val requestObject = jsonValue.convertTo[RequestObject]

				val host = requestObject.host // e.g 10.0.16.98
				val port = requestObject.port // e.g 3306
				val user = requestObject.user // e.g jyotishree
				val password = requestObject.password // e.g getit1234
				val db = requestObject.db // e.g payments
				val table = requestObject.table // e.g txn_tab
				val bookmark = requestObject.bookmark // bookmark key, e.g timestamp
				val bookmarkformat = requestObject.bookmarkformat // format of bookmark e.g dd-mm-YYYYTHH:MM:SSZ
				val primaryKey = requestObject.primaryKey // primaryKey
				val conntype = requestObject.conntype // e.g com.sql.MySQL.driver
				val fullTableSchema = requestObject.fullTableSchema // fully qualified table schema
				// create a datasource to connect for the above request / json / x
				debug("[DEBUG] [SQL] instantiating the dataSource . . .")
				val dataSource = new DataSource(config,conntype,host,port,user,password,db,table,bookmark,bookmarkformat,primaryKey,fullTableSchema)
		    dataSource
		  }

  class Upserter(internalURL: String, internalUser: String, internalPassword: String) extends Serializable{
    lazy val internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword)
    def upsert(primarykey: String, bookmark: String, db: String, table: String) = {
       // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
				val statement = internalConnection.createStatement()
						val upsertQuery = "INSERT INTO `"+string("db.internal.tables.status.name")+"`("+string("db.internal.tables.status.cols.dbname")+","+string("db.internal.tables.status.cols.table")+",`"+string("db.internal.tables.status.cols.primarykey")+"`,"+string("db.internal.tables.status.cols.sourceId")+","+string("db.internal.tables.status.cols.kafkaTargetId")+string("db.internal.tables.status.cols.hdfsTargetId")+") VALUES( '"+db+"','"+table+"','"+primarykey+"', '"+bookmark+"',"+string("db.internal.tables.status.defs.defaultTargetId")+string("db.internal.tables.status.defs.defaultTargetId")+") ON DUPLICATE KEY UPDATE `"+string("db.internal.tables.status.cols.sourceId")+"` = '"+bookmark+"'"        
						val resultSet = statement.executeQuery(upsertQuery)
				internalConnection.close()
    }
  }
}