package com.askme.ramanujan.actors

import spray.routing.HttpServiceActor
import akka.actor.Actor
import grizzled.slf4j.Logging
import com.askme.ramanujan.Configurable
import com.askme.ramanujan.util.CORS
import spray.json.DefaultJsonProtocol
import spray.routing.authentication.BasicAuth
import com.askme.ramanujan.util.DataSource
import akka.actor.Props
import scala.concurrent.ExecutionContext.Implicits.global
import spray.json._
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.routing.Directive.pimpApply
import spray.routing.directives.AuthMagnet.fromContextAuthenticator

import com.google.common.io.{ByteStreams, Files}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import com.typesafe.config.Config
import java.util.concurrent.Executors
import spray.routing.authentication.UserPass
import spray.routing.authentication.UserPassAuthenticator
import spray.routing.authentication.BasicUserContext
import scala.concurrent.Future
import scala.concurrent.Promise

class ApiHandler(val config: Config, val conf: SparkConf, val sqlContext: SQLContext, val listener: ActorRef) extends HttpServiceActor with Actor with Logging with Configurable with CORS with Serializable{

      case class RequestObject(host: String, port: String, user: String, password: String, db: String, table: String, bookmark: String, bookmarkformat: String, primaryKey: String, conntype: String,fullTableSchema: String)
      object RequestJsonProtocol extends DefaultJsonProtocol {
		    implicit val RequestFormat = jsonFormat11(RequestObject)
	    }
	    import RequestJsonProtocol._
	    
	    object UsernameEqualsPasswordAuthenticator extends UserPassAuthenticator[BasicUserContext] {

      override def apply(userPass: Option[UserPass]): Future[Option[BasicUserContext]] = {
        val basicUserContext =
          userPass flatMap {
            case UserPass(username, password) if username == password => Some(BasicUserContext(username))
            case _ => None
          }
          Promise.successful(basicUserContext).future
        }
      }

      override final def receive: Receive = runRoute{
	      authenticate(BasicAuth(UsernameEqualsPasswordAuthenticator,"Ramanujan"))(user =>
	        path("")(
	            getFromResource(s"web/static/index.html")
	        )~
	        post(
	        pathPrefix("api" / "report")(
	            anyParam('report.as[String]){report =>
                     complete {
                       getReport(conf,sqlContext,report) // assuming that HDFS locations and tables are all Fine ! HDFS locs are always main table + partitionID
                     }
	            }
	        )~
	        pathPrefix("api" / "request")(
	            rawJson // take rawJson Only, no need for Entity[as.***] things
	                {
	                  json => 
	                  complete {
	                     relay(json) // convert this json into a requestobject and then datasource and then process it and also put in DB requests
	                     persistInDB(json) // dont know ; nature should be void only
	                  }
	          }
	        )
	        )~
	        getFromResourceDirectory("web/static")
	      /*
	           post{
            pathPrefix("\""+string("handler.baseurl")+"\"" / "\""+string("handler.request")+"\"")(
                  rawJson // take rawJson Only, no need for Entity[as.***] things
	                {
	                  json => 
	                  complete {
	                     relay(json) // convert this json into a requestobject and then datasource and then process it and also put in DB requests
	                     persistInDB(json) // dont know ; nature should be void only
	                  }
	                }
           )
        }~
        get{
          pathPrefix(string("handler.baseurl")) {
            compressResponse() {
              getFromResourceDirectory("web/static")
            }
          }~
          pathPrefix(string("handler.report")){
	          anyParam('report.as[String]){report =>
                     complete {
                       getReport(conf,sqlContext,report) // assuming that HDFS locations and tables are all Fine ! HDFS locs are always main table + partitionID
                     }
	              }
	        }
        }
	      get{
	        pathPrefix(string("handler.report"))(
                 anyParam('report.as[String]){report =>
                     complete {
                       getReport(conf,sqlContext,report) // assuming that HDFS locations and tables are all Fine ! HDFS locs are always main table + partitionID
                     }
	              }
            )
	      }
	      get{
          authenticate(BasicAuth(string("handler.appname")))(user => // get rid of these errors somehow + BasicAuth
            path(string("handler.baseurl"))( // meaning the default path, as in the first , very first thing that gets served over
                getFromResource(string("handler.index.html"))~
                (
                   getFromResourceDirectory("web/static")   
                )
            )~
            pathPrefix(string("handler.report"))(
                 anyParam('report.as[String]){report =>
                     complete {
                       getReport(conf,sqlContext,report) // assuming that HDFS locations and tables are all Fine ! HDFS locs are always main table + partitionID
                     }
	              }
            )
         )
	      }
	      * 
	      */
	      )
	    }
      override def postStop = { // do not know what for . . .
        info("Kill Message received")
      }

      def relay(request: String) = {
        debug("[DEBUG] [REQUEST] [API] request received == "+request)
        case class RequestObject(host: String, port: String, user: String, password: String, db: String, table: String, bookmark: String, bookmarkformat: String, primaryKey: String, conntype: String,fullTableSchema: String)

    		object RequestJsonProtocol extends DefaultJsonProtocol {
		  	  implicit val RequestFormat = jsonFormat11(RequestObject)
		    }
    		import RequestJsonProtocol._
    		val jsonValue = request.parseJson
    		debug("[DEBUG] [REQUEST] [API] the json value == "+jsonValue)

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
				val dataSource = new DataSource(config,conntype,host,port,user,password,db,table,bookmark,bookmarkformat,primaryKey,fullTableSchema)
		    // the process stays the same - be that for DB requests or API requests
    		//val sqlActor = context.actorOf(Props(classOf[SQLActor],config,sqlContext)) // new SQL Actor
    		//val streamingActor = context.actorOf(Props(classOf[StreamingActor],config,conf,sqlContext)) // new streaming Actor
		    //sqlActor ! new SQL(config,dataSource)
		    //streamingActor ! new Streaming(config,dataSource)
		    //listener ! "Having scheduled actors / workers  for : "+dataSource.db+" =and= "+dataSource.table
    		debug("[DEBUG] [REQUEST] [API] the dataSource formed == "+dataSource.toString())
    		import scala.concurrent._ // brute force imported
    	val numberOfCPUs = sys.runtime.availableProcessors() // brute force imported
	    val threadPool = Executors.newFixedThreadPool(numberOfCPUs) // brute force imported
	    implicit val ec = ExecutionContext.fromExecutorService(threadPool) // brute force imported
	    debug("[DEBUG] [REQUEST] [API] doing the SQL-SQL flow for == "+dataSource.db+"_"+dataSource.table)
	    debug("[DEBUG] [REQUEST] [API] obtaining the previous, the current and the affected PKs")
	    var prevBookMark = dataSource.getPrevBookMark()
	    debug("[DEBUG] [REQUEST] [API] the previous bookmark == "+prevBookMark)
	    var currBookMark = dataSource.getCurrBookMark(sqlContext)
	    debug("[DEBUG] the current bookmark == "+currBookMark)
	    var PKsAffectedDF = dataSource.getAffectedPKs(sqlContext,prevBookMark,currBookMark)
	    debug("[DEBUG] [REQUEST] [API] the count of PKs returned == "+PKsAffectedDF.count())
	    if(PKsAffectedDF.rdd.isEmpty()){
		      info("[SQL] no records to upsert in the internal Status Table == for bookmarks : "+prevBookMark+" ==and== "+currBookMark+" for table == "+dataSource.db+"_"+dataSource.table)
		  }
    	else{
		      debug("[DEBUG] [REQUEST] [API] mapping the rows of affected PKs one by one / batch maybe onto STATUS table . . .")
		      PKsAffectedDF.map { x => dataSource.upsert(x(0).toString(),x(1).toString()) } // assuming x(0) / x(1) converts to string with .toString() | insert db / table / primaryKey / sourceId / 0asTargetId
		  }
    	dataSource.updateBookMark(currBookMark)
    	
    	dataSource.getPKs4UpdationKafka(conf,sqlContext)
    	dataSource.getPKs4UpdationHDFS(conf,sqlContext)
    	
	    /*
	    Future {
		   /*
		 		* 1. get previous offset - incr. id / bookmark table -> previous timestamp / bookmark
		 		* 2. get current offset - timestamp / source table -> current timestamp / bookmark
		 		* 3. get rows between - 1. and 2. -> distinct PKs, max(timestamp / bookmark)
		 		* 4. upsert the things from 3.
		 		* 5. current offset is appended to the bookmark table
				*/
    		debug("[DEBUG] [REQUEST] [API] doing the SQL-SQL flow for == "+dataSource.db+"_"+dataSource.table)
		    debug("[DEBUG] [REQUEST] [API] obtaining the previous, the current and the affected PKs")
        var prevBookMark = dataSource.getPrevBookMark() // get the previous bookmark - key,max(autoincr.) group by key filter key -> bookmark / ts | db.internal.tables.bookmarks.defs.defaultBookMarkValue for first time
		    debug("[DEBUG] [REQUEST] [API] the previous bookmark == "+prevBookMark)
        var currBookMark = dataSource.getCurrBookMark(sqlContext) // get the latest / maximum bookmark / ts | db.internal.tables.bookmarks.defs.defaultBookMarkValue for first time
		    debug("[DEBUG] the current bookmark == "+currBookMark)
        var PKsAffectedDF = dataSource.getAffectedPKs(sqlContext,prevBookMark,currBookMark) // distinct PKs and max(timestamp) + WHERE clause | same as PKs, timestamp if not a log table, and PKs / timestamp otherwise | could be empty also
		    debug("[DEBUG] [REQUEST] [API] the count of PKs returned == "+PKsAffectedDF.count())
        if(PKsAffectedDF.rdd.isEmpty()){
		      info("[SQL] no records to upsert in the internal Status Table == for bookmarks : "+prevBookMark+" ==and== "+currBookMark+" for table == "+dataSource.db+"_"+dataSource.table)
		    }
		    else{
		      debug("[DEBUG] [REQUEST] [API] mapping the rows of affected PKs one by one / batch maybe onto STATUS table . . .")
		      PKsAffectedDF.map { x => dataSource.upsert(x(0).toString(),x(1).toString()) } // assuming x(0) / x(1) converts to string with .toString() | insert db / table / primaryKey / sourceId / 0asTargetId
		    }
		    dataSource.updateBookMark(currBookMark) // update the bookmark table - done !
	      }(ExecutionContext.Implicits.global) onSuccess { // future onsuccess check
	        case _ => info("[SQL] [COMPLETED] =/="+dataSource.toString()) // ack message on success
	      }
	      debug("[DEBUG] [REQUEST] [API] [SQL] still executing == "+dataSource.toString())
	      info("[SQL] [EXECUTING] =/="+dataSource.toString())
	      //sender ! new SQLExecuting(config,request)
	      Future {
		      dataSource.getPKs4UpdationKafka(conf,sqlContext) // or straightaway and get rid of the wrapper func pollAndSink - KAFKA
		      dataSource.getPKs4UpdationHDFS(conf,sqlContext) // or straightaway and get rid of the wrapper func pollAndSink - HDFS
	      }(ExecutionContext.Implicits.global) onSuccess {
	        case _ => info("[STREAMING] [COMPLETED] =/="+dataSource.toString()) // ack message on success
	      }
	      debug("[DEBUG] [REQUEST] [API] [STREAMING] still executing == "+dataSource.toString())
    	  info("[STREAMING] [EXECUTING] =/="+dataSource.toString())
		    */
      }

	    def persistInDB(request: String) = {
	      case class RequestObject(host: String, port: String, user: String, password: String, db: String, table: String, bookmark: String, bookmarkformat: String, primaryKey: String, conntype: String,fullTableSchema: String)

    		object RequestJsonProtocol extends DefaultJsonProtocol {
		  	  implicit val RequestFormat = jsonFormat11(RequestObject)
		    }
    		import RequestJsonProtocol._
    		val jsonValue = request.parseJson

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
				val dataSource = new DataSource(config,conntype,host,port,user,password,db,table,bookmark,bookmarkformat,primaryKey,fullTableSchema)
        val  json = "{ \"host\": \""+host+"\",\"port\" : \""+port+"\",\"user\":\""+user+"\",\"password\":\""+password+"\",\"db\":\""+db+"\",\"table\":\""+table+"\",\"bookmark\":\""+bookmark+"\",\"bookmarkformat\":\""+bookmarkformat+"\",\"primaryKey\":\""+primaryKey+"\",\"conntype\":\""+conntype+"\",\"fullTableSchema\":\""+fullTableSchema+"\" }"
        dataSource.insertInitRow(json) // or persist the request thing itself.
        "persisted"
      }

	    def rawJson = extract { _.request.entity.asString}

  def getReport(conf:SparkConf,sqlContext: SQLContext,json: String) = {
        val reportActor = context.actorOf(Props(new ReportActor(conf,sqlContext)))
        reportActor ! json
        "report started"
    }

  def nothing(json: String) = {
        "Nothing to prove, no report to run !!! "+json
      }
}