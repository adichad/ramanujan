package com.askme.ramanujan.actors

import akka.actor.ActorRef
import akka.actor.Actor
import spray.json.DefaultJsonProtocol
import org.apache.spark.sql.Row
import com.askme.ramanujan.Configurable
import akka.actor.Props
import com.askme.ramanujan.util.DataSource

import spray.json._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.typesafe.config.Config

//message formats for Akka Actors
sealed trait PipeMessage
case class StartPipeline(val config: Config,row:Row) extends PipeMessage with Configurable with Serializable {require(!row.isNullAt(row.fieldIndex("db.internal.tables.requests.cols.request")),"row cannot contain null table request values")}// a new pipeline is started for each row
case class SQL(val config: Config,dataSource:DataSource) extends PipeMessage with Configurable with Serializable {require(!(dataSource.table.isEmpty()), "table field cannot be empty");require(!(dataSource.db.isEmpty()), "db field cannot be empty")}// SQL actor gets a datasource object to consume
case class Streaming(val config: Config,dataSource: DataSource) extends PipeMessage with Configurable with Serializable {require(!(dataSource.table.isEmpty()), "table field cannot be empty");require(!(dataSource.db.isEmpty()), "db field cannot be empty")}// Streaming actor gets a datasource object to sink to kafka / hdfs
case class SQLack(val config: Config,val message: DataSource) extends PipeMessage with Configurable with Serializable {require(!(message.table.isEmpty()), "table field cannot be empty");require(!(message.db.isEmpty()), "db field cannot be empty")}// acknowledged that status for this table was updated
case class Streamingack(val config: Config,val message: DataSource) extends PipeMessage with Configurable with Serializable {require(!(message.table.isEmpty()), "table field cannot be empty");require(!(message.db.isEmpty()), "db field cannot be empty")}// acknowledged that streaming for this table was done
case class SQLExecuting(val config: Config,val dataSource: DataSource) extends PipeMessage with Configurable with Serializable {require(!(dataSource.table.isEmpty()), "table field cannot be empty");require(!(dataSource.db.isEmpty()), "db field cannot be empty")}// still executing the SQL-SQL request
case class StreamingExecuting(val config: Config,val dataSource: DataSource) extends PipeMessage with Configurable with Serializable {require(!(dataSource.table.isEmpty()), "table field cannot be empty");require(!(dataSource.db.isEmpty()), "db field cannot be empty")} // still executing the SQL-Sink request

class RequestHandler(val config: Config,val conf: SparkConf,val listener: ActorRef,val sqlContext:SQLContext) extends Actor with Configurable with Serializable{
		  def receive = {
		    case StartPipeline(config,row) => // row.request must not be empty at all+pass config to reflect the Configurable Sugar Methods
		      val sc = SparkContext.getOrCreate(conf) // ideal method
		      val dataSource = transform(config,row) // just a data source off the json request+pass config to reflect the Configurable Sugar Methods
		      val sqlActor = context.actorOf(Props(classOf[SQLActor],config,sqlContext)) // new SQL actor
		      val streamingActor = context.actorOf(Props(classOf[StreamingActor],config,conf,sqlContext)) // new streaming Actor
		      sqlActor ! new SQL(config,dataSource) 
		      streamingActor ! new Streaming(config,dataSource)
		      listener ! "[SQL] Having scheduled actors / workers  for : "+dataSource.db+" =and= "+dataSource.table
		      case SQLack(config,message) =>
		      listener ! "[SQL] [COMPLETED]=/= "+message.toString()
		      case SQLExecuting(config,message) =>
		      listener ! "[SQL] [EXECUTING] =/= "+message.toString()
		      case Streamingack(config,message) =>
		      listener ! "[STREAMING] [COMPLETED]=/= "+message.toString()
		      case StreamingExecuting(config,message) =>
		      listener ! "[STREAMING] [EXECUTING] =/= "+message.toString()
		  }
		  def transform(config: Config,row: Row) = {
		    case class RequestObject(host: String, port: String, user: String, password: String, db: String, table: String, bookmark: String, bookmarkformat: String, primaryKey: String, conntype: String,fullTableSchema: String)

    		object RequestJsonProtocol extends DefaultJsonProtocol {
		  	  implicit val RequestFormat = jsonFormat11(RequestObject)
		    }
    		import RequestJsonProtocol._
		    val request = row.getString(row.fieldIndex("db.internal.tables.requests.cols.request")) // carries the request string in request db
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
		    dataSource
		  }
	  }