package com.askme.ramanujan.actors

import org.apache.spark.sql.functions.udf
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
import java.util.Properties
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Column
import java.sql.DriverManager
import org.apache.spark.sql.Row
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import kafka.producer.KeyedMessage
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import scala.collection.mutable.HashMap
import org.apache.log4j.Logger
/*
class ApiHandler(val config: Config) extends HttpServiceActor with Actor with Configurable with CORS with Serializable {

      object Holder extends Serializable {      
         @transient lazy val log = Logger.getLogger(getClass.getName)    
      }
  
      case class RequestObject(host: String, port: String, user: String, password: String, db: String, table: String, bookmark: String, bookmarkformat: String, primaryKey: String, conntype: String,fullTableSchema: String)
      object RequestJsonProtocol extends DefaultJsonProtocol with Serializable {
		    implicit val RequestFormat = jsonFormat11(RequestObject)
	    }
	    import RequestJsonProtocol._
	    
	    object UsernameEqualsPasswordAuthenticator extends Serializable with UserPassAuthenticator[BasicUserContext] {

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
                       getReport(config,report) // assuming that HDFS locations and tables are all Fine ! HDFS locs are always main table + partitionID
                     }
	            }
	        )~
	        pathPrefix("api" / "request")(
	            rawJson // take rawJson Only, no need for Entity[as.***] things
	                {
	                  json => 
	                  complete {
	                     //relay(json) // convert this json into a requestobject and then datasource and then process it and also put in DB requests
	                     persistInDB(json) // dont know ; nature should be void only
	                  }
	          }
	        )
	        )~
						getFromResourceDirectory("web/static")
	      )
	    }
      override def postStop = { // do not know what for . . .
        Holder.log.info("Kill Message received")
      }

      def relay(request: String) = {
        
        val internalHost = string("db.internal.url") // get the host from env confs - tables bookmark & status mostly
		    val internalPort = string("db.internal.port") // get the port from env confs - tables bookmark & status mostly
		    val internalDB = string("db.internal.dbname") // get the port from env confs - tables bookmark & status mostly
		    val internalUser = string("db.internal.user") // get the user from env confs - tables bookmark & status mostly
		    val internalPassword = string("db.internal.password") // get the password from env confs - tables bookmark & status mostly
		      	
		    val internalURL: String = (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+internalHost+":"+internalPort+"/"+internalDB) // the internal connection DB <status, bookmark, requests> etc
        
        Holder.log.debug("[MY DEBUG STATEMENTS] [REQUEST] [API] request received == "+request)
        case class RequestObject(host: String, port: String, user: String, password: String, db: String, table: String, bookmark: String, bookmarkformat: String, primaryKey: String, conntype: String,fullTableSchema: String)

    		object RequestJsonProtocol extends DefaultJsonProtocol {
		  	  implicit val RequestFormat = jsonFormat11(RequestObject)
		    }
    		import RequestJsonProtocol._
    		val jsonValue = request.parseJson
    		Holder.log.debug("[MY DEBUG STATEMENTS] [REQUEST] [API] the json value == "+jsonValue)

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
    		val conf = sparkConf("spark")
    		var sc = SparkContext.getOrCreate(conf)
    		var sqlContext = new SQLContext(sc);
		    
    		Holder.log.debug("[MY DEBUG STATEMENTS] [REQUEST] [API] the dataSource formed == "+dataSource.toString())
    		import scala.concurrent._ // brute force imported
    	val numberOfCPUs = sys.runtime.availableProcessors() // brute force imported
	    val threadPool = Executors.newFixedThreadPool(numberOfCPUs) // brute force imported
	    implicit val ec = ExecutionContext.fromExecutorService(threadPool) // brute force imported
	    Holder.log.debug("[MY DEBUG STATEMENTS] [REQUEST] [API] doing the SQL-SQL flow for == "+dataSource.db+"_"+dataSource.table)
	    Holder.log.debug("[MY DEBUG STATEMENTS] [REQUEST] [API] obtaining the previous, the current and the affected PKs")
	    var prevBookMark = dataSource.getPrevBookMark()
	    Holder.log.debug("[MY DEBUG STATEMENTS] [REQUEST] [API] the previous bookmark == "+prevBookMark)
	    var currBookMark = dataSource.getCurrBookMark()
	    Holder.log.debug("[MY DEBUG STATEMENTS] the current bookmark == "+currBookMark)
	    var PKsAffectedDF = dataSource.getAffectedPKs(prevBookMark,currBookMark)
	    Holder.log.debug("[MY DEBUG STATEMENTS] [REQUEST] [API] the count of PKs returned == "+PKsAffectedDF.count())
	    if(PKsAffectedDF.rdd.isEmpty()){
		      Holder.log.info("[SQL] no records to upsert in the internal Status Table == for bookmarks : "+prevBookMark+" ==and== "+currBookMark+" for table == "+dataSource.db+"_"+dataSource.table)
		  }
    	val PKsAffectedDF_json = PKsAffectedDF.toJSON
    	
    	PKsAffectedDF_json.foreachPartition { partitionOfRecords => {
    	    var props = new Properties()
    	    props.put("metadata.broker.list", string("sink.kafka.brokers"))//"kafka01.production.askmebazaar.com:9092,kafka02.production.askmebazaar.com:9092,kafka03.production.askmebazaar.com:9092")
		  	  props.put("serializer.class", string("sink.kafka.serializer"))//"kafka.serializer.StringEncoder")
			    props.put("group.id", string("sink.kafka.groupname"))//string("ramanujan"))
			    props.put("producer.type", string("sink.kafka.producer.type"))//xstring("async"))
    	    
			    val producer = new KafkaProducer[String,String](props)
			    
			    partitionOfRecords.foreach
                {
                    case x:String=>{
                        println(x)

                        val message=new ProducerRecord[String, String]("[TOPIC] "+db+"_"+table,db,x) //("output",null,x)
                        producer.send(message)
                    }
          }
			    
    	  }
    	}
    	/*
    	 * The status table upsert thing ignored.
    	 * 
    	else{
		      // trying the upsert workaround here.
		      val statusRecordsDB = sqlContext.read.format(string("db.conn.jdbc")).option("url", string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+string("db.internal.url")+"/"+string("db.internal.dbname")).option("driver",string("db.internal.driver"))
		      .option("dbtable",string("db.internal.tables.status.name")).option("user",string("db.internal.user")).option("password",string("db.internal.password"))
		      .load()
		      val prop: java.util.Properties = new Properties()
		      prop.setProperty("user",string("db.internal.user"))
		      prop.setProperty("password", string("db.internal.password"))
		      
		      Holder.log.debug("[MY DEBUG STATEMENTS] the internal db connections evoked . . .")
      				      
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
		      }
		  }
		  *
		  * straightaway dumping them into the sinks .. .. .. 
		  */
    	// INSERTING INTO KAFKA + HDFS 	
    	/*
    	dataSource.updateBookMark(currBookMark)
    	
    	val db_to_query = dataSource.db
    	val table_to_query = dataSource.table
    	
    	val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
					Map(
										"driver" -> string("db.internal.driver"),
										"url" -> (internalURL+"?user="+internalUser+"&password="+internalPassword),
										"dbtable" -> ("(select "+string("db.internal.tables.status.cols.primarykey")+","+string("db.internal.tables.status.cols.sourceId")+" from "+string("db.internal.tables.status.name")+" where "+string("db.internal.tables.status.cols.dbname")+" in (\""+db_to_query+"\") and "+string("db.internal.tables.status.cols.dbtable")+" in (\""+table_to_query+"\") and "+string("db.internal.tables.status.cols.sourceId")+" > "+string("db.internal.tables.status.cols.kafkaTargetId")+") kafka_tmp")
										)
			).load()
			// worker 1 : http://stackoverflow.com/questions/37285388/spark-scala-task-not-serializable-for-closure?rq=1
			
			@SerialVersionUID(100L)
			class KafkaWrapper1(sqlContext: SQLContext, fn: (Row, SQLContext, Map[String,String], Map[String,String], Properties)  => Unit) extends Serializable {
    	  
    	  var dbFields:Map[String,String] = Map()
			  dbFields += ("db" -> dataSource.db)
			  dbFields += ("table" -> dataSource.table)
			  dbFields += ("host" -> dataSource.host)
			  dbFields += ("port" -> dataSource.port)
			  dbFields += ("user" -> dataSource.user)
			  dbFields += ("pass" -> dataSource.password)
			  dbFields += ("conntype" -> dataSource.conntype)
			  dbFields += ("fullTableSchema" -> dataSource.fullTableSchema)
			  dbFields += ("bookmark" -> dataSource.bookmark)
			  dbFields += ("bookmarkformat" -> dataSource.bookmarkformat)
			  dbFields += ("primarykey" -> dataSource.primarykey)
			
			  val mysql = "mysql"//string("db.type.mysql")
			  val postgres = "postgres"//string("db.type.postgres")
			  val sqlserver = "sqlserver"//string("db.type.sqlserver")
			  val jdbc = "jdbc"//string("db.conn.jdbc")
			  
	  		var connTypes:Map[String,String] = Map()
		  	connTypes += ("mysql" -> mysql)
			  connTypes += ("postgres" -> postgres)
			  connTypes += ("sqlserver" -> sqlserver)
			  connTypes += ("jdbc" -> jdbc)
    	
			  val props = new Properties()
      	props.put("metadata.broker.list", string("sinks.kafka.brokers"))
		  	props.put("serializer.class", string("sinks.kafka.serializer"))
			  props.put("group.id", string("sinks.kafka.groupname"))
			  props.put("producer.type", string("sinks.kafka.producer.type"))
    	  
    	  def sinkMapper(df:DataFrame): RDD[Unit] = df.map { row => fn(row,sqlContext,dbFields,connTypes,props) }
    	}
			
    	val kafkaSinkFunc: (Row,SQLContext,Map[String,String],Map[String,String],Properties) => Unit = (row,sqlContext,map_db,map_conn,props) => {
    	  
    	  val pk = row.getString(0) // get pk from the row
    	  val bkmk = row.getString(1) // get bookmark from row
			  
			  val config = new ProducerConfig(props)
			  val producer = new Producer[String,String](config)
			      
			  if(conntype.contains(map_conn.get("mysql").get)){ // selecting entire rows - insertion into kafka as json and hdfs as ORC
					val jdbcDF = sqlContext.read.format(map_conn.get("mysql").get).options(
							Map(
									"driver" -> conntype,
									"url" -> (map_conn.get("jdbc").get+":"+map_conn.get("mysql").get+"://"+map_db.get("host")+":"+map_db.get("port")+"/"+map_db.get("db")+"?user="+map_db.get("user")+"&password="+map_db.get("pass")),
									"dbtable" -> ("(select "+map_db.get("fullTableSchema")+" from "+map_db.get("table")+" where "+map_db.get("primarykey")+" = "+pk+" and "+map_db.get("bookmark")+" = "+bkmk+") kafka_tmp_ser)") // table Specific
									)
							).load()
					val row_to_insert: Row = jdbcDF.first()
					var rowMap:Map[String,String] = Map()
					val fullTableSchemaArr = map_db.get("fullTableSchema").get.split(",")
					for(i <- 0 until fullTableSchemaArr.length) {
					  rowMap += (fullTableSchemaArr(i) -> row_to_insert.getString(i))  
					} // contructing the json
					implicit val formats = Serialization.formats(NoTypeHints)
					val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
					//val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
					producer.send(new KeyedMessage[String,String](("[TOPIC] "+map_db.get("db").get+"_"+map_db.get("table").get),map_db.get("table").get,json_to_insert)) // topic + key + message
					// KAFKA Over
					//updateTargetIdsBackKafka(db,table,pk,bkmk)
				}
    	  /*
    	   * for the timebeing . . .
    	   * 
				else if(conntype.contains(string("db.type.postgres"))){
				  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] db entered was == postgres")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "org.postgresql.Driver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.postgres")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("select "+map_db.get("fullTableSchema")+" from "+table+" where "+map_db.get("primarykey")+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
									)
							).load()
							val row_to_insert: Row = jdbcDF.first()
							Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] got the row . . .")
							var rowMap:Map[String,String] = Map()
							val fullTableSchemaArr = map_db.get("fullTableSchema").get.split(",")
							for(i <- 0 until fullTableSchemaArr.length) {
					      rowMap += (fullTableSchemaArr(i) -> row_to_insert.getString(i))  // rather apply preprocessing here
					    } // constructing the json
					    implicit val formats = Serialization.formats(NoTypeHints)
					    val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
					    val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
					    Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] the clean string to insert into Kafka == "+cleanStr)
					    producer.send(new KeyedMessage[String,String](("[TOPIC] "+db+"_"+table),db,cleanStr)) // topic + key + message
					    Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] published the clean string . . .")
					    // KAFKA Over
					    updateTargetIdsBackKafka(db,table,pk,bkmk)
					    "[STREAMING] [KAFKA] updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
				}
				else if(conntype.contains(string("db.type.sqlserver"))){
				  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] db entered was == sqlserver")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "com.microsoft.sqlserver.jdbc.SQLServerDriver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.sqlserver")+host+":"+port+";database="+db+";user="+user+";password="+password),
									"dbtable" -> ("select "+map_db.get("fullTableSchema")+" from "+table+" where "+map_db.get("primarykey")+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
									)
							).load()
							val row_to_insert: Row = jdbcDF.first()
							Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] got the row . . .")
							var rowMap:Map[String,String] = Map()
							val fullTableSchemaArr = map_db.get("fullTableSchema").get.split(",")
							for(i <- 0 until fullTableSchemaArr.length) {
					      rowMap += (fullTableSchemaArr(i) -> row_to_insert.getString(i))  
					    }
							implicit val formats = Serialization.formats(NoTypeHints)
							val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
							val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
							Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] the clean string to insert into Kafka == "+cleanStr)
							producer.send(new KeyedMessage[String,String](("[TOPIC] "+db+"_"+table),db,cleanStr)) // topic + key + message
							Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] published the clean string . . .")
							// KAFKA Over
							updateTargetIdsBackKafka(db,table,pk,bkmk)
							"[STREAMING] [KAFKA] updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
				}
				else{
				  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] nothing to publish . . .")
				  // no need to send it to either of kafka or hdfs
					// no supported DB type - return a random string with fullTableSchema
				  val fullTableSchemaArr = map_db.get("fullTableSchema").get.split(",")
				  var rowMap:Map[String,String] = Map()
				  for(i <- 0 until fullTableSchemaArr.length) {
					      rowMap += (fullTableSchemaArr(i) -> "nothing")  
					}
				  implicit val formats = Serialization.formats(NoTypeHints)
				  val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
				  val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
				  // KAFKA Over
				  updateTargetIdsBackKafka(db,table,pk,bkmk)
				}
				*/			
				
    	}
			sc = SparkContext.getOrCreate(conf)
			val kafkaWrapper1 = new KafkaWrapper1(sqlContext,kafkaSinkFunc)
			kafkaWrapper1.sinkMapper(jdbcDF)
			
			if(jdbcDF.rdd.isEmpty()){
								  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [KAFKA] an empty DF of PKs for == "+db_to_query+"_"+table_to_query)
								  //Holder.log.info("no persistences done to the kafka sinks whatsoever . . . for == "+db_to_query+"_"+table_to_query)
	    }
    	else{
								  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [KAFKA] going to persist into == "+db_to_query+"_"+table_to_query)
  								jdbcDF.map { x => {
  								                    //val conf = sparkConf("spark");
  								                    //val sc=SparkContext.getOrCreate(conf);
  								                    //val sqlContext = new SQLContext(sc);
  								                    
  								                    
  								                  } 
  								           } // kafka sink only
		  }
    	*/
    	/*
    	 * Commenting this approach too . . .
    	
    	val db_to_query = dataSource.db
    	val table_to_query = dataSource.table
    	val conntype_to_query = dataSource.conntype
    	val fullTableSchema_to_query = dataSource.fullTableSchema
    	val primarykey = dataSource.primarykey
    	
    	val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
								Map(
										"driver" -> string("db.internal.driver"),
										"url" -> (internalURL+"?user="+internalUser+"&password="+internalPassword),
										"dbtable" -> ("(select "+string("db.internal.tables.status.cols.primarykey")+","+string("db.internal.tables.status.cols.sourceId")+" from "+string("db.internal.tables.status.name")+" where "+string("db.internal.tables.status.cols.dbname")+" in (\""+db_to_query+"\") and "+string("db.internal.tables.status.cols.dbtable")+" in (\""+table_to_query+"\") and "+string("db.internal.tables.status.cols.sourceId")+" > "+string("db.internal.tables.status.cols.kafkaTargetId")+") kafka_tmp")
										)
								).load()
								
			if(jdbcDF.rdd.isEmpty()){
								  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [KAFKA] an empty DF of PKs for == "+db_to_query+"_"+table_to_query)
								  //Holder.log.info("no persistences done to the kafka sinks whatsoever . . . for == "+db_to_query+"_"+table_to_query)
	    }
    	else{
								  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [KAFKA] going to persist into == "+db_to_query+"_"+table_to_query)
  								jdbcDF.map { x => {
  								                    val conf = sparkConf("spark");
  								                    val sc=SparkContext.getOrCreate(conf);
  								                    val sqlContext = new SQLContext(sc);
  								                    fetchFromFactTableAndSinkKafka(x,sqlContext,conntype,fullTableSchema_to_query,host,port,db,table,user,password,primarykey,bookmark)
  								                  } 
  								           } // kafka sink only
		  }
		  * 
		  */
    	
    	//getPKs4UpdationKafka(conf,sqlContext)
    	//getPKs4UpdationHDFS(conf,sqlContext)
    	
	    /*
	    Future {
		   /*
		 		* 1. get previous offset - incr. id / bookmark table -> previous timestamp / bookmark
		 		* 2. get current offset - timestamp / source table -> current timestamp / bookmark
		 		* 3. get rows between - 1. and 2. -> distinct PKs, max(timestamp / bookmark)
		 		* 4. upsert the things from 3.
		 		* 5. current offset is appended to the bookmark table
				*/
    		Holder.log.debug("[MY DEBUG STATEMENTS] [REQUEST] [API] doing the SQL-SQL flow for == "+dataSource.db+"_"+dataSource.table)
		    Holder.log.debug("[MY DEBUG STATEMENTS] [REQUEST] [API] obtaining the previous, the current and the affected PKs")
        var prevBookMark = dataSource.getPrevBookMark() // get the previous bookmark - key,max(autoincr.) group by key filter key -> bookmark / ts | db.internal.tables.bookmarks.defs.defaultBookMarkValue for first time
		    Holder.log.debug("[MY DEBUG STATEMENTS] [REQUEST] [API] the previous bookmark == "+prevBookMark)
        var currBookMark = dataSource.getCurrBookMark(sqlContext) // get the latest / maximum bookmark / ts | db.internal.tables.bookmarks.defs.defaultBookMarkValue for first time
		    Holder.log.debug("[MY DEBUG STATEMENTS] the current bookmark == "+currBookMark)
        var PKsAffectedDF = dataSource.getAffectedPKs(sqlContext,prevBookMark,currBookMark) // distinct PKs and max(timestamp) + WHERE clause | same as PKs, timestamp if not a log table, and PKs / timestamp otherwise | could be empty also
		    Holder.log.debug("[MY DEBUG STATEMENTS] [REQUEST] [API] the count of PKs returned == "+PKsAffectedDF.count())
        if(PKsAffectedDF.rdd.isEmpty()){
		      Holder.log.info("[SQL] no records to upsert in the internal Status Table == for bookmarks : "+prevBookMark+" ==and== "+currBookMark+" for table == "+dataSource.db+"_"+dataSource.table)
		    }
		    else{
		      Holder.log.debug("[MY DEBUG STATEMENTS] [REQUEST] [API] mapping the rows of affected PKs one by one / batch maybe onto STATUS table . . .")
		      PKsAffectedDF.map { x => dataSource.upsert(x(0).toString(),x(1).toString()) } // assuming x(0) / x(1) converts to string with .toString() | insert db / table / primaryKey / sourceId / 0asTargetId
		    }
		    dataSource.updateBookMark(currBookMark) // update the bookmark table - done !
	      }(ExecutionContext.Implicits.global) onSuccess { // future onsuccess check
	        case _ => Holder.log.info("[SQL] [COMPLETED] =/="+dataSource.toString()) // ack message on success
	      }
	      Holder.log.debug("[MY DEBUG STATEMENTS] [REQUEST] [API] [SQL] still executing == "+dataSource.toString())
	      Holder.log.info("[SQL] [EXECUTING] =/="+dataSource.toString())
	      //sender ! new SQLExecuting(config,request)
	      Future {
		      dataSource.getPKs4UpdationKafka(conf,sqlContext) // or straightaway and get rid of the wrapper func pollAndSink - KAFKA
		      dataSource.getPKs4UpdationHDFS(conf,sqlContext) // or straightaway and get rid of the wrapper func pollAndSink - HDFS
	      }(ExecutionContext.Implicits.global) onSuccess {
	        case _ => Holder.log.info("[STREAMING] [COMPLETED] =/="+dataSource.toString()) // ack message on success
	      }
	      Holder.log.debug("[MY DEBUG STATEMENTS] [REQUEST] [API] [STREAMING] still executing == "+dataSource.toString())
    	  Holder.log.info("[STREAMING] [EXECUTING] =/="+dataSource.toString())
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

  def getReport(config:Config,json: String) = {
        val conf = sparkConf("spark")
        val sc = SparkContext.getOrCreate(conf)
        val sqlContext = new SQLContext(sc)
        val reportActor = context.actorOf(Props(new ReportActor(config,sqlContext)))
        reportActor ! json
        "report started"
    }

  def nothing(json: String) = {
        "Nothing to prove, no report to run !!! "+json
      }

  def fetchFromFactTableAndSinkKafka(x: Row,sqlContext: SQLContext,conntype: String,ofullTableSchema: String,
      host: String, port: String, db: String, table: String, user: String, password: String, primarykey: String, bookmark: String) : String = { // had to explicitly do that
			  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] entered the row-by-row sink . . .")
				val pk = x.getString(0) // get pk from the row
				val bkmk = x.getString(1) // get bookmark from row
				Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] doing it for == "+pk)
				
				val props = new Properties()
				props.put("metadata.broker.list", string("sinks.kafka.brokers"))
				props.put("serializer.class", string("sinks.kafka.serializer"))
			  props.put("group.id", string("sinks.kafka.groupname"))
			  props.put("producer.type", string("sinks.kafka.producer.type"))
			  
			  val config = new ProducerConfig(props)
			  val producer = new Producer[String,String](config)
				
				if(conntype.contains(string("db.type.mysql"))){ // selecting entire rows - insertion into kafka as json and hdfs as ORC
				  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] db entered was == mysql")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype,
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("select "+ofullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
									)
							).load()
					val row_to_insert: Row = jdbcDF.first()
					Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] got the row . . .")
					var rowMap:Map[String,String] = Map()
					val fullTableSchemaArr = ofullTableSchema.split(",")
					for(i <- 0 until fullTableSchemaArr.length) {
					  rowMap += (fullTableSchemaArr(i) -> row_to_insert.getString(i))  
					} // contructing the json
					implicit val formats = Serialization.formats(NoTypeHints)
					val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
					val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
					Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] the clean string to insert into Kafka == "+cleanStr)
					producer.send(new KeyedMessage[String,String](("[TOPIC] "+db+"_"+table),db,cleanStr)) // topic + key + message
					Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] published the clean string . . .")
					// KAFKA Over
					updateTargetIdsBackKafka(db,table,pk,bkmk)
					"[STREAMING] [KAFKA] updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
				}
				else if(conntype.contains(string("db.type.postgres"))){
				  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] db entered was == postgres")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "org.postgresql.Driver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.postgres")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("select "+ofullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
									)
							).load()
							val row_to_insert: Row = jdbcDF.first()
							Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] got the row . . .")
							var rowMap:Map[String,String] = Map()
							val fullTableSchemaArr = ofullTableSchema.split(",")
							for(i <- 0 until fullTableSchemaArr.length) {
					      rowMap += (fullTableSchemaArr(i) -> row_to_insert.getString(i))  // rather apply preprocessing here
					    } // constructing the json
					    implicit val formats = Serialization.formats(NoTypeHints)
					    val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
					    val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
					    Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] the clean string to insert into Kafka == "+cleanStr)
					    producer.send(new KeyedMessage[String,String](("[TOPIC] "+db+"_"+table),db,cleanStr)) // topic + key + message
					    Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] published the clean string . . .")
					    // KAFKA Over
					    updateTargetIdsBackKafka(db,table,pk,bkmk)
					    "[STREAMING] [KAFKA] updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
				}
				else if(conntype.contains(string("db.type.sqlserver"))){
				  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] db entered was == sqlserver")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "com.microsoft.sqlserver.jdbc.SQLServerDriver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.sqlserver")+host+":"+port+";database="+db+";user="+user+";password="+password),
									"dbtable" -> ("select "+ofullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
									)
							).load()
							val row_to_insert: Row = jdbcDF.first()
							Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] got the row . . .")
							var rowMap:Map[String,String] = Map()
							val fullTableSchemaArr = ofullTableSchema.split(",")
							for(i <- 0 until fullTableSchemaArr.length) {
					      rowMap += (fullTableSchemaArr(i) -> row_to_insert.getString(i))  
					    }
							implicit val formats = Serialization.formats(NoTypeHints)
							val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
							val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
							Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] the clean string to insert into Kafka == "+cleanStr)
							producer.send(new KeyedMessage[String,String](("[TOPIC] "+db+"_"+table),db,cleanStr)) // topic + key + message
							Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] published the clean string . . .")
							// KAFKA Over
							updateTargetIdsBackKafka(db,table,pk,bkmk)
							"[STREAMING] [KAFKA] updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
				}
				else{
				  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] nothing to publish . . .")
				  // no need to send it to either of kafka or hdfs
					// no supported DB type - return a random string with fullTableSchema
				  val fullTableSchemaArr = ofullTableSchema.split(",")
				  var rowMap:Map[String,String] = Map()
				  for(i <- 0 until fullTableSchemaArr.length) {
					      rowMap += (fullTableSchemaArr(i) -> "nothing")  
					}
				  implicit val formats = Serialization.formats(NoTypeHints)
				  val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
				  val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
				  // KAFKA Over
				  updateTargetIdsBackKafka(db,table,pk,bkmk)
				  "updations EXITTED successfully"
				}
			}

  def preprocess(fetchFromFactTableString: String) = { // for all kinds of preprocessing - left empty
      Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] preprocessing func called . . .")
		  fetchFromFactTableString
		}

  def updateTargetIdsBackKafka(db: String, table: String, pk: String, bkmrk: String) = {
    
      val internalHost = string("db.internal.url") // get the host from env confs - tables bookmark & status mostly
		  val internalPort = string("db.internal.port") // get the port from env confs - tables bookmark & status mostly
		  val internalDB = string("db.internal.dbname") // get the port from env confs - tables bookmark & status mostly
		  val internalUser = string("db.internal.user") // get the user from env confs - tables bookmark & status mostly
		  val internalPassword = string("db.internal.password") // get the password from env confs - tables bookmark & status mostly
		      	
		  val internalURL: String = (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+internalHost+":"+internalPort+"/"+internalDB) // the internal connection DB <status, bookmark, requests> etc

      Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] update the targets back . . .")
      val internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
		  val statement = internalConnection.createStatement()
			val updateTargetQuery = "UPDATE "+string("db.internal.tables.status.name")+" SET "+string("db.internal.tables.status.cols.kafkaTargetId")+"="+string("db.internal.tables.status.cols.sourceId")+" where "+string("db.internal.tables.status.cols.primarykey")+"="+pk+" and "+(string("db.internal.tables.status.cols.sourceId"))+"="+bkmrk+";"
		  statement.executeQuery(updateTargetQuery) // perfectly fine if another upsert might have had happened in the meanwhile
		  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] updated == "+pk+"_"+bkmrk)
		  internalConnection.close()	
  }
}
*/