package com.askme.ramanujan.util

import java.sql.DriverManager
import dispatch.url
import java.sql.Connection
import com.askme.ramanujan.Configurable
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

import org.json4s.jackson.Serialization
import org.json4s.NoTypeHints

import kafka.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.{Properties,Date};
import scala.util.Random
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.SparkConf
import com.typesafe.config.Config


class DataSource(val config: Config,val conntype: String, val host: String, val port: String,           
		val user: String, val password: String, val db: String, val table: String,          
		val bookmark: String, val bookmarkformat: String, val primarykey: String, val fullTableSchema: String) extends Configurable with Serializable{
		  val oConfig = config // maybe this relays config to them all
	    var oconntype: String = conntype // e.g com.mysql.driver.MySQL
			var ohost: String = host // host ip
			var oport: String = port // port no.
			var ouser: String = user // user
			var opassword: String = password // password 
			var odb: String = db //  dbname
			var otable: String = table // tablename
			var obookmark: String = bookmark // bookmark column - e.g timestamp etc etc.
			var obookmarkformat: String = bookmarkformat // timestamp or date or incr. id identifier format - YYYY-mm-ddTHH:MM:SSZ
			var oprimarykey: String = primarykey // primarykey - e.g orderId / payment status / transaction id etc etc
			var ofullTableSchema: String = fullTableSchema // full schema of the table - we pull entire row for Kafka HDFS

			var internalConnection:Connection = null
			val internalHost = string("db.internal.url") // get the host from env confs - tables bookmark & status mostly
			val internalPort = string("db.internal.port") // get the port from env confs - tables bookmark & status mostly
			val internalDB = string("db.internal.dbname") // get the port from env confs - tables bookmark & status mostly
			val internalUser = string("db.internal.user") // get the user from env confs - tables bookmark & status mostly
			val internalPassword = string("db.internal.password") // get the password from env confs - tables bookmark & status mostly

			val hostURL: String = (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+host+":"+port+"/"+db) // pertains to this very db+table : fully qualified name
			val internalURL: String = (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+internalHost+":"+internalPort+"/"+internalDB) // the internal connection DB <status, bookmark, requests> etc
			// returns the //info things
			override def toString(): String = ("[//info] this data source is == "+hostURL) // to Strings : db specific
			def insertInitRow(json: String) = {
		    internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
		    val statement = internalConnection.createStatement()
		    val timeNow = Calendar.getInstance.getTime()
		    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
		    val timeNowStr = format.format(timeNow);
		    val insertRequestQuery = "INSERT INTO `"+string("db.internal.tables.requests.name")+"`(`"+string("db.internal.tables.requests.cols.processDate")+"`,`"+string("db.internal.tables.requests.cols.request")+"`,`"+string("db.internal.tables.requests.cols.status")+"`)" + "VALUES( '"+timeNowStr+"','"+json+"','"+string("db.internal.tables.requests.defs.defaultStatusVal")+"')" // insert '0' as status by default.
		    ////debug("[//debug] the insertQuery == "+insertRequestQuery)
		    statement.executeUpdate(insertRequestQuery)
		    internalConnection.close()
		  }
			// Update the current bookmark
			def updateBookMark(currBookMark: String) = {
			  internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
				val statement = internalConnection.createStatement()
						val insertBkMrkQuery = "INSERT INTO `"+string("db.internal.tables.bookmarks.name")+"`(`"+string("db.internal.tables.bookmarks.cols.dbtablekey")+"`,"+string("db.internal.tables.bookmarks.cols.bookmarkId")+") VALUES( '"+db+"_"+table+"', '"+currBookMark+"')"
						statement.executeQuery(insertBkMrkQuery)
			  internalConnection.close()
			}
			// Spark # 1 job stands over.
			def upsert(primarykey: String, bookmark: String) = {
			  internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
				val statement = internalConnection.createStatement()
						val upsertQuery = "INSERT INTO `"+string("db.internal.tables.status.name")+"`("+string("db.internal.tables.status.cols.dbname")+","+string("db.internal.tables.status.cols.table")+",`"+string("db.internal.tables.status.cols.primarykey")+"`,"+string("db.internal.tables.status.cols.sourceId")+","+string("db.internal.tables.status.cols.kafkaTargetId")+string("db.internal.tables.status.cols.hdfsTargetId")+") VALUES( '"+db+"','"+table+"','"+primarykey+"', '"+bookmark+"',"+string("db.internal.tables.status.defs.defaultTargetId")+string("db.internal.tables.status.defs.defaultTargetId")+") ON DUPLICATE KEY UPDATE `"+string("db.internal.tables.status.cols.sourceId")+"` = '"+bookmark+"'"        
						val resultSet = statement.executeQuery(upsertQuery)
				internalConnection.close()
			}
			// get the previous bookmark, from bookmark table
			def getPrevBookMark() = { // this strictly returns the actual bookmark, e.g the timestamp
			      //debug("[//debug] [SQL] [BookMarks] entered to obtain the previous bookmark . . . == "+toString())
			      internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
				    //debug("[//debug] [SQL] [BookMarks] got the internal connection . . . == "+toString())
			      val statement = internalConnection.createStatement()
			      //debug("[//debug] [SQL] [BookMarks] executing the statement . . . == "+toString())
						val prevOffsetFetchQuery = "select "+string("db.internal.tables.bookmarks.cols.dbtablekey")+" as "+string("db.internal.tables.bookmarks.cols.dbtablekey")+",max("+string("db.internal.tables.bookmarks.cols.id")+") as "+string("db.internal.tables.bookmarks.cols.id")+" from "+string("db.internal.tables.bookmarks.name")+" where "+string("db.internal.tables.bookmarks.cols.dbtablekey")+" in (\""+odb+"_"+otable+"\")"
						//debug("[//debug] [SQL] [BookMarks] prev offset fetch query . . . == "+prevOffsetFetchQuery)
						var resultSet = statement.executeQuery(prevOffsetFetchQuery) // handle first time cases also
						if(resultSet.next()){
						      //debug("[//debug] [SQL] [BookMarks] [previous offset] this db_table  has a row . . . == "+toString())
							    val key: String = resultSet.getString(string("db.internal.tables.bookmarks.cols.dbtablekey"))
									val id: String = resultSet.getString(string("db.internal.tables.bookmarks.cols.id"))
									val prevBookMarkFetchQuery = "select "+string("db.internal.tables.bookmarks.cols.bookmarkId")+" from "+string("db.internal.tables.bookmarks.name")+" where "+string("db.internal.tables.bookmarks.cols.dbtablekey")+" in (\""+ key +"\") and "+string("db.internal.tables.bookmarks.cols.id")+" in ("+ id +")"
									//debug("[//debug] [SQL] [BookMarks] prev bookmark fetch query . . . == "+prevOffsetFetchQuery)
									resultSet = statement.executeQuery(prevBookMarkFetchQuery)
									if(resultSet.next()){
									  //debug("[//debug] [SQL] [BookMarks] [previous bookmark] this db_table  has a row . . . == "+toString())
										val bookmark = resultSet.getString(string("db.internal.tables.bookmarks.cols.bookmarkId"))
										    //debug("[//debug] [SQL] [BookMarks] [previous bookmark] the previous bookmark returned == "+bookmark)
												bookmark
									}
									else {
									      //debug("[//debug] [SQL] [BookMarks] [previous] the default one . . . == "+string("db.internal.tables.bookmarks.defs.defaultBookMarkValue"))
									      string("db.internal.tables.bookmarks.defs.defaultBookMarkValue")
								  }
						}
						else {
						  //debug("[//debug] [SQL] [BookMarks] [previous] the default one . . . == "+string("db.internal.tables.bookmarks.defs.defaultBookMarkValue"))
						  string("db.internal.tables.bookmarks.defs.defaultBookMarkValue")
						}
			}
			// get the current bookmark, from source table
			def getCurrBookMark(sqlContext: SQLContext) = {
			  //debug("[//debug] [SQL] [BookMarks] [current bookmark] getting it . . . ")
				if(conntype.toLowerCase().contains(string("db.type.mysql").toLowerCase())){// lowercase comparisons always
				  //debug("[//debug] [SQL] [BOOKMARKS] [current bookmark] entered mysql types db/table . . .")
				  //debug("[//debug] [SQL] [BOOKMARKS] [current bookmark] getting current bookmark / DF / . . .")
				  //debug("[//debug] [SQL] [SPARKDF query check] select max("+bookmark+") from "+table)
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype,
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("(select max("+bookmark+") as "+bookmark+" from "+table+" ) tmp") // now this is table specific so bookmark column must be passed!
									)
							).load()
							//debug("[//debug] [SQL] [BOOKMARKS] [current bookmark] got the current bookmark . . .")
						  if(jdbcDF.rdd.isEmpty()){
						    //debug("[//debug] [SQL] [BOOKMARKS] [current bookmark] the db of affected keys was all empty . . .")
						    //debug("[//debug] [SQL] [BOOKMARKS] [current bookmark] returning the default value == "+string("db.internal.tables.bookmarks.defs.defaultBookMarkValue"))
						    string("db.internal.tables.bookmarks.defs.defaultBookMarkValue")
						  }
						  else{
						    val currBookMark = jdbcDF.first().get(jdbcDF.first().fieldIndex(bookmark)).toString()
						    //debug("[//debug] [SQL] [BOOKMARKS] [current bookmark] returning the current value == "+currBookMark)
						    currBookMark
						  }
				}
				else if(conntype.toLowerCase().contains(string("db.type.postgres").toLowerCase())){
					//debug("[//debug] [SQL] [BOOKMARKS] [current bookmark] entered postgres types db/table . . .")
				  //debug("[//debug] [SQL] [BOOKMARKS] [current bookmark] getting current bookmark / DF / . . .")
				  val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "org.postgresql.Driver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.postgres")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("select max("+bookmark+") from "+table) // now this is table specific so bookmark column must be passed!
									)
							).load()
							
							if(jdbcDF.rdd.isEmpty()){
							  //debug("[//debug] [SQL] [BOOKMARKS] [current bookmark] the db of affected keys was all empty . . .")
						    //debug("[//debug] [SQL] [BOOKMARKS] [current bookmark] returning the default value == "+string("db.internal.tables.bookmarks.defs.defaultBookMarkValue"))
						    string("db.internal.tables.bookmarks.defs.defaultBookMarkValue")
						  }
						  else{
						    val currBookMark = jdbcDF.first().getString(jdbcDF.first().fieldIndex(bookmark))
						    //debug("[//debug] [SQL] [BOOKMARKS] [current bookmark] returning the current value == "+currBookMark)
						    currBookMark
						  }
				}
				else if(conntype.toLowerCase().contains(string("db.type.sqlserver").toLowerCase())){
				  //debug("[//debug] [SQL] [BOOKMARKS] [current bookmark] entered sqlserver types db/table . . .")
				  //debug("[//debug] [SQL] [BOOKMARKS] [current bookmark] getting current bookmark / DF / . . .")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "com.microsoft.sqlserver.jdbc.SQLServerDriver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.sqlserver")+"://"+host+":"+port+";database="+db+";user="+user+";password="+password),
									"dbtable" -> ("select max("+bookmark+") from "+table) // now this is table specific so bookmark column must be passed!
									)
							).load()
							
							if(jdbcDF.rdd.isEmpty()){
							  //debug("[//debug] [SQL] [BOOKMARKS] [current bookmark] the db of affected keys was all empty . . .")
						    //debug("[//debug] [SQL] [BOOKMARKS] [current bookmark] returning the default value == "+string("db.internal.tables.bookmarks.defs.defaultBookMarkValue"))
						    string("db.internal.tables.bookmarks.defs.defaultBookMarkValue")
						  }
						  else{
						    val currBookMark = jdbcDF.first().getString(jdbcDF.first().fieldIndex(bookmark))
						    //debug("[//debug] [SQL] [BOOKMARKS] [current bookmark] returning the current value == "+currBookMark)
						    currBookMark
						  }
				}
				else{
					// no supported DB type
				  //debug("[//debug] [SQL] [BOOKMARKS] [current bookmark] entered some unsupported DB Driver / type . . .")
					string("db.internal.tables.bookmarks.defs.defaultBookMarkValue")
				}
			}
			def getAffectedPKs(sqlContext: SQLContext,prevBookMark: String,currBookMark: String) = {
			  //debug("[//debug] [SQL] [AFFECTED PKs] getting it . . . ")
				if(conntype.toLowerCase().contains(string("db.type.mysql").toLowerCase())){
				  //debug("[//debug] [SQL] [AFFECTED PKs] entered mysql types db/table . . .")
				  //debug("[//debug] [SQL] [AFFECTED PKs] getting affected PKs / DF / . . .")
				  //debug("[//debug] [SQL] [AFFECTED PKs] [QUERY] === (select "+primarykey+" as "+primarykey+", max("+bookmark+") as "+bookmark+" from "+table+" where "+bookmark+" >= \""+prevBookMark+"\" and "+bookmark+" <= \""+currBookMark+"\" group by "+primarykey+") overtmp")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype,
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("(select "+primarykey+" as "+primarykey+", max("+bookmark+") as "+bookmark+" from "+table+" where "+bookmark+" >= \""+prevBookMark+"\" and "+bookmark+" <= \""+currBookMark+"\" group by "+primarykey+") overtmp") // table specific
									)
							).load()
							jdbcDF
				}
				else if(conntype.toLowerCase().contains(string("db.type.postgres").toLowerCase())){
				  //debug("[//debug] [SQL] [AFFECTED PKs] entered postgres types db/table . . .")
				  //debug("[//debug] [SQL] [AFFECTED PKs] getting affected PKs / DF / . . .")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "org.postgresql.Driver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.postgres")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("select "+primarykey+", max("+bookmark+") from "+table+" where "+bookmark+" >= "+prevBookMark+" and "+bookmark+" <= "+currBookMark+" group by "+primarykey) // table specific
									)
							).load()
							jdbcDF
				}
				else if(conntype.toLowerCase().contains(string("db.type.sqlserver").toLowerCase())){
				  //debug("[//debug] [SQL] [AFFECTED PKs] entered sqlserver types db/table . . .")
				  //debug("[//debug] [SQL] [AFFECTED PKs] getting affected PKs / DF / . . .")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "com.microsoft.sqlserver.jdbc.SQLServerDriver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.sqlserver")+"://"+host+":"+port+";database="+db+";user="+user+";password="+password),
									"dbtable" -> ("select "+primarykey+", max("+bookmark+") from "+table+" where "+bookmark+" >= "+prevBookMark+" and "+bookmark+" <= "+currBookMark+" group by "+primarykey) // table specific
									)
							).load()
							jdbcDF
				}
				else{
					// no supported DB type
				  //debug("[//debug] [SQL] [AFFECTED PKs] entered some unsupported db / table . . .")
				  //debug("[//debug] [SQL] [AFFECTED PKs] returning an empty / DF / . . .")
					sqlContext.emptyDataFrame // just return one empty data frame, no rows, no columns
				}
			}
			def getPKs4UpdationKafka(conf: SparkConf,sqlContext: SQLContext) = { // get the primary keys for insertion, as well as their timestamps/ bookmarks
				val table_to_query = table; val db_to_query = db
				//debug("[//debug] [STREAMING] [PKs] [KAFKA] for == "+db_to_query+"_"+table_to_query)
				val sc = SparkContext.getOrCreate(conf)
						val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
								Map(
										"driver" -> string("db.internal.driver"),
										"url" -> (internalURL+"?user="+internalUser+"&password="+internalPassword),
										"dbtable" -> ("select "+string("db.internal.tables.status.cols.primarykey")+","+string("db.internal.tables.status.cols.sourceId")+" from "+string("db.internal.tables.status.name")+" where "+string("db.internal.tables.status.cols.dbname")+" = "+db_to_query+" and "+string("db.internal.tables.status.cols.table")+" = "+table_to_query+" and "+string("db.internal.tables.status.cols.sourceId")+" > "+string("db.internal.tables.status.cols.kafkaTargetId"))
										)
								).load()
								if(jdbcDF.rdd.isEmpty()){
								  //debug("[//debug] [STREAMING] [PKs] [KAFKA] an empty DF of PKs for == "+db_to_query+"_"+table_to_query)
								  //info("no persistences done to the kafka sinks whatsoever . . . for == "+db_to_query+"_"+table_to_query)
								}
								else{
								  //debug("[//debug] [STREAMING] [PKs] [KAFKA] going to persist into == "+db_to_query+"_"+table_to_query)
  								jdbcDF.map { x => fetchFromFactTableAndSinkKafka(x,sqlContext) } // kafka sink only
								}
				
								// No piggyMerge in here.
			}
			def getPKs4UpdationHDFS(conf: SparkConf,sqlContext: SQLContext) = { // get the primary keys for insertion, as well as their timestamps/ bookmarks
				val table_to_query = table; val db_to_query = db
				//debug("[//debug] [STREAMING] [PKs] [HDFS] for == "+db_to_query+"_"+table_to_query)
				val sc = SparkContext.getOrCreate(conf)
						val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
								Map(
										"driver" -> string("db.internal.driver"),
										"url" -> (internalURL+"?user="+internalUser+"&password="+internalPassword),
										"dbtable" -> ("select "+string("db.internal.tables.status.cols.primarykey")+","+string("db.internal.tables.status.cols.sourceId")+" from "+string("db.internal.tables.status.name")+" where "+string("db.internal.tables.status.cols.dbname")+" = "+db_to_query+" and "+string("db.internal.tables.status.cols.table")+" = "+table_to_query+" and "+string("db.internal.tables.status.cols.sourceId")+" > "+string("db.internal.tables.status.cols.hdfsTargetId"))
										)
								).load()
								if(jdbcDF.rdd.isEmpty()){
								  //debug("[//debug] [STREAMING] [PKs] [HDFS] an empty DF of PKs for == "+db_to_query+"_"+table_to_query)
								  //info("no persistences done to the sinks whatsoever . . . for == "+db_to_query+"_"+table_to_query)
								}
								else{
								  //debug("[//debug] [STREAMING] [PKs] [HDFS] going to persist into == "+db_to_query+"_"+table_to_query)
  								jdbcDF.map { x => fetchFromFactTableAndSinkHDFS(x,sqlContext) } // hdfs append
								}
								//debug("[//debug] [STREAMING] [PKs] [HDFS] [PiggyMerge] going to persist into == "+db_to_query+"_"+table_to_query)
								piggyMerge(sc,sqlContext,db_to_query,table_to_query) // take the two tables, and merge them
			}

			def fetchFromFactTableAndSinkKafka(x: Row,sqlContext: SQLContext) : String = { // had to explicitly do that
			  //debug("[//debug] [STREAMING] [KAFKA] entered the row-by-row sink . . .")
				val pk = x.getString(0) // get pk from the row
				val bkmk = x.getString(1) // get bookmark from row
				//debug("[//debug] [STREAMING] [KAFKA] doing it for == "+pk)
				
				val props = new Properties()
				props.put("metadata.broker.list", string("sinks.kafka.brokers"))
				props.put("serializer.class", string("sinks.kafka.serializer"))
			  props.put("group.id", string("sinks.kafka.groupname"))
			  props.put("producer.type", string("sinks.kafka.producer.type"))
			  
			  val config = new ProducerConfig(props)
			  val producer = new Producer[String,String](config)
				
				if(conntype.contains(string("db.type.mysql"))){ // selecting entire rows - insertion into kafka as json and hdfs as ORC
				  //debug("[//debug] [STREAMING] [KAFKA] db entered was == mysql")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype,
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("select "+ofullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
									)
							).load()
					val row_to_insert: Row = jdbcDF.first()
					//debug("[//debug] [STREAMING] [KAFKA] got the row . . .")
					var rowMap:Map[String,String] = Map()
					val fullTableSchemaArr = ofullTableSchema.split(",")
					for(i <- 0 until fullTableSchemaArr.length) {
					  rowMap += (fullTableSchemaArr(i) -> row_to_insert.getString(i))  
					} // contructing the json
					implicit val formats = Serialization.formats(NoTypeHints)
					val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
					val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
					//debug("[//debug] [STREAMING] [KAFKA] the clean string to insert into Kafka == "+cleanStr)
					producer.send(new KeyedMessage[String,String](("[TOPIC] "+db+"_"+table),db,cleanStr)) // topic + key + message
					//debug("[//debug] [STREAMING] [KAFKA] published the clean string . . .")
					// KAFKA Over
					updateTargetIdsBackKafka(db,table,pk,bkmk)
					"[STREAMING] [KAFKA] updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
				}
				else if(conntype.contains(string("db.type.postgres"))){
				  //debug("[//debug] [STREAMING] [KAFKA] db entered was == postgres")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "org.postgresql.Driver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.postgres")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("select "+ofullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
									)
							).load()
							val row_to_insert: Row = jdbcDF.first()
							//debug("[//debug] [STREAMING] [KAFKA] got the row . . .")
							var rowMap:Map[String,String] = Map()
							val fullTableSchemaArr = ofullTableSchema.split(",")
							for(i <- 0 until fullTableSchemaArr.length) {
					      rowMap += (fullTableSchemaArr(i) -> row_to_insert.getString(i))  // rather apply preprocessing here
					    } // constructing the json
					    implicit val formats = Serialization.formats(NoTypeHints)
					    val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
					    val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
					    //debug("[//debug] [STREAMING] [KAFKA] the clean string to insert into Kafka == "+cleanStr)
					    producer.send(new KeyedMessage[String,String](("[TOPIC] "+db+"_"+table),db,cleanStr)) // topic + key + message
					    //debug("[//debug] [STREAMING] [KAFKA] published the clean string . . .")
					    // KAFKA Over
					    updateTargetIdsBackKafka(db,table,pk,bkmk)
					    "[STREAMING] [KAFKA] updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
				}
				else if(conntype.contains(string("db.type.sqlserver"))){
				  //debug("[//debug] [STREAMING] [KAFKA] db entered was == sqlserver")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "com.microsoft.sqlserver.jdbc.SQLServerDriver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.sqlserver")+host+":"+port+";database="+db+";user="+user+";password="+password),
									"dbtable" -> ("select "+fullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
									)
							).load()
							val row_to_insert: Row = jdbcDF.first()
							//debug("[//debug] [STREAMING] [KAFKA] got the row . . .")
							var rowMap:Map[String,String] = Map()
							val fullTableSchemaArr = ofullTableSchema.split(",")
							for(i <- 0 until fullTableSchemaArr.length) {
					      rowMap += (fullTableSchemaArr(i) -> row_to_insert.getString(i))  
					    }
							implicit val formats = Serialization.formats(NoTypeHints)
							val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
							val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
							//debug("[//debug] [STREAMING] [KAFKA] the clean string to insert into Kafka == "+cleanStr)
							producer.send(new KeyedMessage[String,String](("[TOPIC] "+db+"_"+table),db,cleanStr)) // topic + key + message
							//debug("[//debug] [STREAMING] [KAFKA] published the clean string . . .")
							// KAFKA Over
							updateTargetIdsBackKafka(db,table,pk,bkmk)
							"[STREAMING] [KAFKA] updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
				}
				else{
				  //debug("[//debug] [STREAMING] [KAFKA] nothing to publish . . .")
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
			
			def fetchFromFactTableAndSinkHDFS(x: Row,sqlContext: SQLContext) : String = { // had to explicitly do that
			  //debug("[//debug] [STREAMING] [HDFS] entered hdfs persistence function . . .")
				val pk = x.getString(0) // get pk from the row
				val bkmk = x.getString(1) // get bookmark from row
				//debug("[//debug] [STREAMING] [HDFS] doing it for == "+pk)
				if(conntype.contains(string("db.type.mysql"))){ // selecting entire rows - insertion into kafka as json and hdfs as ORC
				  //debug("[//debug] [STREAMING] [HDFS] dbtype == mysql")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype,
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("select "+ofullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
									)
							).load()
					// put the row into parent HDFS location + bookmarks wise year/month/date
					//debug("[//debug] [STREAMING] [HDFS] inserting into the hdfs the entire (jdbc)DF")
					jdbcDF.write.partitionBy(bookmark).mode(SaveMode.Append).orc(formPartitionPath(string("sinks.hdfs.prefixPath"),db,table,bkmk))
					updateTargetIdsBackHDFS(db,table,pk,bkmk)
					"[STREAMING] [HDFS] updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
				}
				else if(conntype.contains(string("db.type.postgres"))){
				  //debug("[//debug] [STREAMING] [HDFS] dbtype == postgres")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "org.postgresql.Driver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.postgres")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("select "+ofullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
									)
							).load()
					    // put the row into parent HDFS location + bookmarks wise year/month/date
					    //debug("[//debug] [STREAMING] [HDFS] inserting into the hdfs the entire (jdbc)DF")
							jdbcDF.write.partitionBy(bookmark).mode(SaveMode.Append).orc(formPartitionPath(string("sinks.hdfs.prefixPath"),db,table,bkmk))
					    updateTargetIdsBackHDFS(db,table,pk,bkmk)
					    "updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
				}
				else if(conntype.contains(string("db.type.sqlserver"))){
				  //debug("[//debug] [STREAMING] [HDFS] dbtype == sqlserver")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "com.microsoft.sqlserver.jdbc.SQLServerDriver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.sqlserver")+host+":"+port+";database="+db+";user="+user+";password="+password),
									"dbtable" -> ("select "+fullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
									)
							).load()
							// put the row into parent HDFS location + bookmarks wise year/month/date
							//debug("[//debug] [STREAMING] [HDFS] inserting into the hdfs the entire (jdbc)DF")
							jdbcDF.write.partitionBy(bookmark).mode(SaveMode.Append).orc(formPartitionPath(string("sinks.hdfs.prefixPath"),db,table,bkmk))
							updateTargetIdsBackHDFS(db,table,pk,bkmk)
							"updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
				}
				else{ // no need to send it to either of kafka or hdfs
					// no supported DB type - return a random string with fullTableSchema
				  // KAFKA Over
				  //debug("[//debug] [STREAMING] [HDFS] some unsupported db type . . .")
				  updateTargetIdsBackHDFS(db,table,pk,bkmk)
				  "updations EXITTED successfully"
				}
			}

  def preprocess(fetchFromFactTableString: String) = { // for all kinds of preprocessing - left empty
      //debug("[//debug] [STREAMING] preprocessing func called . . .")
		  fetchFromFactTableString
		}

  def updateTargetIdsBackKafka(db: String, table: String, pk: String, bkmrk: String) = {
      //debug("[//debug] [STREAMING] [KAFKA] update the targets back . . .")
      internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
		  val statement = internalConnection.createStatement()
			val updateTargetQuery = "UPDATE "+string("db.internal.tables.status.name")+" SET "+string("db.internal.tables.status.cols.kafkaTargetId")+"="+string("db.internal.tables.status.cols.sourceId")+" where "+string("db.internal.tables.status.cols.primarykey")+"="+pk+" and "+(string("db.internal.tables.status.cols.sourceId"))+"="+bkmrk+";"
		  statement.executeQuery(updateTargetQuery) // perfectly fine if another upsert might have had happened in the meanwhile
		  //debug("[//debug] [STREAMING] [KAFKA] updated == "+pk+"_"+bkmrk)
		  internalConnection.close()	
  }
  def updateTargetIdsBackHDFS(db: String, table: String, pk: String, bkmrk: String) = {
      //debug("[//debug] [STREAMING] [HDFS] update the targets back . . .")
      internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
		  val statement = internalConnection.createStatement()
			val updateTargetQuery = "UPDATE "+string("db.internal.tables.status.name")+" SET "+string("db.internal.tables.status.cols.hdfsTargetId")+"="+string("db.internal.tables.status.cols.sourceId")+" where "+string("db.internal.tables.status.cols.primarykey")+"="+pk+" and "+(string("db.internal.tables.status.cols.sourceId"))+"="+bkmrk+";"
		  statement.executeQuery(updateTargetQuery) // perfectly fine if another upsert might have had happened in the meanwhile
		  //debug("[//debug] [STREAMING] [HDFS] updated == "+pk+"_"+bkmrk)
		  internalConnection.close()	
  }

  def formPartitionPath(prefix: String, db: String, table: String,bookmark: String) = {
      //debug("[//debug] [STREAMING] [HDFS] form some goddamn partition path . . .")
		  prefix+db+"_"+table+"/dt="+bookmark.substring(0, int("sinks.hdfs.partitionEnd")) // not using the bookmark because of no partition,or infact
		}

  def piggyMerge(sc: SparkContext,sqlContext: SQLContext, db_to_query: String, table_to_query: String) = { // upsert HDFS work-around
		  // pig like load, join, generate -> store
      /*
       * one fallacy is that we will have to load all the partitions, because a very old sourceId might be just getting updated suxessfully
       * hdfsMAIN <- mainHDFS_URL+db_to_query+table_to_query
       * hdfsCHANGES <- mainHDFS_URL_db_to_query+table_to_query
       * merge hdfsMAIN + hdfsCHANGES --> update/overwrite the hdfsMAIN
       * join them on a key
       * overwrite hdfsMAIN location - try partitioning as year/month/date
       */
      //debug("[//debug] [STREAMING] [HDFS] piggy Merge was called"+db_to_query+"_"+table_to_query)
      val fullTableSchema = StructType(ofullTableSchema.split(',').map { x => StructField(x,StringType,true) }) // schema
      
      val hdfsMAINdf = sqlContext.read.format("orc").load(getTable("main",db_to_query,table_to_query)).toDF(ofullTableSchema) // all partitions
      //debug("[//debug] [STREAMING] [HDFS] hdfs Main got it . . .")
      val hdfsCHANGESdf = sqlContext.read.format("orc").load(getTable("changes",db_to_query,table_to_query)).toDF(ofullTableSchema) // all partition
      //debug("[//debug] [STREAMING] [HDFS] hdfs Changes got it . . .")
      val hdfs1df = hdfsMAINdf.join(hdfsCHANGESdf, hdfsMAINdf(primarykey) !== hdfsCHANGESdf(primarykey)) // get the non affected ones ! Costly
      //debug("[//debug] [STREAMING] [HDFS] hdfs1df got it . . .")
      val dfUnion = hdfs1df.unionAll(hdfsCHANGESdf)
      //debug("[//debug] [STREAMING] [HDFS] dfUnion Main got it . . .")
      // pig like flow - uncommon records + new editions/changes
      // now this dfUnion overrides the hdfsMAINdf location - either bookmark partition or map it to date time etc. - even bookmark is fine
      //debug("[//debug] [STREAMING] [HDFS] upserting . . .")
      dfUnion.write.mode(SaveMode.Overwrite).partitionBy(bookmark).format("orc").save(getTable("main", db_to_query, table_to_query)) // partition by needed ?!?
	    //debug("[//debug] [STREAMING] [HDFS] upserted . . .")	
  }

    def getTable(arg: String, db_to_query: String, table_to_query: String) = {
		  // placeholder -> return the main and changes tables only
      //debug("[//debug] [STREAMING] [HDFS] getting the table . . .")
      arg+db_to_query+table_to_query
		}

}