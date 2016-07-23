package com.askme.ramanujan.server
import java.sql.DriverManager
import java.util.Calendar
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import com.askme.ramanujan.Configurable
import com.askme.ramanujan.actors.{Listener, WorkerActor}
import com.askme.ramanujan.util.DataSource
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.apache.spark.SparkContext
import org.apache.spark.scheduler._
import org.apache.spark.sql.SQLContext
import spray.json.{DefaultJsonProtocol, _}

class RootServer(val config: Config) extends Configurable with Server with Logging with Serializable {
  
//    object Holder extends Serializable {
//     @transient lazy val log = Logger.getLogger(getClass.getName)
//    }
    // normal sparkContext - initialize it from conf.spark
    debug("[DEBUG] initialize the conf for spark and the spark context . . .")
		val conf = sparkConf("spark")

		val sc: SparkContext = SparkContext.getOrCreate(conf) // ideally this is what one must do - getOrCreate
		val customSparkListener: CustomSparkListener = new CustomSparkListener()
	  sc.addSparkListener(customSparkListener)

		// one sqlContext - to pass on
		val sqlContext = new SQLContext(sc)

		//create an actorSystem
		info("creating the actor system == "+string("actorSystem.name"))
	  private implicit val pipelineSystem = ActorSystem(string("actorSystem.name"))

	  // the listener, an option sys log class basically - one listener only
	  info("creating the listener actor == "+string("actorSystem.actors.listener"))
		val listener = pipelineSystem.actorOf(Props(classOf[Listener],config), name = string("actorSystem.actors.listener")) // one listener

//		val indexService = string("sinks.druid.overlord")//"overlord" // Your overlord's druid.service, with slashes replaced by colons.
//		val firehosePattern = string("sinks.druid.firehose")//"druid:firehose:%s" // Make up a service pattern, include %s somewhere in it.
//		val discoveryPath = string("sinks.druid.discovery")//"/druid/discovery" // Your overlord's druid.discovery.curator.path.
//
//		val curator = CuratorFrameworkFactory.builder().connectString(string("sinks.hdfs.zookeeper.url"))
//		.retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
//		.build()
//		curator.start()

		// the master class
		//info("creating the master actor == "+string("actorSystem.actors.master"))
		//val requestHandlerRef = pipelineSystem.actorOf(Props(classOf[RequestHandler],config,conf,listener,sqlContext), name = string("actorSystem.actors.master"))
		// take a table in - completely? 

		/*
		 * COMMENTING THE GENERAL SPARK SQL APPROACH - USE A DB CONN INSTEAD.
		 * debug("[DEBUG]creating requestsRecordsDB")
		var requestsRecordsDB = sqlContext.read.format(string("db.conn.jdbc")).option("url", string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+string("db.internal.url")+"/"+string("db.internal.dbname")).option("driver",string("db.internal.driver"))
		.option("dbtable",string("db.internal.tables.requests.name")).option("user",string("db.internal.user")).option("password",string("db.internal.password"))
		.load()
		*   
		*/

		debug("[DEBUG] invoking the internal db connections . . .")
		val internalHost = string("db.internal.url") // get the host from env confs - tables bookmark & status mostly
		val internalPort = string("db.internal.port") // get the port from env confs - tables bookmark & status mostly
		val internalDB = string("db.internal.dbname") // get the port from env confs - tables bookmark & status mostly
	  val internalUser = string("db.internal.user") // get the user from env confs - tables bookmark & status mostly
		val internalPassword = string("db.internal.password") // get the password from env confs - tables bookmark & status mostly
		
		val internalURL: String = string("db.conn.jdbc")+":"+string("db.conn.use")+"://"+internalHost+":"+internalPort+"/"+internalDB // the internal connection DB <status, bookmark, requests> etc
		
		var connection = DriverManager.getConnection(internalURL, internalUser, internalPassword)

	  debug("[DEBUG] the initial internal db connection invoked . . .")
		//val statement = connection.createStatement()

		// while 1 loop - constant poll

	  def getMins(freqUnit: String, freqDim: String): Int = {
		  if(freqDim.toLowerCase().contains("min")){
		  	freqUnit.toInt
		  }
			else if(freqDim.toLowerCase().contains("hour")){
			  freqUnit.toInt * 60 // ok if hardcoded
		  }
		  else if(freqDim.toLowerCase().contains("day")){
			  freqUnit.toInt * 3600 // ok if hardcoded
		  }
		  else  if(freqDim.toLowerCase().contains("week")){
			  freqUnit.toInt * 25200 // ok if hardcoded
		  }
		  else freqUnit.toInt * 25200 * 4
	  }

		//while(true) {
			debug("[DEBUG] [REQUESTS] REQUESTS TABLE QUERY == SELECT * FROM "+string("db.internal.tables.requests.name"))
			val statement = connection.createStatement()
			val resultSet = statement.executeQuery("SELECT * FROM "+string("db.internal.tables.requests.name")) // get all requests

			while(resultSet.next()){ // the values and fields are always realtime
				// metadata
				val processDate = resultSet.getString(string("db.internal.tables.requests.cols.processDate"))
				val request = resultSet.getString(string("db.internal.tables.requests.cols.request"))
				val host = resultSet.getString(string("db.internal.tables.requests.cols.host"))
				val port = resultSet.getString(string("db.internal.tables.requests.cols.port"))
				val dbname = resultSet.getString(string("db.internal.tables.requests.cols.dbname"))
				val dbtable = resultSet.getString(string("db.internal.tables.requests.cols.dbtable"))
				// health
				val lastStarted = resultSet.getString(string("db.internal.tables.requests.cols.lastStarted"))
				val lastEnded = resultSet.getString(string("db.internal.tables.requests.cols.lastEnded"))
				val runFrequency = resultSet.getString(string("db.internal.tables.requests.cols.runFrequency"))
				val totalRuns = resultSet.getString(string("db.internal.tables.requests.cols.totalRuns"))
				val successRuns = resultSet.getString(string("db.internal.tables.requests.cols.success"))
				val failureRuns = resultSet.getString(string("db.internal.tables.requests.cols.failure"))
				val exceptions = resultSet.getString(string("db.internal.tables.requests.cols.exceptions"))
				val currentState = resultSet.getString(string("db.internal.tables.requests.cols.currentState")) // currentState

				debug("[DEBUG] [RECORD 1@Atime@] processDate == "+processDate)
				debug("[DEBUG] [RECORD 1@Atime@] request == "+request)
				debug("[DEBUG] [RECORD 1@Atime@] host == "+host)
				debug("[DEBUG] [RECORD 1@Atime@] port == "+port)
				debug("[DEBUG] [RECORD 1@Atime@] dbname == "+dbname)
				debug("[DEBUG] [RECORD 1@Atime@] dbtable == "+dbtable)
				debug("[DEBUG] [RECORD 1@Atime@] lastStarted == "+lastStarted)
				debug("[DEBUG] [RECORD 1@Atime@] lastEnded == "+lastEnded)
				debug("[DEBUG] [RECORD 1@Atime@] runFrequency == "+runFrequency)
				debug("[DEBUG] [RECORD 1@Atime@] successRuns == "+successRuns)
				debug("[DEBUG] [RECORD 1@Atime@] failureRuns == "+failureRuns)
				debug("[DEBUG] [RECORD 1@Atime@] exceptions == "+exceptions)
				debug("[DEBUG] [RECORD 1@Atime@] currentState == "+currentState)

				debug("[DEBUG] [REQUESTS] [WHILE 1] current dataSource == "+request.toString())
				// val format = new SimpleDateFormat"%Y-%m-%d %H:%M:%S")
				val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
				val lastStartedDate = format.parse(lastStarted)
				val lastEndedDate = format.parse(lastEnded)

				debug("[DEBUG] [DATES @formatted@ lastStartedDate == "+lastStartedDate)
				debug("[DEBUG] [DATES @formatted@ lastEndedDate == "+lastEndedDate)

				val currentDateDate = Calendar.getInstance().getTime()
				val currentDateStr = format.format(currentDateDate)

				debug("[DEBUG] [DATES @formatted@ currentDateStr == "+currentDateStr)

				def randomString(length: Int) = scala.util.Random.alphanumeric.take(length).mkString

				if(totalRuns.toInt == 0){ // start it. the first time
					// update :
					// totalRuns ++
					// currentStatus = RUNNING
					// lastStarted
					//statement.executeUpdate("UPDATE "+string("db.internal.tables.requests.name")+" SET "+string("db.internal.tables.requests.cols.lastStarted")+" = "+currentDateStr+" , "+string("db.internal.tables.requests.cols.totalRuns")+" = "+string("db.internal.tables.requests.cols.totalRuns")+" + 1 , "+string("db.internal.tables.requests.cols.currentState")+" = "+string("db.internal.tables.requests.defs.running")+" where "+string("db.internal.tables.requests.cols.host")+" = "+host+" and "+string("db.internal.tables.requests.cols.dbname")+" = "+dbname+" and "+string("db.internal.tables.requests.cols.dbname")+" = "+dbtable)
					// id, host, db, table, request, time, hash, exception, notes
					val hash = randomString(10)
					debug("[DEBUG] hash @redundant@ == "+hash)
					debug("INSERT INTO `"+string("db.internal.tables.runninglogs.name")+"` (`"+string("db.internal.tables.runninglogs.cols.host")+"`,`"+string("db.internal.tables.runninglogs.cols.port")+"`,`"+string("db.internal.tables.runninglogs.cols.dbname")+"`,`"+string("db.internal.tables.runninglogs.cols.dbtable")+"`,`"+string("db.internal.tables.runninglogs.cols.runTimeStamp")+"`,`"+string("db.internal.tables.runninglogs.cols.hash")+"`,`"+string("db.internal.tables.runninglogs.cols.exceptions")+"`,`"+string("db.internal.tables.runninglogs.cols.notes")+"`) VALUES( '"+host+"','"+port+"','"+dbname+"','"+dbtable+"','"+currentDateStr+"','"+hash+"','none','started first run ever')")
					val statement1 = connection.createStatement()
					statement1.executeUpdate("INSERT INTO `"+string("db.internal.tables.runninglogs.name")+"` (`"+string("db.internal.tables.runninglogs.cols.host")+"`,`"+string("db.internal.tables.runninglogs.cols.port")+"`,`"+string("db.internal.tables.runninglogs.cols.dbname")+"`,`"+string("db.internal.tables.runninglogs.cols.dbtable")+"`,`"+string("db.internal.tables.runninglogs.cols.runTimeStamp")+"`,`"+string("db.internal.tables.runninglogs.cols.hash")+"`,`"+string("db.internal.tables.runninglogs.cols.exceptions")+"`,`"+string("db.internal.tables.runninglogs.cols.notes")+"`) VALUES( '"+host+"','"+port+"','"+dbname+"','"+dbtable+"','"+currentDateStr+"','"+hash+"','none','started first run ever')")
					debug("[DEBUG] inserted into running logs (STARTED) ### total Runs were 0 so far . . .")
					debug("UPDATE "+string("db.internal.tables.requests.name")+" SET "+string("db.internal.tables.requests.cols.lastStarted")+" = \""+currentDateStr+"\" , "+string("db.internal.tables.requests.cols.totalRuns")+" = "+string("db.internal.tables.requests.cols.totalRuns")+" + 1 , "+string("db.internal.tables.requests.cols.currentState")+" = \""+string("db.internal.tables.requests.defs.defaultRunningState")+"\" where "+string("db.internal.tables.requests.cols.host")+" = \""+host+"\" and "+string("db.internal.tables.requests.cols.dbname")+" = \""+dbname+"\" and "+string("db.internal.tables.requests.cols.dbname")+" = \""+dbtable+"\"")
					val statement2 = connection.createStatement()
					statement2.executeUpdate("UPDATE "+string("db.internal.tables.requests.name")+" SET "+string("db.internal.tables.requests.cols.lastStarted")+" = \""+currentDateStr+"\" , "+string("db.internal.tables.requests.cols.totalRuns")+" = "+string("db.internal.tables.requests.cols.totalRuns")+" + 1 , "+string("db.internal.tables.requests.cols.currentState")+" = \""+string("db.internal.tables.requests.defs.defaultRunningState")+"\" where "+string("db.internal.tables.requests.cols.host")+" = \""+host+"\" and "+string("db.internal.tables.requests.cols.port")+" = \""+port+"\" and "+string("db.internal.tables.requests.cols.dbname")+" = \""+dbname+"\" and "+string("db.internal.tables.requests.cols.dbtable")+" = \""+dbtable+"\"")
					val dataSource = transform(request) // send this message
					debug("[DEBUG] request transformed into a @datasource@ . . .")
					val workerActor = pipelineSystem.actorOf(Props(classOf[WorkerActor],config))
					workerActor ! new TableMessage(listener,dataSource,hash)
					debug("[DEBUG] a @@ worker actor @@ assigned")
					//val druidActor = pipelineSystem.actorOf(Props(classOf[DruidActor],config))
					//druidActor ! new DruidMessage(listener,dataSource,hash)

				}
				else{
					val diffInMinsEndedStarted = TimeUnit.MINUTES.convert(lastEndedDate.getTime() - lastStartedDate.getTime(),TimeUnit.MILLISECONDS) // milliseconds to minutes
					if(diffInMinsEndedStarted < 0){ // it must be either still running or failed !
						  debug("[DEBUG] difference in minutes between @@lastStarted@@ and @@lastEnded@@ is less than ZERO . . .")
							if(exceptions == string("db.internal.tables.requests.defs.NoException")){
								debug("[DEBUG] the previous run is not complete yet, it is still running . . .")
								debug("[DEBUG] [REQUESTS] [WHILE 1] still executing == "+request+" and current state == "+currentState)
							}
							else{
								debug("[DEBUG] [REQUESTS] [WHILE 1] encountered exception == "+exceptions+" for dataSource == "+request)
								debug("[DEBUG] ##re-running , ##re-trying . . .")
								val diffInMinsCurrentStarted = TimeUnit.MINUTES.convert(currentDateDate.getTime() - lastStartedDate.getTime(),TimeUnit.MILLISECONDS)
								val schedulingFrequencyParts = runFrequency.split(" ")
								val freqUnit = schedulingFrequencyParts(0)
								val freqDim = schedulingFrequencyParts(1)
								val freqInMins = getMins(freqUnit,freqDim)
								debug("[DEBUG] the scheduling frequency == "+runFrequency+" || got converted into mins() : "+freqInMins)
								if(diffInMinsCurrentStarted > freqInMins){
									val hash = randomString(10)
									debug("[DEBUG] hash @redundant@ == "+hash)
									debug("[DEBUG] [REQUESTS] [WHILE 1] it is time to run the script again . . .")
									val statement1 = connection.createStatement()
									statement1.executeUpdate("INSERT INTO `"+string("db.internal.tables.runninglogs.name")+"` (`"+string("db.internal.tables.runninglogs.cols.host")+"`,`"+string("db.internal.tables.runninglogs.cols.port")+"`,`"+string("db.internal.tables.runninglogs.cols.dbname")+"`,`"+string("db.internal.tables.runninglogs.cols.dbtable")+"`,`"+string("db.internal.tables.runninglogs.cols.runTimeStamp")+"`,`"+string("db.internal.tables.runninglogs.cols.hash")+"`,`"+string("db.internal.tables.runninglogs.cols.exceptions")+"`,`"+string("db.internal.tables.runninglogs.cols.notes")+"`) VALUES( '"+host+"','"+port+"','"+dbname+"','"+dbtable+"','"+currentDateStr+"','"+hash+"','none','started the next run . . .')")
									debug("[DEBUG] inserted into running logs (STARTED) . . .")
									val statement2 = connection.createStatement()
									statement2.executeUpdate("UPDATE "+string("db.internal.tables.requests.name")+" SET "+string("db.internal.tables.requests.cols.lastStarted")+" = \""+currentDateStr+"\" , "+string("db.internal.tables.requests.cols.totalRuns")+" = "+string("db.internal.tables.requests.cols.totalRuns")+" + 1 , "+string("db.internal.tables.requests.cols.currentState")+" = \""+string("db.internal.tables.requests.defs.defaultRunningState")+"\" where "+string("db.internal.tables.requests.cols.host")+" = \""+host+"\" and "+string("db.internal.tables.requests.cols.port")+" = \""+port+"\" and "+string("db.internal.tables.requests.cols.dbname")+" = \""+dbname+"\" and "+string("db.internal.tables.requests.cols.dbtable")+" = \""+dbtable+"\"")
									val dataSource = transform(request) // send this message
									debug("[DEBUG] request transformed into a @datasource@ . . .")
									val workerActor = pipelineSystem.actorOf(Props(classOf[WorkerActor],config))
									workerActor ! new TableMessage(listener,dataSource,hash)
									debug("[DEBUG] a @@ worker actor @@ assigned")
									//val druidActor = pipelineSystem.actorOf(Props(classOf[DruidActor],config))
									//druidActor ! new DruidMessage(listener,dataSource,hash)

								}
								else{
									debug("[DEBUG] [REQUESTS] [WHILE 1] waiting to run on == "+request)
								}
							}
					}
					else {
						val diffInMinsCurrentStarted = TimeUnit.MINUTES.convert(currentDateDate.getTime() - lastStartedDate.getTime(),TimeUnit.MILLISECONDS)
						val schedulingFrequencyParts = runFrequency.split(" ")
						val freqUnit = schedulingFrequencyParts(0)
						val freqDim = schedulingFrequencyParts(1)
						val freqInMins = getMins(freqUnit,freqDim)
						if(diffInMinsCurrentStarted > freqInMins){
							val hash = randomString(10)
							debug("[DEBUG] hash @redundant@ == "+hash)
							debug("[DEBUG] [REQUESTS] [WHILE 1] it is time to run the script again . . .")
							val statement1 = connection.createStatement()
							statement1.executeUpdate("INSERT INTO `"+string("db.internal.tables.runninglogs.name")+"` (`"+string("db.internal.tables.runninglogs.cols.host")+"`,`"+string("db.internal.tables.runninglogs.cols.port")+"`,`"+string("db.internal.tables.runninglogs.cols.dbname")+"`,`"+string("db.internal.tables.runninglogs.cols.dbtable")+"`,`"+string("db.internal.tables.runninglogs.cols.runTimeStamp")+"`,`"+string("db.internal.tables.runninglogs.cols.hash")+"`,`"+string("db.internal.tables.runninglogs.cols.exceptions")+"`,`"+string("db.internal.tables.runninglogs.cols.notes")+"`) VALUES( '"+host+"','"+port+"','"+dbname+"','"+dbtable+"','"+currentDateStr+"','"+hash+"','none','started the next run . . .')")
							debug("[DEBUG] inserted into running logs (STARTED) . . .")
							val statement2 = connection.createStatement()
							statement2.executeUpdate("UPDATE "+string("db.internal.tables.requests.name")+" SET "+string("db.internal.tables.requests.cols.lastStarted")+" = \""+currentDateStr+"\" , "+string("db.internal.tables.requests.cols.totalRuns")+" = "+string("db.internal.tables.requests.cols.totalRuns")+" + 1 , "+string("db.internal.tables.requests.cols.currentState")+" = \""+string("db.internal.tables.requests.defs.defaultRunningState")+"\" where "+string("db.internal.tables.requests.cols.host")+" = \""+host+"\" and "+string("db.internal.tables.requests.cols.port")+" = \""+port+"\" and "+string("db.internal.tables.requests.cols.dbname")+" = \""+dbname+"\" and "+string("db.internal.tables.requests.cols.dbtable")+" = \""+dbtable+"\"")
							val dataSource = transform(request) // send this message
							debug("[DEBUG] request transformed into a @datasource@ . . .")
							val workerActor = pipelineSystem.actorOf(Props(classOf[WorkerActor],config))
							workerActor ! new TableMessage(listener,dataSource,hash)
							debug("[DEBUG] a @@ worker actor @@ assigned")
							//val druidActor = pipelineSystem.actorOf(Props(classOf[DruidActor],config))
							//druidActor ! new DruidMessage(listener,dataSource,hash)

						}
						else{
							debug("[DEBUG] [REQUESTS] [WHILE 1] waiting to run on == "+request)
						}
					}
				}
			}
		//}

		// get all the records - Requests Table
		//debug("[DEBUG] [REQUESTS TABLE QUERY] SELECT * FROM "+string("db.internal.tables.requests.name"))
		//val resultSet = statement.executeQuery("SELECT * FROM "+string("db.internal.tables.requests.name"))


//    val curator = CuratorFrameworkFactory.builder().connectString(string("sinks.hdfs.zookeeper.url"))
//    .retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
//    .build();
//    curator.start();

//		val lines = KafkaUtils.createStream[Array[Byte], String,
//      DefaultDecoder, StringDecoder](
//      ssc,
//      kafkaConf,
//      Map(topic -> 1),
//      StorageLevel.MEMORY_ONLY_SER).map(_._2) // means we only get the jsons
    /*    
    	/* recieve offsets from the RDD */
      lines.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
     }
    */
    //{u'createdtime': u'2016-07-02 14:29:15.0', u'sourceorderid': u'AB12410922P2085415', u'salesorderid': u'98145973', u'TSpartitionKey': u'dt=2016-07-02', u'website_createtime': u'2016-07-02 14:13:30.0'}
    
//    val druidDataSource: String = topic
//    val dimensions = dataSource.fullTableSchema.split(',').toIndexedSeq
//
//    val aggregators = Seq(new CountAggregatorFactory("cnt"), new LongSumAggregatorFactory("baz", "baz"))
//
//    // Tranquility needs to be able to extract timestamps from your object type (in this case, Map<String, Object>).
//    val timestamper = (eventMap: Map[String, Any]) => new DateTime(eventMap("timestamp"))

    // Tranquility needs to be able to serialize your object type. By default this is done with Jackson. If you want to
    // provide an alternate serializer, you can provide your own via ```.objectWriter(...)```. In this case, we won't
    // provide one, so we're just using Jackson:
//    val druidService = DruidBeams
//    .builder(timestamper)
//    .curator(curator)
//    .discoveryPath(discoveryPath)
//    .location(DruidLocation(indexService, firehosePattern, druidDataSource))
//    .rollup(DruidRollup(SpecificDruidDimensions(dimensions), aggregators, QueryGranularities.ALL))
//    .tuning(
//      ClusteredBeamTuning(
//      segmentGranularity = Granularity.HOUR,
//      windowPeriod = new Period("PT10M"),
//      partitions = 1,
//      replicants = 1
//     )
//    )
//  .buildService()
  
  
      
      
    //val jsonlines = lines.map(jsonStrToMap(_))//lines.map(mapit(_,dataSource.fullTableSchema))
    //val druidAck = jsonlines.map(druidService(Seq(_)))
		    /*
		  else{
		      /*
		       * because this Option 1 is giving a NotSerializationException
		       * 
		      //debug("[DEBUG] mapping the rows of affected PKs one by one / batch maybe onto STATUS table . . .")
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
		  * 
		  */
		  
		  //dataSource.getPKs4UpdationKafka(conf,sqlContext)
		  //dataSource.getPKs4UpdationHDFS(conf,sqlContext)
		  
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
		    //debug("[DEBUG] doing the SQL-SQL flow for == "+dataSource.db+"_"+dataSource.table)
		    //debug("[DEBUG] obtaining the previous, the current and the affected PKs")
        var prevBookMark = dataSource.getPrevBookMark() // get the previous bookmark - key,max(autoincr.) group by key filter key -> bookmark / ts | db.internal.tables.bookmarks.defs.defaultBookMarkValue for first time
		    //debug("[DEBUG] the previous bookmark == "+prevBookMark)
        var currBookMark = dataSource.getCurrBookMark(sqlContext) // get the latest / maximum bookmark / ts | db.internal.tables.bookmarks.defs.defaultBookMarkValue for first time
		    //debug("[DEBUG] the current bookmark == "+currBookMark)
        var PKsAffectedDF = dataSource.getAffectedPKs(sqlContext,prevBookMark,currBookMark) // distinct PKs and max(timestamp) + WHERE clause | same as PKs, timestamp if not a log table, and PKs / timestamp otherwise | could be empty also
		    //debug("[DEBUG] the count of PKs returned == "+PKsAffectedDF.count())
        if(PKsAffectedDF.rdd.isEmpty()){
		      //info("[SQL] no records to upsert in the internal Status Table == for bookmarks : "+prevBookMark+" ==and== "+currBookMark+" for table == "+dataSource.db+"_"+dataSource.table)
		    }
		    else{
		      //debug("[DEBUG] mapping the rows of affected PKs one by one / batch maybe onto STATUS table . . .")
		      PKsAffectedDF.map { x => dataSource.upsert(x(0).toString(),x(1).toString()) } // assuming x(0) / x(1) converts to string with .toString() | insert db / table / primaryKey / sourceId / 0asTargetId
		    }
		    //debug("[DEBUG] updating the current bookmark for this db + table into BOOKMARKS table . . .")
		    dataSource.updateBookMark(currBookMark) // update the bookmark table - done !
	      }(ExecutionContext.Implicits.global) onSuccess { // future onSuccess check
	        case _ => //info("[SQL] [COMPLETED] =/="+dataSource.toString()) // ack message on success
	      }
	      //debug("[DEBUG] [SQL] still executing == "+dataSource.toString())
	      //info("[SQL] [EXECUTING] =/="+dataSource.toString())
	      //sender ! new SQLExecuting(config,request)
	      Future {
		      dataSource.getPKs4UpdationKafka(conf,sqlContext) // or straightaway and get rid of the wrapper func pollAndSink - KAFKA
		      dataSource.getPKs4UpdationHDFS(conf,sqlContext) // or straightaway and get rid of the wrapper func pollAndSink - HDFS
	      }(ExecutionContext.Implicits.global) onSuccess {
	        case _ => //info("[STREAMING] [COMPLETED] =/="+dataSource.toString()) // ack message on success
	      }
	      //debug("[DEBUG] [STREAMING] still executing == "+dataSource.toString())
    	  //info("[STREAMING] [EXECUTING] =/="+dataSource.toString())
    	  */
    //debug("[DEBUG] closing down the internal connection . . .")
		connection.close()
		////debug("[DEBUG] requestsRecordsDB.count() == "+requestsRecordsDB.count())
		// getting just those rows which were 0 i.e not even SQL persistence  (commenting) - filter
		//requestsRecordsDB = requestsRecordsDB.where(requestsRecordsDB(string("db.internal.tables.requests.cols.status"))==="\""+string("db.internal.tables.requests.defs.defaultStatusVal")+"\"")// make sure to put status as 0 and update them also.
		////debug("[DEBUG] filtered ....")
		////debug("[DEBUG] records after filtering on status == "+requestsRecordsDB.count())
		// for all the records fetched above, parse the requests and send them to actors etc.
		////debug("[DEBUG] Sending the records one by one to the Pipeline . . .")
		//requestsRecordsDB.foreach { 
		  // to avoid the Serialization Exceptions
		  //row  => requestHandlerRef ! new StartPipeline(config,row) 
		//}
		// the init process i.e that reads from the db is complete
		//debug("[DEBUG] starting the api service actor . . .")

//		private val api = pipelineSystem.actorOf(Props(classOf[ApiHandler],config), name = string("actorSystem.actors.service"))
//		private implicit val timeout = Timeout(Duration.apply(int("handler.timeout.scalar"), string("handler.timeout.unit")))//from root.server
//    //debug("[DEBUG] on for the transport IO(Http) Actor . . .")
//		private val transport = IO(Http)
//    override def bind {
//      //debug("[DEBUG] binding the api service actor to 127.0.0.1:9999")
//      transport ! Http.Bind(api, interface = string("host"), port = int("port"))
//      //info("server bound: " + string("host") + ":" + int("port"))
//    }
	  
		def mapit(json: String,fullTableSchema: String) = {
      json.substring(1, json.length - 1)
        .split(",")
        .map(_.split(":"))
        .map { case Array(k, v) => (k.substring(1, k.length-1), v.substring(1, v.length-1))}
        .toMap      
    }
		
//	  override def close() {
//	    //debug("[DEBUG] time to shut the api service actor and the entire system / rootserver down. . .")
//      transport ? Http.Unbind
//      pipelineSystem.stop(api)
//      pipelineSystem.shutdown()
//      //info("server shutdown complete: " + string("host") + ":" + int("port"))
//    }

  def transform(request: String) = {
				var request_ = request.replace("u'","\"")
				request_ = request_.replaceAll("'","\"")
		    case class RequestObject(host: String, port: String, user: String, password: String, db: String, table: String, bookmark: String, bookmarkformat: String, primaryKey: String, conntype: String,fullTableSchema: String,partitionCol: String,druidMetrics: String,druidDims: String,runFrequency: String)

    		object RequestJsonProtocol extends DefaultJsonProtocol {
		  	  implicit val RequestFormat = jsonFormat15(RequestObject)
		    }
    		import RequestJsonProtocol._
		    val jsonValue = request_.parseJson
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
				val hdfsPartitionCol = requestObject.partitionCol
				val druidMetrics = requestObject.druidMetrics
				val druidDims = requestObject.druidDims
				val runFrequency = requestObject.runFrequency
				// create a datasource to connect for the above request / json / x
				//debug("[DEBUG] [SQL] instantiating the dataSource . . .")
				val dataSource = new DataSource(config,conntype,host,port,user,password,db,table,bookmark,bookmarkformat,primaryKey,fullTableSchema,hdfsPartitionCol,druidMetrics,druidDims,runFrequency)
		    dataSource
		  }

//  class Upserter(internalURL: String, internalUser: String, internalPassword: String) extends Serializable{
//    lazy val internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword)
//    def upsert(primarykey: String, bookmark: String, db: String, table: String) = {
//       // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
//				val statement = internalConnection.createStatement()
//						val upsertQuery = "INSERT INTO `"+string("db.internal.tables.status.name")+"`("+string("db.internal.tables.status.cols.dbname")+","+string("db.internal.tables.status.cols.table")+",`"+string("db.internal.tables.status.cols.primarykey")+"`,"+string("db.internal.tables.status.cols.sourceId")+","+string("db.internal.tables.status.cols.kafkaTargetId")+string("db.internal.tables.status.cols.hdfsTargetId")+") VALUES( '"+db+"','"+table+"','"+primarykey+"', '"+bookmark+"',"+string("db.internal.tables.status.defs.defaultTargetId")+string("db.internal.tables.status.defs.defaultTargetId")+") ON DUPLICATE KEY UPDATE `"+string("db.internal.tables.status.cols.sourceId")+"` = '"+bookmark+"'"
//						val resultSet = statement.executeQuery(upsertQuery)
//				internalConnection.close()
//    }
//  }

  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
  implicit val formats = org.json4s.DefaultFormats
    import org.json4s.native.JsonMethods._
    parse(jsonStr).extract[Map[String, String]]
  }

	class CustomSparkListener extends SparkListener {
		override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
			debug(s"application ended at time : ${applicationEnd.time}")
		}
		override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit ={
			debug(s"[SPARK LISTENER DEBUGS] application Start app attempt id : ${applicationStart.appAttemptId}")
			debug(s"[SPARK LISTENER DEBUGS] application Start app id : ${applicationStart.appId}")
			debug(s"[SPARK LISTENER DEBUGS] application start app name : ${applicationStart.appName}")
			debug(s"[SPARK LISTENER DEBUGS] applicaton start driver logs : ${applicationStart.driverLogs}")
			debug(s"[SPARK LISTENER DEBUGS] application start spark user : ${applicationStart.sparkUser}")
			debug(s"[SPARK LISTENER DEBUGS] application start time : ${applicationStart.time}")
		}
		override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
			debug(s"[SPARK LISTENER DEBUGS] ${executorAdded.executorId}")
			debug(s"[SPARK LISTENER DEBUGS] ${executorAdded.executorInfo}")
			debug(s"[SPARK LISTENER DEBUGS] ${executorAdded.time}")
		}
		override  def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
			debug(s"[SPARK LISTENER DEBUGS] the executor removed Id : ${executorRemoved.executorId}")
			debug(s"[SPARK LISTENER DEBUGS] the executor removed reason : ${executorRemoved.reason}")
			debug(s"[SPARK LISTENER DEBUGS] the executor temoved at time : ${executorRemoved.time}")
		}

		override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
			debug(s"[SPARK LISTENER DEBUGS] job End id : ${jobEnd.jobId}")
			debug(s"[SPARK LISTENER DEBUGS] job End job Result : ${jobEnd.jobResult}")
			debug(s"[SPARK LISTENER DEBUGS] job End time : ${jobEnd.time}")
		}
		override def onJobStart(jobStart: SparkListenerJobStart) {
			debug(s"[SPARK LISTENER DEBUGS] Job started with properties ${jobStart.properties}")
			debug(s"[SPARK LISTENER DEBUGS] Job started with time ${jobStart.time}")
			debug(s"[SPARK LISTENER DEBUGS] Job started with job id ${jobStart.jobId.toString}")
			debug(s"[SPARK LISTENER DEBUGS] Job started with stage ids ${jobStart.stageIds.toString()}")
			debug(s"[SPARK LISTENER DEBUGS] Job started with stages ${jobStart.stageInfos.size} : $jobStart")
		}

		override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
			debug(s"[SPARK LISTENER DEBUGS] Stage ${stageCompleted.stageInfo.stageId} completed with ${stageCompleted.stageInfo.numTasks} tasks.")
			debug(s"[SPARK LISTENER DEBUGS] Stage details : ${stageCompleted.stageInfo.details.toString}")
			debug(s"[SPARK LISTENER DEBUGS] Stage completion time : ${stageCompleted.stageInfo.completionTime}")
			debug(s"[SPARK LISTENER DEBUGS] Stage details : ${stageCompleted.stageInfo.rddInfos.toString()}")
		}
		override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
			debug(s"[SPARK LISTENER DEBUGS] Stage properties : ${stageSubmitted.properties}")
			debug(s"[SPARK LISTENER DEBUGS] Stage rddInfos : ${stageSubmitted.stageInfo.rddInfos.toString()}")
			debug(s"[SPARK LISTENER DEBUGS] Stage submission Time : ${stageSubmitted.stageInfo.submissionTime}")
			debug(s"[SPARK LISTENER DEBUGS] Stage submission details : ${stageSubmitted.stageInfo.details.toString()}")
		}
		override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
			debug(s"[SPARK LISTENER DEBUGS] task ended reason : ${taskEnd.reason}")
			debug(s"[SPARK LISTENER DEBUGS] task type : ${taskEnd.taskType}")
			debug(s"[SPARK LISTENER DEBUGS] task Metrics : ${taskEnd.taskMetrics}")
			debug(s"[SPARK LISTENER DEBUGS] task Info : ${taskEnd.taskInfo}")
			debug(s"[SPARK LISTENER DEBUGS] task stage Id : ${taskEnd.stageId}")
			debug(s"[SPARK LISTENER DEBUGS] task stage attempt Id : ${taskEnd.stageAttemptId}")
			debug(s"[SPARK LISTENER DEBUGS] task ended reason : ${taskEnd.reason}")
		}
		override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
			debug(s"[SPARK LISTENER DEBUGS] stage Attempt id : ${taskStart.stageAttemptId}")
			debug(s"[SPARK LISTENER DEBUGS] stage Id : ${taskStart.stageId}")
			debug(s"[SPARK LISTENER DEBUGS] task Info : ${taskStart.taskInfo}")
		}
		override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {
			debug(s"[SPARK LISTENER DEBUGS] the unpersist RDD id : ${unpersistRDD.rddId}")
		}
	}

}

sealed trait PipeMessage
case class TableMessage(listener: ActorRef, dataSource: DataSource, hash: String) extends PipeMessage {require(!dataSource.table.isEmpty(), "table field cannot be empty");require(!dataSource.db.isEmpty(), "db field cannot be empty")}
case class DruidMessage(listener: ActorRef, dataSource: DataSource, hash: String) extends PipeMessage {require(!dataSource.table.isEmpty(), "table field cannot be empty");require(!dataSource.db.isEmpty(), "db field cannot be empty")}
case class WorkerDone(dataSource: DataSource) extends PipeMessage
case class WorkerExecuting(dataSource: DataSource) extends PipeMessage