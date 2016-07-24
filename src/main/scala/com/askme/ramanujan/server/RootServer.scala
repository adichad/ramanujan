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

		val internalHost = string("db.internal.url") // get the host from env confs - tables bookmark & status mostly
		val internalPort = string("db.internal.port") // get the port from env confs - tables bookmark & status mostly
		val internalDB = string("db.internal.dbname") // get the port from env confs - tables bookmark & status mostly
	  val internalUser = string("db.internal.user") // get the user from env confs - tables bookmark & status mostly
		val internalPassword = string("db.internal.password") // get the password from env confs - tables bookmark & status mostly
		
		val internalURL: String = string("db.conn.jdbc")+":"+string("db.conn.use")+"://"+internalHost+":"+internalPort+"/"+internalDB // the internal connection DB <status, bookmark, requests> etc
		
		var connection = DriverManager.getConnection(internalURL, internalUser, internalPassword)

	def transform(request: String) = {
		var request_ = request.replace("u'","\"")
		request_ = request_.replaceAll("'","\"")
		case class RequestObject(host: String, port: String, user: String, password: String, db: String, table: String, bookmark: String, bookmarkformat: String, primaryKey: String, conntype: String,fullTableSchema: String,partitionCol: String,druidMetrics: String,druidDims: String,runFrequency: String)

		object RequestJsonProtocol extends DefaultJsonProtocol {
			implicit val RequestFormat = jsonFormat15(RequestObject)
		}
		import RequestJsonProtocol._
		val jsonValue = request_.parseJson
		debug("[MY DEBUG STATEMENTS] [SQL] [transform json value] == "+jsonValue)

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
		//debug("[MY DEBUG STATEMENTS] [SQL] instantiating the dataSource . . .")
		val dataSource = new DataSource(config,conntype,host,port,user,password,db,table,bookmark,bookmarkformat,primaryKey,fullTableSchema,hdfsPartitionCol,druidMetrics,druidDims,runFrequency)
		dataSource
	}

	  def getMins(freqScalar: String, freqUnitMeasure: String): Int = {
		  if(freqUnitMeasure.toLowerCase().contains("db.internal.tables.requests.defs.min")){
				freqScalar.toInt
		  }
			else if(freqUnitMeasure.toLowerCase().contains("db.internal.tables.requests.defs.hour")){
				freqScalar.toInt * 60 // ok if hardcoded
		  }
		  else if(freqUnitMeasure.toLowerCase().contains("db.internal.tables.requests.defs.day")){
				freqScalar.toInt * 1440 // ok if hardcoded
		  }
		  else  if(freqUnitMeasure.toLowerCase().contains("db.internal.tables.requests.defs.week")){
				freqScalar.toInt * 10080 // ok if hardcoded
		  }
		  else {
				debug("[MY DEBUG STATEMENTS] [GET RUN FREQUENCY(in mins)] ambiguous scheduling frequency input, did you mean $something WEEKS ?")
				freqScalar.toInt * 25200 * 4
			}
	  }

	def randomString(length: Int) = scala.util.Random.alphanumeric.take(length).mkString

	def startASpin(host: String,port: String,dbname: String,dbtable: String,currentDateStr: String,runStartMessage: String,request: String) = {
		val hash = randomString(10)
		val insertRecRunningLOGS = "INSERT INTO `"+string("db.internal.tables.runninglogs.name")+"` (`"+string("db.internal.tables.runninglogs.cols.host")+"`,`"+string("db.internal.tables.runninglogs.cols.port")+"`,`"+string("db.internal.tables.runninglogs.cols.dbname")+"`,`"+string("db.internal.tables.runninglogs.cols.dbtable")+"`,`"+string("db.internal.tables.runninglogs.cols.runTimeStamp")+"`,`"+string("db.internal.tables.runninglogs.cols.hash")+"`,`"+string("db.internal.tables.runninglogs.cols.exceptions")+"`,`"+string("db.internal.tables.runninglogs.cols.notes")+"`) VALUES( '"+host+"','"+port+"','"+dbname+"','"+dbtable+"','"+currentDateStr+"','"+hash+"','none','"+runStartMessage+"')"
		val insertRecRunningstatement = connection.createStatement()
		insertRecRunningstatement.executeUpdate(insertRecRunningLOGS)

		val updateRequestsRunningQuery = "UPDATE "+string("db.internal.tables.requests.name")+" SET "+string("db.internal.tables.requests.cols.lastStarted")+" = \""+currentDateStr+"\" , "+string("db.internal.tables.requests.cols.totalRuns")+" = "+string("db.internal.tables.requests.cols.totalRuns")+" + 1 , "+string("db.internal.tables.requests.cols.currentState")+" = \""+string("db.internal.tables.requests.defs.defaultRunningState")+"\" where "+string("db.internal.tables.requests.cols.host")+" = \""+host+"\" and "+string("db.internal.tables.requests.cols.port")+" = \""+port+"\" and "+string("db.internal.tables.requests.cols.dbname")+" = \""+dbname+"\" and "+string("db.internal.tables.requests.cols.dbtable")+" = \""+dbtable+"\""
		val updateRequestsRunningstatement = connection.createStatement()
		updateRequestsRunningstatement.executeUpdate(updateRequestsRunningQuery)

		val dataSource = transform(request)

		val workerActor = pipelineSystem.actorOf(Props(classOf[WorkerActor],config))
		workerActor ! new TableMessage(listener,dataSource,hash)
		//val druidActor = pipelineSystem.actorOf(Props(classOf[DruidActor],config))
		//druidActor ! new DruidMessage(listener,dataSource,hash)
	}

	while(true) {
			val statement = connection.createStatement()
			val getAllRequestsQuery = "SELECT * FROM "+string("db.internal.tables.requests.name")
			val resultSet = statement.executeQuery(getAllRequestsQuery)

			while(resultSet.next()){
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
				val currentState = resultSet.getString(string("db.internal.tables.requests.cols.currentState"))
				val exceptions = resultSet.getString(string("db.internal.tables.requests.cols.exceptions"))
				val notes = resultSet.getString(string("db.internal.tables.requests.cols.notes"))

				debug("[MY DEBUG STATEMENTS] [REQUESTS] [WHILE 1] current dataSource == "+request.toString())

				// val format = new SimpleDateFormat"%Y-%m-%d %H:%M:%S")
				val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
				val lastStartedDate = format.parse(lastStarted)
				val lastEndedDate = format.parse(lastEnded)

				val currentDateDate = Calendar.getInstance().getTime()
				val currentDateStr = format.format(currentDateDate)

				if(totalRuns.toInt == 0){ // start it. the first time
					val runStartMessage = "starting the first Run Ever . . ."
					startASpin(host,port,dbname,dbtable,currentDateStr,runStartMessage,request)
				}
				else{
					val diffInMinsEndedStarted = TimeUnit.MINUTES.convert(lastEndedDate.getTime() - lastStartedDate.getTime(),TimeUnit.MILLISECONDS)
					if(diffInMinsEndedStarted < 0){ // it must be either still running or failed !
							if(currentState == string("db.internal.tables.requests.defs.defaultRunningState")){
								debug("[MY DEBUG STATEMENTS] [REQUESTS] [WHILE 1] the previous run is not complete yet, it is still running without fail so far . . .")
								debug("[MY DEBUG STATEMENTS] [REQUESTS] [WHILE 1] still executing == "+request)
								debug("[MY DEBUG STATEMENTS] [REQUESTS] [WHILE 1] current state == "+currentState)
							}
							else{
								debug("[MY DEBUG STATEMENTS] [REQUESTS] [WHILE 1] [ALERT] encountered some exception in last run == "+exceptions+" for dataSource == "+request)
								debug("[MY DEBUG STATEMENTS] ##re-running , ##re-trying . . .$ db == "+dbname+" @and@ $ table == "+dbtable)

								val diffInMinsCurrentStarted = TimeUnit.MINUTES.convert(currentDateDate.getTime() - lastStartedDate.getTime(),TimeUnit.MILLISECONDS)

								val schedulingFrequencyParts = runFrequency.trim().split(" ")

								val freqScalar = schedulingFrequencyParts(0)
								val freqUnitMeasure = schedulingFrequencyParts(1)
								val freqInMins = getMins(freqScalar,freqUnitMeasure)
								debug("[MY DEBUG STATEMENTS] the scheduling frequency == "+runFrequency+" || got converted into mins() : "+freqInMins)

								if(diffInMinsCurrentStarted > freqInMins){
									val runStartMessage = "starting the Next Run | previous NOT OK . . ."
									startASpin(host,port,dbname,dbtable,currentDateStr,runStartMessage,request)
								}
								else{
									debug("[MY DEBUG STATEMENTS] [REQUESTS] [WHILE 1] [previous NOT OK] waiting to run on == "+request)
								}
							}
					}
					else {
						val diffInMinsCurrentStarted = TimeUnit.MINUTES.convert(currentDateDate.getTime() - lastStartedDate.getTime(),TimeUnit.MILLISECONDS)
						val schedulingFrequencyParts = runFrequency.trim().split(" ")

						val freqScalar = schedulingFrequencyParts(0)
						val freqUnitMeasure = schedulingFrequencyParts(1)
						val freqInMins = getMins(freqScalar,freqUnitMeasure)

						if(diffInMinsCurrentStarted > freqInMins){
							val runStartMessage = "starting the Next Run | previous OK . . ."
							startASpin(host,port,dbname,dbtable,currentDateStr,runStartMessage,request)
						}
						else{
							debug("[MY DEBUG STATEMENTS] [REQUESTS] [WHILE 1] [previous OK] waiting to run on == "+request)
						}
					}
				}
			}
		}
    //debug("[MY DEBUG STATEMENTS] closing down the internal connection . . .")
		connection.close()

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