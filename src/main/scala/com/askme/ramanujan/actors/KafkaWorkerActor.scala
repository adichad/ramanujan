package com.askme.ramanujan.actors

import java.sql.{Connection, DriverManager}
import java.util.concurrent.Executors

import akka.actor.Actor
import com.askme.ramanujan.Configurable
import com.askme.ramanujan.server.KafkaMessage
import com.askme.ramanujan.util.KafkaSource
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import kafka.serializer.StringDecoder
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.{Failure, Success}

/**
  * Created by Sumit on 31/07/16.
  */
class KafkaWorkerActor(val config: Config) extends Actor with Configurable with Logging {
  val conf = sparkConf("spark")
  val sc = SparkContext.getOrCreate(conf)
  val sqlContext = new SQLContext(sc)

  def getDFCols(dfSchema: StructType) = {
    val dfColLen = dfSchema.length
    var hiveTableschema = scala.collection.mutable.MutableList[String]()
    for(i <- 0 until dfColLen){
      val schema = dfSchema(i)
      val varname = schema.name.toString
      hiveTableschema += varname
    }
    hiveTableschema.mkString(" , ")
  }

  def makePartitionsManageHive(keys: Array[Any], byPartitionArray: Array[DataFrame], cluster: String, topic: String, alias: String, kafkaSource: KafkaSource, hiveCon: Connection) = {
    for (i <- 0 to (keys.length - 1)) {
      debug("[MY DEBUG TEMP] " +keys(i).toString())
      val partKey_ = keys(i).toString().split("=")(0)
      val key_ = keys(i).toString().split("=")(1)

      val df_ = byPartitionArray(i)

      val dfSchema = df_.schema

      val parentHDFSPath = "hdfs://"+string("sinks.hdfs.url")+":"+string("sinks.hdfs.port")+"/parquet1_"+cluster+"_"+topic+"_"+alias

      val hdfspath = "hdfs://"+string("sinks.hdfs.url")+":"+string("sinks.hdfs.port")+"/parquet1_"+cluster + "_" +topic + "_" + alias + "/partitioned_on_" + keys(i).toString()
      val hdfspathTemp = "hdfs://"+string("sinks.hdfs.url")+":"+string("sinks.hdfs.port")+"/parquet1_"+cluster + "_" +topic + "_" + alias + "_tmp/partitioned_on_" + keys(i).toString()

      val hconf = sc.hadoopConfiguration

      hconf.set("fs.default.name", "hdfs://" + string("sinks.hdfs.url"))
      hconf.set("fs.defaultFS", "hdfs://" + string("sinks.hdfs.url"))

      val hdfs = org.apache.hadoop.fs.FileSystem.get(hconf)

      val parentPathExistsBefore = hdfs.exists(new Path(parentHDFSPath))

      if(!parentPathExistsBefore){
        hdfs.mkdirs(new Path(parentHDFSPath))
        //val parentTableCreateQuery = "CREATE TABLE IF NOT EXISTS hive_table_tsv_"+(kafkaSource.cluster).replaceAll("[^A-Za-z0-9]", "_")+"_"+(kafkaSource.topic).replaceAll("[^A-Za-z0-9]", "_")+"_"+(kafkaSource.alias).replaceAll("[^A-Za-z0-9]", "_")+" ("+kafkaSource.getColAndType()+") PARTITIONED BY (partitioned_on_"+kafkaSource.hdfsPartitionCol+" STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' LOCATION '"+parentHDFSPath+"'" // STORED AS PARQUET LOCATION '"+parentHDFSPath+"'"
        val parentTableCreateQuery = "CREATE EXTERNAL TABLE IF NOT EXISTS hive_table_parquet1_"+(kafkaSource.cluster).replaceAll("[^A-Za-z0-9]", "_")+"_"+(kafkaSource.topic).replaceAll("[^A-Za-z0-9]", "_")+"_"+(kafkaSource.alias).replaceAll("[^A-Za-z0-9]", "_")+" ("+kafkaSource.getColAndType(dfSchema)+") PARTITIONED BY (partitioned_on_"+kafkaSource.hdfsPartitionCol+" STRING) STORED AS PARQUET LOCATION '"+parentHDFSPath+"'" // STORED AS PARQUET LOCATION '"+parentHDFSPath+"'"

        debug("[MY DEBUG STATEMENTS] [CREATE TABLES] [HIVE QUERY] == "+parentTableCreateQuery)
        val hiveCreateTableStmt = hiveCon.createStatement()
        val createTableRes = hiveCreateTableStmt.execute(parentTableCreateQuery)

        debug("[MY DEBUG STATEMENTS] [CREATE TABLES] create table statement executed . . . ")
        //hiveContext.sql(parentTableCreateQuery)
      }
      else{
        debug("[MY DEBUG STATEMENTS] [CREATE TABLES] =/= parent hdfs path was already there . . .")
      }
      val partitionExistsBefore = hdfs.exists(new Path(hdfspath))

      if (partitionExistsBefore) {
        debug("[MY DEBUG STATEMENTS] [HDFS] [DUMP] The path was present before == " + hdfspath)
        val partitionb4df = sqlContext.read.parquet(hdfspath).toDF(getDFCols(dfSchema).split(','): _*) //(dataSource.fullTableSchema)
        //val partitionb4df = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema","true").option("delimiter","\t").load(hdfspath)

        val w = Window.partitionBy(col(kafkaSource.primaryKey)).orderBy(col((kafkaSource.bookmark)).desc)
        val df_dedupe_ = df_.sort(col(kafkaSource.primaryKey),col(kafkaSource.bookmark).desc).dropDuplicates(Seq(kafkaSource.primaryKey))

        val affectedPKs = df_dedupe_.select(kafkaSource.primaryKey).rdd.map(r => r(0).toString()).collect() //r(0).asInstanceOf[String]).collect()
        val sc = SparkContext.getOrCreate(conf)
        val affectedPKsBrdcst = sc.broadcast(affectedPKs)

        val filterFunc: (String => Boolean) = (arg: String) => !affectedPKsBrdcst.value.contains(arg)
        val sqlfunc = udf(filterFunc)
        val prunedPartitionb4df = partitionb4df.filter(sqlfunc(col(kafkaSource.primaryKey)))

        val persistPartitionAfterdf = prunedPartitionb4df.unionAll(df_dedupe_)
        persistPartitionAfterdf.write.format("parquet").mode("overwrite").save(hdfspathTemp)
        //persistPartitionAfterdf.write.format("com.databricks.spark.csv").option("delimiter","\t").save(hdfspathTemp)

        hdfs.delete(new org.apache.hadoop.fs.Path(hdfspath),true)
        hdfs.rename(new org.apache.hadoop.fs.Path(hdfspathTemp),new org.apache.hadoop.fs.Path(hdfspath))
        hdfs.delete(new org.apache.hadoop.fs.Path(hdfspathTemp),true)

        affectedPKsBrdcst.unpersist()
        //affectedPKsBrdcst.destroy()
      }
      else {
        debug("[MY DEBUG STATEMENTS] [HDFS] [DUMP] The path was not present before == " + hdfspath)
        //val w = Window.partitionBy(col(kafkaSource.primaryKey)).orderBy(col((kafkaSource.bookmark)).desc)
        val df_dedupe_ = df_.sort(col(kafkaSource.primaryKey),col(kafkaSource.bookmark).desc).dropDuplicates(Seq(kafkaSource.primaryKey))

        df_dedupe_.write.format("parquet").mode("overwrite").save(hdfspath)
        //df_dedupe_.write.format("com.databricks.spark.csv").option("delimiter","\t").save(hdfspath)

        //val partitionAddQuery = "ALTER TABLE hive_table_"+(dataSource.host).replaceAll("[^A-Za-z0-9]", "_")+"_"+(dataSource.db).replaceAll("[^A-Za-z0-9]", "_")+"_"+(dataSource.table).replaceAll("[^A-Za-z0-9]", "_")+" ADD PARTITION (partitioned_on_"+(partKey_)+"='"+(key_)+"') location '"+hdfspath+"'"
        val partitionAddQuery = "ALTER TABLE hive_table_parquet1_"+(kafkaSource.cluster).replaceAll("[^A-Za-z0-9]", "_")+"_"+(kafkaSource.topic).replaceAll("[^A-Za-z0-9]", "_")+"_"+(kafkaSource.alias).replaceAll("[^A-Za-z0-9]", "_")+" ADD PARTITION (partitioned_on_"+(partKey_)+"='"+(key_)+"') location '"+hdfspath+"'"
        debug("[MY DEBUG STATEMENTS] [ALTER TABLES] [HIVE QUERY] == "+partitionAddQuery)
        val hiveAddPartitionStmt = hiveCon.createStatement()
        val addPartitionHiveRes = hiveAddPartitionStmt.execute(partitionAddQuery)
        //hiveContext.sql(partitionAddQuery)
      }
    }
  }

  def sinkKafkaHdfsHive(frame: DataFrame, kafkaSource: KafkaSource, hiveCon: Connection) = {
    import sqlContext.implicits._
    val keys: Array[Any] = frame.select("partition").distinct.collect.flatMap(_.toSeq)
    val byPartitionArray: Array[DataFrame] = keys.map(key => frame.where($"partition" <=> key))
    val cluster = kafkaSource.cluster
    val topic = kafkaSource.topic
    val alias = kafkaSource.alias

    makePartitionsManageHive(keys,byPartitionArray,cluster,topic, alias, kafkaSource,hiveCon)
  }

  override def receive = {

    case KafkaMessage(listener, kafkaSource, hash) => {

      listener ! "[MY DEBUG STATEMENTS] [SINK RUNS] Starting to sink the following datasource == " + kafkaSource.toString()

      val hiveDriver = string("db.conn.hive.driver")
      Class.forName(hiveDriver)

      val hiveCon = DriverManager.getConnection(string("db.conn.jdbc")+":"+string("db.conn.hive.version")+"://"+string("db.conn.hive.host")+":"+string("db.conn.hive.port")+"/"+string("db.conn.hive.defaultdb"),"APP","abc123")
      import scala.concurrent._
      val numberOfCPUs = sys.runtime.availableProcessors()
      val threadPool = Executors.newFixedThreadPool(numberOfCPUs)
      implicit val ec = ExecutionContext.fromExecutorService(threadPool)

      listener ! "throwing the datasource == " + kafkaSource.toString() + " to a future . . ."

      val ssc = new StreamingContext(sc, Seconds(int("streaming.kafka.batchDuration")))

      val cluster = kafkaSource.cluster
      val topic = kafkaSource.topic

      val topicsSet = topic.split(",").toSet

      val future = Future {
        var kafkaParams = Map[String,String]()
        if(cluster == string("streaming.kafka.cluster.type.staging")) {
          kafkaParams = Map[String, String]("metadata.broker.list" -> "kafka01.staging.askmebazaar.com:9092,kafka02.staging.askmebazaar.com:9092,kafka03.staging.askmebazaar.com:9092") //string("streaming.kafka.cluster.brokers.staging"))
        }
        else{
          kafkaParams = Map[String, String]("metadata.broker.list" -> "kafka01.production.askmebazaar.com:9092,kafka02.production.askmebazaar.com:9092,kafka03.production.askmebazaar.com:9092") //string("streaming.kafka.cluster.brokers.production"))
        }
        val streamingMessages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaParams, topicsSet)

        val jsonRecs: DStream[String] = streamingMessages.map(_._2)

        jsonRecs.foreachRDD(jsonRDD => {
          val rddData: DataFrame = sqlContext.read.json(jsonRDD)
          val RDDdataCount: Long = rddData.count()
          debug("[MY DEBUG STATEMENTS] [FUTURE] [RUNNING] [KAFKA] {{" + hash + "}} the count of PKs returned == " + RDDdataCount + "##for## datasource == " + kafkaSource.toString())
          if (RDDdataCount == 0) {
            debug("[MY DEBUG STATEMENTS] [FUTURE] [RUNNING] [KAFKA] {{" + hash + "}} no records for the current rdd being examined ... all for topic == "+kafkaSource.toString())
          }
          else{
            val rddDataConverted = kafkaSource.convertTargetTypes(rddData)
            if (kafkaSource.hdfsPartitionCol.isEmpty() || kafkaSource.hdfsPartitionCol == kafkaSource.bookmark) {
              debug("[MY DEBUG STATEMENTS] [FUTURE] [RUNNING] {{" + hash + "}} picking up the default partition column == " + kafkaSource.bookmark)
              val partitionCol = kafkaSource.bookmark
              val partitionColFunc = udf((ts: String, partKey: String) => {
                try {
                  partKey + "=" + ts.substring(0, math.min(math.max(1, ts.length()), 10)).replaceAll("[^A-Za-z0-9]", "_") // 10 digit partition size
                } catch {
                  case _: Throwable => {
                    partKey + "=defaultNullPartition"
                  }
                }
              })
              val rddDataConverted_partition = rddDataConverted.withColumn("partition", partitionColFunc(col(kafkaSource.bookmark), lit(partitionCol)))

              sinkKafkaHdfsHive(rddDataConverted_partition: DataFrame, kafkaSource, hiveCon)
            }
          }
        })
          sys.ShutdownHookThread {
          debug("[MY DEBUG STATEMENTS] [STREAMING] [SIGTERM KILL] [GRACEFUL SHUTDOWN] Gracefully stopping Spark Streaming Application")
          ssc.stop(true, true)
          debug("[MY DEBUG STATEMENTS] [STREAMING] [SIGTERM KILL] [GRACEFUL SHUTDOWN] Application stopped")
        }

        ssc.start()
        ssc.awaitTermination()

    }(ExecutionContext.Implicits.global) onComplete {
        case Success(value) => {
          listener ! "[MY DEBUG STATEMENTS] [FUTURE] [COMPLETE] Completed Successfully a run for the datasource == " + kafkaSource.toString()
          debug("[MY DEBUG STATEMENTS] inserting/updating the running logs and current bookmark for this db + table into BOOKMARKS table . . .")
          kafkaSource.insertInKafkaRunLogsPassed(hash)
          kafkaSource.updateInKafkaRequestsPassed(hash)
        }
        case Failure(e) => {
          debug("[MY DEBUG STATEMENTS] [FUTURE] [FAILURE] [EXCEPTION] execution failed for data source == "+kafkaSource.toString()+ " @and@ hash == "+hash)
          debug("[MY DEBUG STATEMENTS] [FUTURE] [FAILURE] [EXCEPTION] reasons for failure data source == "+kafkaSource.toString()+ " @and@ hash == "+hash + " was ### "+e)
          e.printStackTrace
          kafkaSource.insertInKafkaRunLogsFailed(hash,e.asInstanceOf[Exception])
          kafkaSource.updateInKafkaRequestsFailed(hash,e.asInstanceOf[Exception])

        }
      }
      listener ! "[MY DEBUG STATEMENTS] [FUTURE] [EXECUTING] Executing a run for the datasource == " + kafkaSource.toString()
    }
  }
}