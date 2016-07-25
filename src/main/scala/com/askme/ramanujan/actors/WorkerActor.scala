package com.askme.ramanujan.actors

import java.sql.{Connection, DriverManager}
import java.util.Properties
import java.util.concurrent.Executors

import akka.actor.Actor
import com.askme.ramanujan.Configurable
import com.askme.ramanujan.server.TableMessage
import com.askme.ramanujan.util.DataSource
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.{Failure, Success}

/**
  * Created by Sumit on 18/07/16.
  */
class WorkerActor(val config: Config) extends Actor with Configurable with Logging {
  val conf = sparkConf("spark")
  val sc = SparkContext.getOrCreate(conf)
  val sqlContext = new SQLContext(sc)

  def sinkToKaphka(PKsAffectedDF_json: RDD[String],dbname: String,dbtable: String) = {
    PKsAffectedDF_json.foreachPartition {
      partitionOfRecords => {
        val props = new Properties()
        props.put("bootstrap.servers", "kafka01.production.askmebazaar.com:9092,kafka02.production.askmebazaar.com:9092,kafka03.production.askmebazaar.com:9092")
        //props.put("metadata.broker.list", "localhost:9092")//"kafka01.production.askmebazaar.com:2181,kafka02.production.askmebazaar.com:2181,kafka03.production.askmebazaar.com:2181")
        props.put("group.id", "ramanujan")
        props.put("producer.type", "async")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("request.required.acks", "1")
        props.put("auto.create.topics.enable", "true")

        val producer = new KafkaProducer[String, String](props)
        partitionOfRecords.foreach {
          case x: String => {
            val message = new ProducerRecord[String, String]("TOPIC_" + dbname + "_" + dbtable, dbname, x)
            producer.send(message).get()
          }
        }
        producer.close()
      }
    }
  }

  def makePartitionsManageHive(keys: Array[Any],byPartitionArray: Array[DataFrame],dbname: String, dbtable: String, dataSource: DataSource,hiveCon: Connection) = {
    for (i <- 0 to (keys.length - 1)) {
      val partKey_ = keys(i).toString().split("=")(0)
      val key_ = keys(i).toString().split("=")(1)

      val df_ = byPartitionArray(i)
      //val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

      val parentHDFSPath = "hdfs://"+string("sinks.hdfs.url")+":"+string("sinks.hdfs.port")+"/"+dbname+"_"+dbtable

      val hdfspath = "hdfs://"+string("sinks.hdfs.url")+":"+string("sinks.hdfs.port")+"/"+dbname + "_" + dbtable + "/partitioned_on_" + keys(i).toString()
      val hdfspathTemp = "hdfs://"+string("sinks.hdfs.url")+":"+string("sinks.hdfs.port")+"/"+dbname + "_" + dbtable + "_tmp/partitioned_on_" + keys(i).toString()

      val hconf = sc.hadoopConfiguration

      hconf.set("fs.default.name", "hdfs://" + string("sinks.hdfs.url") + ":" + string("sinks.hdfs.port"))
      hconf.set("fs.defaultFS", "hdfs://" + string("sinks.hdfs.url") + ":" + string("sinks.hdfs.port"))

      val hdfs = org.apache.hadoop.fs.FileSystem.get(hconf)

      val parentPathExistsBefore = hdfs.exists(new Path(parentHDFSPath))

      if(!parentPathExistsBefore){
        hdfs.mkdirs(new Path(parentHDFSPath))
        val parentTableCreateQuery = "CREATE TABLE IF NOT EXISTS hive_table_"+(dataSource.host).replaceAll("[^a-zA-Z]", "")+"_"+(dataSource.db).replaceAll("[^a-zA-Z]", "")+"_"+(dataSource.table).replaceAll("[^a-zA-Z]", "")+" ("+dataSource.getColAndType()+") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' COMMENT '"+dataSource.host+"_"+dataSource.db+"_"+dataSource.table+"' PARTITIONED BY (partitioned_on_"+dataSource.hdfsPartitionCol+" STRING)"// STORED AS PARQUET LOCATION '"+parentHDFSPath+"'"
        val hiveCreateTableStmt = hiveCon.createStatement()
        val createTableRes = hiveCreateTableStmt.execute(parentTableCreateQuery)

        debug("[MY DEBUG STATEMENTS] [CREATE TABLES] [HIVE QUERY] == "+parentTableCreateQuery)
        debug("[MY DEBUG STATEMENTS] [CREATE TABLES] create table statement executed . . . ")
        //hiveContext.sql(parentTableCreateQuery)
      }
      else{
        debug("[MY DEBUG STATEMENTS] [CREATE TABLES] =/= parent hdfs path was already there . . .")
      }

      val partitionExistsBefore = hdfs.exists(new Path(hdfspath))

      if (partitionExistsBefore) {
        debug("[MY DEBUG STATEMENTS] [HDFS] [DUMP] The path was present before == " + hdfspath)
        //val partitionb4df = sqlContext.read.parquet(hdfspath).toDF((dataSource.fullTableSchema + ",partition").split(','): _*) //(dataSource.fullTableSchema)
        val partitionb4df = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema","true").option("delimiter","\\t").load(hdfspath)

        val w = Window.partitionBy(col(dataSource.primarykey)).orderBy(col((dataSource.bookmark)).desc)
        val df_dedupe_ = df_.sort(col(dataSource.primarykey),col(dataSource.bookmark).desc).dropDuplicates(Seq(dataSource.primarykey))

        val affectedPKs = df_dedupe_.select(dataSource.primarykey).rdd.map(r => r(0).asInstanceOf[String]).collect()
        val sc = SparkContext.getOrCreate(conf)
        val affectedPKsBrdcst = sc.broadcast(affectedPKs)

        val filterFunc: (String => Boolean) = (arg: String) => !affectedPKsBrdcst.value.contains(arg)
        val sqlfunc = udf(filterFunc)
        val prunedPartitionb4df = partitionb4df.filter(sqlfunc(col(dataSource.primarykey)))

        val persistPartitionAfterdf = prunedPartitionb4df.unionAll(df_dedupe_)
        //persistPartitionAfterdf.write.format("parquet").mode("overwrite").save(hdfspathTemp)
        persistPartitionAfterdf.write.format("com.databricks.spark.csv").option("delimiter","\\t").save(hdfspathTemp)

        hdfs.delete(new org.apache.hadoop.fs.Path(hdfspath),true)
        hdfs.rename(new org.apache.hadoop.fs.Path(hdfspathTemp),new org.apache.hadoop.fs.Path(hdfspath))
        hdfs.delete(new org.apache.hadoop.fs.Path(hdfspathTemp),true)

        //affectedPKsBrdcst.destroy()
      }
      else {
        debug("[MY DEBUG STATEMENTS] [HDFS] [DUMP] The path was not present before == " + hdfspath)
        val w = Window.partitionBy(col(dataSource.primarykey)).orderBy(col((dataSource.bookmark)).desc)
        val df_dedupe_ = df_.sort(col(dataSource.primarykey),col(dataSource.bookmark).desc).dropDuplicates(Seq(dataSource.primarykey))

        //df_dedupe_.write.format("parquet").mode("overwrite").save(hdfspath)
        df_dedupe_.write.format("com.databricks.spark.csv").option("delimiter","\\t").save(hdfspath)

        val partitionAddQuery = "ALTER TABLE hive_table_"+(dataSource.host).replaceAll("[^a-zA-Z]", "")+"_"+(dataSource.db).replaceAll("[^a-zA-Z]", "")+"_"+(dataSource.table).replaceAll("[^a-zA-Z]", "")+" ADD PARTITION (partitioned_on_"+(partKey_)+"='"+(key_)+"') location '"+hdfspath+"'"
        val hiveAddPartitionStmt = hiveCon.createStatement()
        val addPartitionHiveRes = hiveAddPartitionStmt.execute(partitionAddQuery)
        debug("[HIVE QUERY] == "+partitionAddQuery)
        //hiveContext.sql(partitionAddQuery)
      }
    }
  }

  def sinkKafkaHdfsHive(PKsAffectedDF_partition: DataFrame, dataSource: DataSource, hiveCon: Connection) = {
    import sqlContext.implicits._
    val keys: Array[Any] = PKsAffectedDF_partition.select("partition").distinct.collect.flatMap(_.toSeq)
    val byPartitionArray: Array[DataFrame] = keys.map(key => PKsAffectedDF_partition.where($"partition" <=> key))
    val dbname = dataSource.db
    val dbtable = dataSource.table

    val PKsAffectedDF_json = PKsAffectedDF_partition.toJSON

    sinkToKaphka(PKsAffectedDF_json,dbname,dbtable)

    makePartitionsManageHive(keys,byPartitionArray,dbname, dbtable, dataSource,hiveCon)
  }

  override def receive = {

    case TableMessage(listener, dataSource, hash) => {

      listener ! "[MY DEBUG STATEMENTS] [SINK RUNS]Starting to sink the following datasource == " + dataSource.toString()

      val hiveDriver = string("db.conn.hive.driver")
      Class.forName(hiveDriver)

      //val hiveCon = DriverManager.getConnection("jdbc:hive2://localhost:10000/default")
      val hiveCon = DriverManager.getConnection(string("db.conn.jdbc")+":"+string("db.conn.hive.version")+"://"+string("db.conn.hive.host")+":"+string("db.conn.hive.port")+"/"+string("db.conn.hive.defaultdb"))

      import scala.concurrent._
      val numberOfCPUs = sys.runtime.availableProcessors()
      val threadPool = Executors.newFixedThreadPool(numberOfCPUs)
      implicit val ec = ExecutionContext.fromExecutorService(threadPool)

      listener ! "throwing the datasource == " + dataSource.toString() + " to a future . . ."

      var prevBookMark = ""
      var currBookMark = ""

      val future = Future {
        //try {
          prevBookMark = dataSource.getPrevBookMark()
          debug("[MY DEBUG STATEMENTS] [FUTURE] [RUNNING] {{" + hash + "}} the previous bookmark == " + prevBookMark + "##for## datasource == " + dataSource.toString())
          currBookMark = dataSource.getCurrBookMark()
          debug("[MY DEBUG STATEMENTS] [FUTURE] [RUNNING] {{" + hash + "}} the current bookmark == " + currBookMark + "##for## datasource == " + dataSource.toString())

          val PKsAffectedDF: DataFrame = dataSource.getAffectedPKs(prevBookMark, currBookMark)
          val affectedPKsCount: Long = PKsAffectedDF.count()
          debug("[MY DEBUG STATEMENTS] [FUTURE] [RUNNING] {{" + hash + "}} the count of PKs returned == " + affectedPKsCount + "##for## datasource == " + dataSource.toString())

          if (affectedPKsCount == 0) {
            info("[MY DEBUG STATEMENTS] [FUTURE] [RUNNING] {{" + hash + "}} no records to upsert in the internal Status Table == for bookmarks : " + prevBookMark + " ==and== " + currBookMark + " for table == " + dataSource.db + "_" + dataSource.table + " @host@ == " + dataSource.host)
          }

          if (dataSource.hdfsPartitionCol.isEmpty()) {
            debug("[MY DEBUG STATEMENTS] [FUTURE] [RUNNING] {{" + hash + "}} picking up the default partition column == " + dataSource.bookmark)
            val partitionCol = dataSource.bookmark
            val partitionColFunc = udf((ts: String, partKey: String) => {
              partKey + "='" + ts.replaceAll(" ", "_").substring(0, int("sinks.hdfs.tsPartitionEnd")) + "'" // 10 digit partition size
            })
            val PKsAffectedDF_partition = PKsAffectedDF.withColumn("partition", partitionColFunc(col(dataSource.bookmark), lit("dt")))

            sinkKafkaHdfsHive(PKsAffectedDF_partition: DataFrame, dataSource, hiveCon)

            dataSource.insertInRunLogsPassed(hash)
            dataSource.updateInRequestsPassed(hash)
            dataSource.updateBookMark(currBookMark)
            "[MY DEBUG STATEMENTS] run completed."
          }
          else {
            debug("[MY DEBUG STATEMENTS] [FUTURE] [RUNNING] {{" + hash + "}} picking up the input partition column == " + dataSource.hdfsPartitionCol)
            val partitionCol = dataSource.hdfsPartitionCol
            val partitionColFunc = udf((colName: String, partKey: String) => {
              try {
                partKey + "=" + colName
              } catch {
                case _: Throwable => {
                  partKey + "=defaultNullPartition"
                }
              }
            })
            val PKsAffectedDF_partition: DataFrame = PKsAffectedDF.withColumn("partition", partitionColFunc(col(dataSource.hdfsPartitionCol), lit(dataSource.hdfsPartitionCol)))

            sinkKafkaHdfsHive(PKsAffectedDF_partition: DataFrame, dataSource, hiveCon)

            debug("[MY DEBUG STATEMENTS] inserting/updating the running logs and current bookmark for this db + table into BOOKMARKS table . . .")
            //dataSource.insertInRunLogsPassed(hash)
            //dataSource.updateInRequestsPassed(hash)
            //dataSource.updateBookMark(currBookMark)
            "[MY DEBUG STATEMENTS] run completed."
          }
        //} catch {
        //  case e : Exception => {
        //    debug("[MY DEBUG STATEMENTS] [FUTURE] [FAILURE] [EXCEPTION] execution failed for data source == "+dataSource.toString()+ " @and@ hash == "+hash)
        //    debug("[MY DEBUG STATEMENTS] [FUTURE] [FAILURE] [EXCEPTION] reasons for failure data source == "+dataSource.toString()+ " @and@ hash == "+hash + " was ### "+e)
        //    e.printStackTrace
        //    dataSource.insertInRunLogsFailed(hash,e)
        //    dataSource.updateInRequestsFailed(hash,e)
        //  }
        //}
      }(ExecutionContext.Implicits.global) onComplete {
        case Success(value) => {
          listener ! "[MY DEBUG STATEMENTS] [FUTURE] [COMPLETE] Completed Successfully a run for the datasource == " + dataSource.toString()
          dataSource.insertInRunLogsPassed(hash)
          dataSource.updateInRequestsPassed(hash)
          dataSource.updateBookMark(currBookMark)
        }
        case Failure(e) => {
          debug("[MY DEBUG STATEMENTS] [FUTURE] [FAILURE] [EXCEPTION] execution failed for data source == "+dataSource.toString()+ " @and@ hash == "+hash)
          debug("[MY DEBUG STATEMENTS] [FUTURE] [FAILURE] [EXCEPTION] reasons for failure data source == "+dataSource.toString()+ " @and@ hash == "+hash + " was ### "+e)
          e.printStackTrace
          dataSource.insertInRunLogsFailed(hash,e.asInstanceOf[Exception])
          dataSource.updateInRequestsFailed(hash,e.asInstanceOf[Exception])

        }
      }
      listener ! "[MY DEBUG STATEMENTS] [FUTURE] [EXECUTING] Completed Successfully a run for the datasource == " + dataSource.toString()
    }
  }
}
