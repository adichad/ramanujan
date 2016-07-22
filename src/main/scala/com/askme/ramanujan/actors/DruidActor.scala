package com.askme.ramanujan.actors

import java.util.concurrent.Executors

import akka.actor.Actor
import com.askme.ramanujan.Configurable
import com.askme.ramanujan.server.DruidMessage
import com.metamx.common.Granularity
import com.metamx.tranquility.beam.ClusteredBeamTuning
import com.metamx.tranquility.druid.{DruidBeams, DruidLocation, DruidRollup, SpecificDruidDimensions}
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import io.druid.granularity.QueryGranularities
import io.druid.query.aggregation.{CountAggregatorFactory, LongSumAggregatorFactory}
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.{DateTime, Period}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by Sumit on 18/07/16.
  */
class DruidActor(val config: Config) extends Actor with Configurable with Logging{
  val conf = sparkConf("spark")
  val sc = SparkContext.getOrCreate(conf)
  val sqlContext = new SQLContext(sc)

  val indexService = string("sinks.druid.overlord")//"overlord" // Your overlord's druid.service, with slashes replaced by colons.
  val firehosePattern = string("sinks.druid.firehose")//"druid:firehose:%s" // Make up a service pattern, include %s somewhere in it.
  val discoveryPath = string("sinks.druid.discovery")//"/druid/discovery" // Your overlord's druid.discovery.curator.path.

  val curator = CuratorFrameworkFactory.builder().connectString(string("sinks.hdfs.zookeeper.url"))
    .retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
    .build()
  curator.start()

  override def receive: Receive = {
    case DruidMessage(listener,dataSource,hash) => {
      val numberOfCPUs = sys.runtime.availableProcessors()
      val threadPool = Executors.newFixedThreadPool(numberOfCPUs)
      implicit val ec = ExecutionContext.fromExecutorService(threadPool)
      Future {
        val topic = string("sinks.kafka.topic.prefix")+dataSource.db+"_"+dataSource.table
        val ssc = new StreamingContext(sc,Seconds(int("sinks.kafka.streaming.microbatch.time")))

        val streaming_broker_list = string("sinks.kafka.bootstrap.servers")
        val streaming_zookeeper_connect = string("sinks.kafka.metadata.brokers.list")
        val streaming_timeout_conn_ms = string("sinks.kafka.streaming.connection.timeout")
        val group = string("sinks.kafka.streaming.connection.group")

        val kafkaConf = Map(
          "metadata.broker.list" -> streaming_broker_list,
          "zookeeper.connect" -> streaming_zookeeper_connect,
          "group" -> group,
          "zookeeper.connection.timeout.ms" -> streaming_timeout_conn_ms)
        val lines = KafkaUtils.createStream[Array[Byte], String,
          DefaultDecoder, StringDecoder](
          ssc,
          kafkaConf,
          Map(topic -> 1),
          StorageLevel.MEMORY_ONLY_SER).map(_._2)
        val druidDataSource: String = topic
        val dimensions = dataSource.fullTableSchema.split(',').toIndexedSeq
        val aggregators = Seq(new CountAggregatorFactory("cnt"), new LongSumAggregatorFactory("baz", "baz"))
        val timestamper = (eventMap: Map[String, Any]) => new DateTime(eventMap("timestamp"))

        val druidService = DruidBeams
          .builder(timestamper)
          .curator(curator)
          .discoveryPath(discoveryPath)
          .location(DruidLocation(indexService, firehosePattern, druidDataSource))
          .rollup(DruidRollup(SpecificDruidDimensions(dimensions), aggregators, QueryGranularities.ALL))
          .tuning(
            ClusteredBeamTuning(
              segmentGranularity = Granularity.HOUR,
              windowPeriod = new Period("PT10M"),
              partitions = 1,
              replicants = 1
            )
          )
          .buildService()
      }

    }
  }
}
