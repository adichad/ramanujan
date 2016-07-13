package com.askme.ramanujan.actors

import akka.actor.Actor
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import com.typesafe.config.Config
import com.askme.ramanujan.Configurable
import org.apache.log4j.Logger

class ReportActor(val config: Config,val sqlContext: SQLContext) extends Actor with Configurable with Serializable{
  
  object Holder extends Serializable {      
     @transient lazy val log = Logger.getLogger(getClass.getName)    
  }
  
  val conf = sparkConf("spark")
  val sc = SparkContext.getOrCreate(conf)
  def receive = {
    case "none" => nothing(sc,"none")
    case "omniture_report" =>  getOmnitureReport(sc)
  }

  def nothing(sc: SparkContext,json: String) = {
    Holder.log.info("Nothing to prove, no report to run !!! "+json)
  }

  def getOmnitureReport(sc: SparkContext) = {
    // the queries get executed here, as-is
    var amb_orders = sqlContext.read.format("orc").load("live_amb_orders_amb_orders").toDF("#schema#")
    var amb_order_details = sqlContext.read.format("orc").load("live_amb_orders_amb_orders").toDF("#schema#")
    amb_orders = amb_orders.filter("date_created > '2016-03-20'")
    amb_orders = amb_orders.select("date_created", "order_total","id")
    amb_order_details = amb_order_details.select("orders_id", "subscribed_product_id")
    var sql3 = amb_orders.join(amb_order_details, amb_orders("id") === amb_order_details("orders_id"),"left_outer").groupBy("subscribed_product_id")
  }
}