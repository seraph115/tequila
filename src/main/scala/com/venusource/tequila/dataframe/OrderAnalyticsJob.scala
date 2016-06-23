package com.venusource.tequila.dataframe

import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.desc

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import spark.jobserver.SparkJob
import spark.jobserver.SparkJobInvalid
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobValidation

object OrderAnalyticsJob extends SparkJob {
  
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName("OrderAnalyticsJob")
    val sc = new SparkContext(conf)
    val config = ConfigFactory.parseString("")
    val results = runJob(sc, config)
    println("Result is " + results)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("startDay.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No startDay.string config param"))
      Try(config.getString("endDay.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No .string config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    val startDay = config.getString("startDay.string")
    val endDay = config.getString("endDay.string")
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    val df1 = hiveContext.sql(s"SELECT distinct id FROM orders where status=2 and created_at >='${startDay}' and created_at <= '${endDay}'")
    val df2 = hiveContext.sql(s"SELECT distinct id,product_code,quantity,o_id, '${endDay} 00:00:00' as assayed_at,'${endDay}' as day_at  FROM order_items where created_at >='${startDay}' and created_at <= '${endDay}'")
    val df4 = hiveContext.sql(s"SELECT distinct id,code FROM products where created_at >='${startDay}' and created_at <= '${endDay}'")
    val df5 = df2.join(df4, df2("product_code") === df4("code"),"left").select(df2("quantity"),df2("o_id"),df2("assayed_at"),df2("day_at"),df4("id"))
    val df3 = df1.join(df5, df1("id") === df5("o_id"), "left").groupBy(df5("id"), df5("day_at"),df5("assayed_at")).sum("quantity")
    df3.write.mode(SaveMode.Append).partitionBy("day_at").saveAsTable("recent_sales")
  }
  
}