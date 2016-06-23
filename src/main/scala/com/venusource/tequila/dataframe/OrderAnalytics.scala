package com.venusource.tequila.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._


object OrderAnalytics {
  
  def main(args: Array[String]) { 
      val day_regx = """\d{1,4}-\d{1,2}-\d{1,2}""".r

        val conf = new SparkConf().setMaster("local[2]").setAppName("HiveTable")
        val sc = new SparkContext(conf)
    
        val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
        //hiveContext.sql("SELECT * FROM orders where status=2 and created_at='2016-06-04'").collect().foreach(println)
        
        //
        hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        //
        
        val df1 = hiveContext.sql("SELECT distinct id FROM orders where status=2 and created_at >='2016-05-23' and created_at <= '2016-06-22'")
        val df2 = hiveContext.sql("SELECT distinct id,product_code,quantity,'2016-06-22 00:00:00' as assayed_at, o_id,'2016-06-22' as day_at FROM order_items where created_at >='2016-05-22' and created_at <= '2016-06-21'")
        val df4 = hiveContext.sql("SELECT distinct id,code FROM products where created_at >='2016-05-23' and created_at <= '2016-06-22'")
        val df5 = df2.join(df4, df2("product_code") === df4("code"),"left").select(df2("quantity"),df2("o_id"),df2("assayed_at"),df2("day_at"),df4("id"))
        //df1.printSchema()
        //df2.printSchema()
        
//        df1.show()
//        df2.show()
        
        val df3 = df1.join(df5, df1("id") === df5("o_id"), "left").groupBy(df5("id"), df5("day_at"),df5("assayed_at")).sum("quantity")
        df3.show()
        df3.write.mode(SaveMode.Append).partitionBy("day_at").saveAsTable("recent_sales")

  }
  
}
