package com.venusource.tequila.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object OrderAnalytics {
  
  def main(args: Array[String]) { 
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("HiveTable")    
    val sc = new SparkContext(conf)

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    //hiveContext.sql("SELECT * FROM orders where status=2 and created_at='2016-06-04'").collect().foreach(println)
    
    val df1 = hiveContext.sql("SELECT * FROM orders where status=2 and created_at='2016-06-05'")
    val df2 = hiveContext.sql("SELECT * FROM order_items where created_at='2016-06-05'")
    
    //df1.printSchema()
    //df2.printSchema()
    
    //df1.show()
    //df2.show()
    
    df1.join(df2, df1("id") === df2("o_id"), "left").groupBy("product_code").count().orderBy("count").show()
    //df1.join(df2, df1("id") === df2("o_id"), "left").collect().foreach(println)
  }
  
}
