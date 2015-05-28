package com.wanodujia.ti.sparkr

/**
 * @author fegnlei@wandoujia.com, 15-5-25
 */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark._
import org.apache.spark.rdd.RDD

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result

import java.util.List

import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}

object FeatBaseService {
  def getConf(tableName: String, fieldsStr: String, date: String): Configuration = {

    val conf = HBaseConfiguration.create

    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val fields = fieldsStr.split(',')

    var cols = ""
    for (fd <- fields) {
      if (cols == "")
        cols += "data:" + fd
      else
        cols += " " + "data:" + fd
    }

    conf.set(TableInputFormat.SCAN_COLUMNS, cols)
    conf.set(TableInputFormat.SCAN_TIMESTAMP, date)
    conf.set(TableInputFormat.SCAN_CACHEDROWS, "100")

    conf
  }

  def test(jsc: JavaSparkContext, args: Array[String]):
  org.apache.spark.api.java.JavaRDD[Int] = {

    val testRDD = jsc.sc.parallelize(Array(1, 2, 3, 4, 5))
    JavaRDD.fromRDD(testRDD)
  }

  def testHBase(jsc: JavaSparkContext, args: Array[String]):
  org.apache.spark.api.java.JavaRDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable, org.apache.hadoop.hbase.client.Result)] = {
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, args(0))

    val hBaseRDD = jsc.sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    hBaseRDD.count()
    JavaRDD.fromRDD(hBaseRDD)
  }

  // API
  def getFeats(jsc: JavaSparkContext, conf: HBaseConfiguration): org.apache.spark.api.java.JavaRDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable, org.apache.hadoop.hbase.client.Result)] = {
    val hBaseRDD = jsc.sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    JavaRDD.fromRDD(hBaseRDD)
  }
}
