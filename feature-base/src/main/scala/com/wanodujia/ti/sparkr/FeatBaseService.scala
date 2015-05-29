package com.wanodujia.ti.sparkr

/**
 * @author fegnlei@wandoujia.com, 15-5-25
 */

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}

import scala.collection.JavaConverters._

object FeatBaseService {
  def getConf(tableName: String, fieldsStr: String, dateRange: String): Configuration = {

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

    val dates = dateRange.split(',')
    val dateStart = dates(0)
    val dateEnd = dates(1)

    conf.set(TableInputFormat.SCAN_COLUMNS, cols)
    conf.set(TableInputFormat.SCAN_TIMERANGE_START, dateStart)
    conf.set(TableInputFormat.SCAN_TIMERANGE_END, dateEnd)
    conf.set(TableInputFormat.SCAN_CACHEDROWS, "100")

    conf
  }

  def getFeats(jsc: JavaSparkContext, conf: Configuration): JavaRDD[Array[String]] = {
    val hBaseRDD = jsc.sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    //keyValue is a RDD[java.util.list[hbase.KeyValue]]
    val keyValue = hBaseRDD.map(x => x._2).map(_.list)

    //outPut is a RDD[String], in which each line represents a record in HBase
    val outPut = keyValue.flatMap(x => x.asScala.map(cell => Array(
        Bytes.toStringBinary(CellUtil.cloneRow(cell)),        // row key
        Bytes.toStringBinary(CellUtil.cloneFamily(cell)),     // cf
        Bytes.toStringBinary(CellUtil.cloneQualifier(cell)),  // cq
        cell.getTimestamp.toString,                           // ts
        Bytes.toStringBinary(CellUtil.cloneValue(cell))       // val
      )
    )
    )

    JavaRDD.fromRDD(outPut)
  }

  //////////////// APIs
  /**
   * @param jsc
   * @param featList
   * @param dateRange
   * @return
   */
  def getFeats(jsc: JavaSparkContext, featList: String, dateRange: String): JavaRDD[Array[String]] = {

    val conf = FeatBaseService.getConf("udid_feat_base", featList, dateRange)

    val featRdd = FeatBaseService.getFeats(jsc, conf)

    featRdd
  }
}
