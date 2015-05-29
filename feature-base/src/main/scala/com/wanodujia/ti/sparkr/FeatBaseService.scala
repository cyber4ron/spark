package com.wanodujia.ti.sparkr

/**
 * @author fegnlei@wandoujia.com, 15-5-25
 */

import java.util

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
    conf.set("fields", fieldsStr)

    conf
  }

  def constrFeat(kv: org.apache.hadoop.hbase.KeyValue): Array[String] = {
    Array(
      Bytes.toStringBinary(CellUtil.cloneRow(cell)), // row key
      Bytes.toStringBinary(CellUtil.cloneQualifier(cell)), // cq
      cell.getTimestamp.toString, // ts
      Bytes.toStringBinary(CellUtil.cloneValue(cell)) // val
    )
  }

  def getFeats(jsc: JavaSparkContext, conf: Configuration): JavaRDD[Array[String]] = {
    val hbaseResultRDD = jsc.sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val flatResultRDD = hbaseResultRDD.map(x => x._2)
      .map(_.listCells())
      .flatMap(x => x.asScala.map(cell => constrFeat(cell)))

    val outputRDD = flatResultRDD
      .groupBy(ft => ft(0) + ft(2)) // group key is udid + ts
      .map((groupKey: String, feats: Iterable[Array[String]]) => {
      val featMap = new util.HashMap[String, String]()
      feats.foreach(featMap.put(_(1), _(3)))

      val resultFeats = ""
      conf.get("feats").split(',').foreach(ft => {
        if (resultFeats == "") resultFeats += featMap.get(ft)
        else resultFeats += "," + featMap.get(ft)
      })

      return Array(ft(0), resultFeats, ft(2)) // Array(udid, features, ts)
    })

    JavaRDD.fromRDD(outputRDD)
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
