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
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
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

    def constrFeat(cell: org.apache.hadoop.hbase.Cell): Array[String] = {

        val featArr = Array(
            Bytes.toStringBinary(CellUtil.cloneRow(cell)), // key
            Bytes.toStringBinary(CellUtil.cloneQualifier(cell)), // cq
            cell.getTimestamp.toString, // ts
            Bytes.toStringBinary(CellUtil.cloneValue(cell)) // val
        )
        featArr
    }

    def getHBaseFeats(jsc: JavaSparkContext, conf: Configuration): RDD[Array[String]] = {

        println("========> getting hbase result rdd...")
        val resultRDD = jsc.sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result])

        println("========> flattening result rdd...")
        val flattenRDD = resultRDD.map(x => x._2)
            .map(_.listCells())
            .flatMap(x => x.asScala.map(cell => constrFeat(cell)))
        flattenRDD
    }

    def computeFeats(flattenRDD: RDD[Array[String]], featNames: String, num: Integer, seed: Long): JavaRDD[Array[String]] = {

        println("========> grouping flatten rdd...")
        val groupedRDD = flattenRDD
            .groupBy(ft => ft(0) + "," + ft(2)) // group key is udid + ts

        println("========> counting grouped rdd...")
        val count = groupedRDD.count()

        println("========> sampling grouped rdd...")
        val sampledRDD = groupedRDD.sample(false, 1.0 * num / count, seed)

        println("========> converting to output format...")
        val outputRDD = sampledRDD.map(item => {

            val groupKey = item._1
            val feats = item._2

            val featMap = new util.HashMap[String, String]() // feat name -> feat val
            feats.foreach(ft => featMap.put(ft(1), ft(3)))

            val resultFeats = new util.ArrayList[String]()
            featNames.split(',').foreach(ftName => resultFeats.add(featMap(ftName)))

            val splits = groupKey.split(',')
            val udid = splits(0)
            val ts = splits(1)

            val outputArray = new util.ArrayList[String]()

            outputArray.add(udid)
            outputArray.addAll(resultFeats)
            outputArray.add(ts)

            outputArray.toArray(new Array[String](outputArray.size)) // e.g. Array(udid, feat1, feat2, feat3, ts)
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
    def getFeats(jsc: JavaSparkContext, featList: String, dateRange: String, num: Integer, seed: Long): JavaRDD[Array[String]] = {

        val conf = FeatBaseService.getConf("udid_feat_base", featList, dateRange)

        val hbaseResultRDD = FeatBaseService.getHBaseFeats(jsc, conf)

        val outputRDD = FeatBaseService.computeFeats(hbaseResultRDD, featList, num, seed)

        outputRDD
    }
}
