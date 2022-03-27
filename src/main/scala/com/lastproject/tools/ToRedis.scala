package com.lastproject.tools

import com.lastproject.bean.LogBean
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import com.lastproject.utils.RedisUtil
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

object ToRedis {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("参数错误")
      sys.exit()
    }

    //配置环境对象
    var Array(inputPath) = args
    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("toRedis")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    //清洗数据
    val log: RDD[String] = sc.textFile(inputPath)
    val logRDD: RDD[LogBean] = log.map(_.split(",", -1)).filter(_.length >= 85).map(LogBean(_)).filter(t => {
      t.appid.nonEmpty
    })

    //写入redis
    val rdd1: RDD[LogBean] = logRDD.mapPartitions(log => {
      val jedis: Jedis = RedisUtil.getJedis
      var res = List[LogBean]()
      while (log.hasNext) {
        var bean: LogBean = log.next()
        if (bean.appname == "" || bean.appname.isEmpty) {
          val str = jedis.get(bean.appid)
          bean.appname = str
        }
        res.::=(bean)
      }
      jedis.close()
      res.iterator
    })

    rdd1.map(x=>())

  }
}
