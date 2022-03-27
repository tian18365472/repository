package com.lastproject.analyse

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ProcityAnalyseForRDD {

  def main(args: Array[String]): Unit = {

    //判断参数是否正确
    if (args.length != 2) {
      println(
        """
          |缺少参数
          |inputpath
          |""".stripMargin)
      sys.exit()
    }

    //生成sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    var spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[*]").getOrCreate()

    var sc = spark.sparkContext

    var Array(inputPath,outputPath) = args

    val line = sc.textFile(inputPath)

    val field = line.map(_.split(",",-1))

    val proCityRDD = field.filter(_.length >= 85).map(arr =>{

      var pro = arr(24)
      var city = arr(25)
      ((pro,city),1)
    })

    //btcw
    val reduceRDD:RDD[((String,String),Int)] = proCityRDD.reduceByKey(_ + _)

    val rdd2:RDD[(String,(String,Int))] = reduceRDD.map(arr => {
      (arr._1._1,(arr._1._2,arr._2))
    })

    val num:Long = rdd2.map(x => {
      (x._1,1)
    }).reduceByKey(_ + _).count()

    rdd2.partitionBy(new HashPartitioner(num.toInt)).saveAsTextFile(outputPath)

    spark.stop()

    sc.stop()


  }

}
