package CodeInBook

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}

object FPGPractice {
  def main(arg:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("scala Spark Test").setMaster("local[3]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile("hdfs://192.168.37.145:9000/testdata/FPGTestData")
    val examples = data.map(_.split(" ")).cache()
    
    val minSupport = 0.2
    val numPartition = 10
    val model = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartition)
      .run(examples)

    model.freqItemsets.collect().foreach{
      itemset => println(itemset.items.mkString("[",",","]") + ", " + itemset.freq)
    }
  }
}
