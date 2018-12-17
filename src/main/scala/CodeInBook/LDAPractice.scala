package CodeInBook

import org.apache.spark.ml.clustering.DistributedLDAModel
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object LDAPractice {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("scala Spark Test").setMaster("local[3]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val data = sc.textFile("hdfs://192.168.37.145:9000/testdata/LDATestData")
    val parsedData = data.map(s=>Vectors.dense(s.trim.split(" ").map(_.toDouble)))
    parsedData.collect().foreach(println)
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()
    corpus.collect().foreach(println)
    val ldaModel = new LDA()
      .setK(3)
      .setDocConcentration(5)
      .setTopicConcentration(5)
      .setMaxIterations(20)
      .setSeed(100L)
      .setCheckpointInterval(10)
      .setOptimizer("em")
      .run(corpus)

    println("Learned topics" + ldaModel.vocabSize+" words")
    val topics = ldaModel.topicsMatrix
    for{topic <- Range(0,3)}{
      print("Topic "+topic + ":")
      for(word <- Range(0,ldaModel.vocabSize)){print(" "+topics(word,topic)+"\n")}
    }
    ldaModel.describeTopics(4)

//    val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
    //distLDAModel.topicDistributnios.collect
  }
}
