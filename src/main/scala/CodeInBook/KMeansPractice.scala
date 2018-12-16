package CodeInBook

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object KMeansPractice {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("scala Spark Test").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://192.168.37.145:9000/testdata/kmeansData").map(s => Vectors.dense(s.split(" ").map(_.toDouble))).filter(x=>
      x(0) < 20 && x(1) < 20 && x(2) < 20
    ).cache()

    val initMode = "Random"//"k-means||"
    val numClusters = 3
    val numIterations = 20

    val model = new KMeans().setInitializationMode(initMode).setK(numClusters).setMaxIterations(numIterations).run(data)
    println("The Centers found: ")
    model.clusterCenters.foreach(println)
    val WSSSE = model.computeCost(data)
    println("Within Set Sum of Squared Errors = " + WSSSE)
  }
}
