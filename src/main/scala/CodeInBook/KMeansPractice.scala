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
    val TestData1 = Vectors.dense(10.1,10.2,10.3)
    val TestData2 = Vectors.dense(5.1,5.3,5.5)
    val prediction1 = (TestData1,model.predict(TestData1))
    val prediction2 = (TestData2,model.predict(TestData2))
    println(prediction1._1 + "\t" + prediction1._2)
    println(prediction2._1 + "\t" + prediction2._2)
  }
}
