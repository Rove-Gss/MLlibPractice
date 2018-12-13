import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object SVMPractice {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("scala Spark Test").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val data = MLUtils.loadLibSVMFile(sc, "hdfs://192.168.37.145:9000/testdata/LIBSVMTestData")

    val splits = data.randomSplit(Array(0.6, 0.4), seed = 1231456)
    val training = splits(0).cache()
    val test = splits(1)

    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    val predictionAndLabel = test.map {
      point =>
        val score = model.predict(point.features)
        (score, point.label)
    }
    val printPrediction = predictionAndLabel.take(20)
    println("prediction" + "\t" + "label")
    for (i <- 0 to printPrediction.length - 1) {
      println(printPrediction(i)._1 + "\t" + printPrediction(i)._2)
    }

    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("Area under ROC = " + accuracy)
  }
}
