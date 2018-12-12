import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.{IsotonicRegression, IsotonicRegressionModel}

object IsotonicRegressionPractice {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LR Practice").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://192.168.37.145:9000/testdata/IRTestData")
    val parsedData = data.map {
      str =>
        val splitsdata = str.split(",").map(_.toDouble)
        (splitsdata(0), splitsdata(1), 1.0)
    }
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 2312L)
    val training = splits(0)
    val test = splits(1)

    val model = new IsotonicRegression().setIsotonic(true).run(training)
    val x = model.boundaries
    val y = model.predictions
    println("boundaries" + "\t" + "predictions")
    for (i <- 0 to x.length - 1) {
      println(x(i) + "\t" + y(i))
    }

    val predictionAndLabel = test.map {
      point =>
        val predictedLabel = model.predict(point._2)
        (predictedLabel, point._1)
    }

    val print_predict = predictionAndLabel.take(20)
    println("prediction" + "\t" + "label")
    for (i <- 0 to print_predict.length - 1) {
        println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

//    val meanSquaredError = predictionAndLabel.map{case (p,1) => math.pow((p - 1),2)}.mean()
    // println("Mean Squared Error = " + meanSquaredError)

    val ModelPath = "hdfs://192.168.37.145:9000/ModelSave/IRModel"
    model.save(sc,ModelPath)
    //val modelRead = IsotonicRegressionModel()
  }
}
