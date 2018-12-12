import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object NaiveBayesPractice {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("scala Spark Test").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://192.168.37.145:9000/testdata/NBTestData")
    val parsedData = data.map { line =>
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }

    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 123123)
    val training = splits(0)
    val test = splits(1)

    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")//bernoulli型需要特征值不为0

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val print_predict = predictionAndLabel.take(20)
    println("prediction" + "\t" + "label")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("accuracy:" + accuracy)
  }
}

