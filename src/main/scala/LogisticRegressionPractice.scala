import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object LogisticRegressionPractice {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LR Practice").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val data=MLUtils.loadLibSVMFile(sc,"hdfs://192.168.37.145:9000/testdata/LORTestData")

    val training = data.cache()

    val test :LabeledPoint = new LabeledPoint(0, Vectors.dense(174,110,22,160))
    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training)

    val testResult = (model.predict(test.features),test.label)
    println("prediction" + "\t" +"label")
    println(testResult._1 + "\t" + testResult._2)

//    val predictionAndLabels = test.map{
//      case LabeledPoint(label,features) =>
//        val prediction = model.predict(features)
//        (prediction,label)
//    }
//    val printPredict = predictionAndLabels.take(20)
//    println("prediction" + "\t" +"label")
//    for(i <- 0 to printPredict.length - 1){
//      println(printPredict(i)._1 + "\t" + printPredict(i)._2)
//    }
//
//    val metrics = new MulticlassMetrics(predictionAndLabels)
//    val precision = metrics.accuracy
//    println("Precision = "+ precision)
  }
}
