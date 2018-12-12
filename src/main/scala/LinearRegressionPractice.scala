import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LinearRegressionPractice {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("LR Practice").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val data_path = "hdfs://192.168.37.145:9000/testdata/LRTestData"
    val data = sc.textFile(data_path)
    val examples = data.map{line=>val parts = line.split(',')
    LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()

    val numIterations = 300
    val stepSize = 1
    val miniBatchFraction = 1.0
    val model = LinearRegressionWithSGD.train(examples,numIterations,stepSize,miniBatchFraction)
    val testData = sc.parallelize(Array(Vectors.dense(0.4267,1.2253),Vectors.dense(6.0,7.0),Vectors.dense(7.0,8.5)))
    val predictResult = model.predict(testData)
    predictResult.collect().foreach(println)
    //model.weights
    //model.intercep
//    val predictList = sc.parallelize(Array(5.0,6.0,7.0,8.0,9.0));
//    //val predictList = sc.parallelize(Array(5.0,6.0,7.0,8.0),3)
//    val tmp = examples.map(_.features)
//    val prediction = model.predict(predictList)
//    //val prediction = model.predict(predictList)
//    examples.map(_.features).collect().foreach(println)
//    prediction.collect().foreach(println)
    //val predictionAndLabel = prediction.zip(examples.map(_.label))
    //examples.map(_.label).foreach(println)
    //val predictionAndLabel = prediction.zip(predictList)
    //val print_predict = prediction.take(50)
//    println("prediction"+"\t"+"label")
//    for(i <- 0 to print_predict.length -1)
//      {
//        println(print_predict(i)._1 + "\t" + print_predict(i)._2)
//      }


  }
}
