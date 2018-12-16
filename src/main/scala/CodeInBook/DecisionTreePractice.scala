package CodeInBook

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object DecisionTreePractice {
 def main(args:Array[String]): Unit ={
   val conf = new SparkConf().setAppName("scala Spark Test").setMaster("local[3]")
   val sc = new SparkContext(conf)

   val data = MLUtils.loadLibSVMFile(sc,"hdfs://192.168.37.145:9000/testdata/LIBSVMTestData")
   val splits = data.randomSplit(Array(0.7,0.3))
   val (trainingData,testData) = (splits(0),splits(1))

   val numClasses = 2
   val categoricalFeaturesInfo = Map[Int,Int]()
   val impurity = "entropy"
   val maxDepth = 5
   val maxBins = 32

   val model = DecisionTree.trainClassifier(trainingData,numClasses,categoricalFeaturesInfo,impurity,maxDepth,maxBins)

   val labelAndPreds = testData.map{point =>
     val prediction = model.predict(point.features)
     (point.label,prediction)
   }

   println("label:" + "\t" + "prediction")

   labelAndPreds.collect().foreach(println)
   val testError = labelAndPreds.filter(r=>r._1 != r._2).count.toDouble / testData.count()
   println("Test Error: " + testError)

 }
}
