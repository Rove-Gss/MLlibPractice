package Application

import org.apache.spark.mllib.classification.{NaiveBayes, SVMWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
* 用UCI adult数据集中的数据来预测一个给定的成年人的收入是否大于50k.
* 用3种分类算法解决这个问题：朴素贝叶斯、SVM、决策树。
* 先选取部分数据进行考虑。
 */

object IncomePrediction {
  def dataFilter(x: String): LabeledPoint = {
    val ret = x.split(", ")
    val age = ret(0).toInt
    var ageV = 0.0
    if (age > 16 && age <= 21) ageV = 1.0
    else if (age > 21 && age <= 27) ageV = 2.0
    else if (age > 27 && age <= 33) ageV = 3.0
    else if (age > 33 && age <= 39) ageV = 4.0
    else if (age > 39 && age <= 45) ageV = 5.0
    else if (age > 45 && age <= 51) ageV = 6.0
    else if (age > 51 && age <= 57) ageV = 7.0
    else if (age > 57 && age <= 63) ageV = 8.0
    else if (age > 63 && age <= 69) ageV = 9.0
    else if (age > 69 && age <= 75) ageV = 10.0
    else ageV = 10.0

    val workclass = ret(1)
    var workclassV = workclass match {
      case "Private" => 1.0
      case "Self-emp-not-inc" => 2.0
      case "Self-emp-inc" => 3.0
      case "Federal-gov" => 4.0
      case "Local-gov" => 5.0
      case "State-gov" => 6.0
      case "Without-pay" => 7.0
      case "Never-worked" => 8.0
      case _ => 0.0
    }

    val education_num = ret(4).toInt
    var education_numV = 0.0
    if (education_num < 4) education_numV = 1.0
    else if (education_num < 7) education_numV = 2.0
    else if (education_num < 9) education_numV = 3.0
    else if (education_num < 10) education_numV = 4.0
    else if (education_num < 11) education_numV = 5.0
    else if (education_num < 13) education_numV = 6.0
    else if (education_num < 14) education_numV = 7.0
    else education_numV = 8.0

    val education = ret(3)
    var educationV = education match {
      case "Bachelors" => 1.0
      case "Some-college" => 2.0
      case "11th" => 3.0
      case "HS-grad" => 4.0
      case "Prof-school" => 5.0
      case "Assoc-acdm" => 6.0
      case "Assoc-voc" => 7.0
      case "9th" => 8.0
      case "7th-8th" => 9.0
      case "12th" => 10.0
      case "Masters" => 11.0
      case "1st-4th" => 12.0
      case "10th" => 13.0
      case "Doctorate" => 14.0
      case "5th-6th" => 15.0
      case "Preschool" => 16.0
      case _ => 0.0
    }

    val occupation = ret(6)
    var occupationV = occupation match {
      case "Tech-support" => 1.0
      case "Craft-repair" => 2.0
      case "Other-service" => 3.0
      case "Sales" => 4.0
      case "Exec-managerial" => 5.0
      case "Prof-specialty" => 6.0
      case "Handlers-cleaners" => 7.0
      case "Machine-op-inspct" => 8.0
      case "Adm-clerical" => 9.0
      case "Farming-fishing" => 10.0
      case "Transport-moving" => 11.0
      case "Priv-house-serv" => 12.0
      case "Protective-serv" => 13.0
      case "Armed-Forces" => 14.0
      case _ => 0.0
    }
    val hoursPerWeek = ret(12).toInt
    var hoursPerWeekV = 0.0

    if (hoursPerWeek < 20) hoursPerWeekV = 1.0
    else if (hoursPerWeek < 50) hoursPerWeekV = 2.0
    else if (hoursPerWeek < 60) hoursPerWeekV = 3.0
    else if (hoursPerWeek < 90) hoursPerWeekV = 4.0
    else hoursPerWeekV = 5.0

    val incomeLevel = ret(14)
    var label = incomeLevel match {
      case "<=50K" => 0.0
      case ">50K" => 1.0
      case "<=50K." => 0.0
      case ">50K." => 1.0
      case _ => 0.0
    }
    LabeledPoint(label, Vectors.dense(Array( workclassV, educationV, occupationV,  hoursPerWeekV)))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Income Prediction").setMaster("local[3]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rawData = sc.textFile("hdfs://192.168.37.145:9000/testdata/personData")
    val LabeledData = rawData.map { x =>
      dataFilter(x)
    }
    val rawDataForTest = sc.textFile("hdfs://192.168.37.145:9000/testdata/personDataTest")

    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 30

    val DecisionTreemodel = DecisionTree.trainClassifier(LabeledData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
    val dataForTest = rawDataForTest.map { x =>
      dataFilter(x)
    }

    val DecisionTreeResult = dataForTest.map { x =>
      (x.label, DecisionTreemodel.predict(x.features),"features:",x.features)
    }
    println("Reality: Prediction:")
    DecisionTreeResult.filter(x => x._1 != x._2).foreach(println)
    val DecisionTreeaccuracy = DecisionTreeResult.filter(x => x._1 == x._2).count().toDouble / dataForTest.count().toDouble
    println(DecisionTreeaccuracy)

  }
}
