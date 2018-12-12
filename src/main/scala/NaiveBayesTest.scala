import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, NaiveBayes}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object NaiveBayesTest {

  def DataParse(s:Array[String]): LabeledPoint={
    var ret:Array[Double] = new Array[Double](7)
    ret(0) = s(0) match{//transform buying price
      case "vhigh"=>0.0
      case "high"=>1.0
      case "med"=>2.0
      case "low"=>3.0
    }
    ret(1) = s(1) match{//transform maintain price
      case "vhigh"=>0.0
      case "high"=>1.0
      case "med"=>2.0
      case "low"=>3.0
    }
    ret(2) = s(2) match{//transform doors
      case "2"=>0.0
      case "3"=>1.0
      case "4"=>2.0
      case "5more"=>3.0
    }
    ret(3) = s(3) match{//transform persons
      case "2"=>0.0
      case "4"=>1.5
      case "more"=>3.0
    }
    ret(4) = s(4) match{//transform lug_boot
      case "small"=>0.0
      case "med"=>1.5
      case "big"=>3.0
    }
    ret(5) = s(5) match{//transform safty
      case "low"=>0.0
      case "med"=>1.5
      case "high"=>3.0
    }
    ret(6) = s(6) match{//transform acceptability
      case "unacc" => 0.0
      case "acc" =>1.0
      case "good"=>2.0
      case "vgood"=>3.0
    }
    LabeledPoint(ret(6),Vectors.dense(Array(ret(0),ret(1),ret(2),ret(3),ret(4),ret(5))))
  }

  def main(args:Array[String]): Unit ={
      val conf = new SparkConf().setAppName("scala Spark Test").setMaster("spark://192.168.37.145:7077")
      val sc = new SparkContext(conf)

      val data = sc.textFile("hdfs://192.168.37.145:9000/testdata/carData")

      val parsedData = data.map{
        line=> val splits = line.split(",")
          val parsedSplits = DataParse(splits)
          parsedSplits
      }
    parsedData.cache()

    val model = NaiveBayes.train(parsedData,lambda = 0.0, modelType = "multinomial")
    val predictResult = parsedData.map(p => (model.predict(p.features), p.label,p.features))
    println("Truth:\tPrediction:")
    predictResult.filter(x=>x._1 != x._2).collect().foreach(println)
    val accuracy = 1.0 * predictResult.filter(x => x._1 == x._2).count() / parsedData.count()
    println("accuracy:"+accuracy)
  }


}


