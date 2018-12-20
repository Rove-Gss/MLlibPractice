package CodeInBook

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.{SparkConf, SparkContext}

object ALSPractice {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LR Practice").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://192.168.37.145:9000/testdata/ALSTestData")
    val ratings = data.map(_.split(",") match{
      case Array(user,item,rate)=>
        Rating(user.toInt,item.toInt,rate.toFloat)
    })
    val rank = 10
    val numIterations = 20
    val model = ALS.train(ratings,rank,numIterations,0.01)

    val usersProducts = ratings.map{
      case Rating(user,product,rate)=>
        (user,product)
    }
    val predictions = model.predict(usersProducts).map{
      case Rating(user,product,rate)=>
        ((user,product),rate)
    }
    val ratesAndPreds = ratings.map{
      case Rating(user,product,rate)=>
        ((user,product),rate)
    }
//    val MSE = ratesAndPreds.map{
//      case((user,product), (r1, r2))=>
//        val err= (r1 - r2)
//        err * err
//    }.mean()

  }
}
