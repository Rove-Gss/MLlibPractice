package CodeInBook

import org.apache.spark.{SparkConf, SparkContext}

object MLlibPractice {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("scala Spark Test").setMaster("local")
    val sc = new SparkContext(conf)

    /*
  * part：测试colStats。
  * description:  colStats是列统计方法，可以计算每列的最大值、最小值、平均值、方差值
  *               以及L1、L2范数。
  */
    //    val data_path = "hdfs://192.168.37.145:9000/testdata/sample_stat"
    //    val data = sc.textFile(data_path).flatMap(_.split(" ")).map(f=>f.toDouble)
    //    val data1 = data.map(f => Vectors.dense(f))
    //
    //    val stat1 = Statistics.colStats(data1)
    //    println(stat1.count, stat1.max, stat1.min, stat1.mean, stat1.normL1, stat1.normL2, stat1.numNonzeros, stat1.variance)

    /*
  * part：测试Pearson系数、Spearman相关系数
  * description:  Pearson系数用来描述两个数值变量的线性相关性，一般用于正态分布
  *               Spearman系数同样用来描述两个变量的相关性，不过对于分布的要求不严格。
  */
    //        val data_path = "hdfs://192.168.37.145:9000/testdata/sample_stat"
    //        val data = sc.textFile(data_path).flatMap(_.split(" ")).map(f => f.toDouble)
    //        val data1 = data.map(f => Vectors.dense(f))
    //        val corr1 = Statistics.corr(data1, "pearson")
    //        val corr2 = Statistics.corr(data1, "spearman")
    //        val x1 = sc.parallelize(List(1.0, 2.0, 3.0, 4.0))
    //        val y1 = sc.parallelize(List(1.0, 0.0, 2.0, -1.0))
    //        //val corr3 = Statistics.corr(x1, y1, "pearson")
    //        val corr3 = Statistics.corr(x1, y1, "spearman")
    //        println("corr1:" + corr1)
    //        println("corr2:" + corr2)
    //        println("corr3:" + corr3)

    /*
  * part：测试载入LIBSVM格式数据
  * description:  SVM:支持向量机，通常用来模式识别、分类与回归分析
  */

    //    val data = MLUtils.loadLibSVMFile(sc,"hdfs://192.168.37.145:9000/testdata/LIBSVMTest")
    //    data.collect().foreach(println)

    /*
  * part 测试生成KMeans的训练样本数据
  * description: KMeans中，K指生成K个簇，Means指将每个聚类中均值作为该聚类的中心值。
  */

    //    val KMeansRDD = KMeansDataGenerator.generateKMeansRDD(sc,40,5,3,1.0,2)//sc:spark环境 40:样本数 5:聚类数 3：数据维度 1.0:初始中心分布的缩放因子 2:RDD分区数
    //    println(KMeansRDD.count())
    //    KMeansRDD.take(5).foreach(println)

    /*
  * part 测试生成线性回归训练样本数据
  * description:
  */

    //    val LinearRDD = LinearDataGenerator.generateLinearRDD(sc,40,3,1.0,2,0.0)//sc:spark环境 40:样本数量 3:数据维度 1.0：epsilon因子 2：分区数 0.0：干扰
    //    LinearRDD.collect().foreach(println)

    /*
  * part 测试生成逻辑回归训练样本数据
  * description:
  */
    //    val LogisticRDD = LogisticRegressionDataGenerator.generateLogisticRDD(sc,40,3,1.0,2,0.5)//sc:spark环境 40:样本数量 3:样本特征数 1.0:epsilon因子 2:RDD分区数 0.5 标签为1的概率
    //    LogisticRDD.collect().foreach(println)
  }
}
